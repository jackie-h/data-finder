"""
End-to-end test: the finance example model mapped to GraphQL
and queried through a mock GraphQL HTTP server, using code-generated finders.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import datetime
import numpy as np
import pytest
from numpy.testing import assert_array_equal

from datafinder import QueryRunnerBase
from datafinder_graphql.graphql_engine import GraphQLConnect
from datafinder_graphql.generator import generate as graphql_generate
from mapping_markdown.graphql_mapping_markdown import load as load_graphql_mapping
from model.graphql_mapping import (
    GraphQLEndpoint,
    GraphQLAssociationMapping,
    GraphQLProcessingMilestone,
    GraphQLBusinessDateMilestone,
    GraphQLBiTemporalMilestone,
)

FIXTURE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "mapping_markdown", "tests", "finance_graphql_mapping.md")
)

_GRAPHQL_FINDER_MODULES = [
    "account_finder", "instrument_finder", "trade_finder", "contractualposition_finder",
]

# ---------------------------------------------------------------------------
# Mock data — covers basic, temporal, and association scenarios
# ---------------------------------------------------------------------------

_MOCK_ACCOUNTS = [
    {"id": 211978, "name": "Trading Account 1"},
]

# Instruments: processing-temporal (price changes over time)
_INSTRUMENT_HISTORY = [
    {"symbol": "IBM",  "price": 1203.5, "_from": "2020-01-01T08:00:00", "_to": "2020-01-01T09:00:05"},
    {"symbol": "IBM",  "price": 2304.5, "_from": "2020-01-01T09:00:05", "_to": None},
    {"symbol": "GS",   "price": 45.7,   "_from": "2020-01-01T08:00:00", "_to": None},
    {"symbol": "AAPL", "price": 84.11,  "_from": "2020-01-01T08:00:06", "_to": None},
]

# Contractual positions: business-date keyed (filtered by businessDate arg)
_POSITION_HISTORY = [
    {"businessDate": "2024-01-10", "quantity": 200.0,  "npv": 1500.0},
    {"businessDate": "2024-01-11", "quantity": 1000.0, "npv": 7500.0},
]

# Trades: bi-temporal (asOf) with nested account object
_TRADE_HISTORY = [
    {"symbol": "IBM",  "price": 1203.5, "account": {"id": 211978, "name": "Trading Account 1"},
     "_from": "2020-01-01T08:00:05", "_to": "2020-01-01T10:30:00"},
    {"symbol": "IBM",  "price": 3000.5, "account": {"id": 211978, "name": "Trading Account 1"},
     "_from": "2020-01-01T10:30:00", "_to": None},
    {"symbol": "GS",   "price": 45.7,   "account": {"id": 211978, "name": "Trading Account 1"},
     "_from": "2022-01-01T10:30:00", "_to": None},
    {"symbol": "AAPL", "price": 84.11,  "account": {"id": 211978, "name": "Trading Account 1"},
     "_from": "2022-01-01T10:30:00", "_to": None},
]

# Captures the most recently received GraphQL query string for assertion
_last_query: list[str] = []


def _extract_arg(query: str, arg_name: str) -> str | None:
    m = re.search(rf'{arg_name}:\s*"([^"]+)"', query)
    return m.group(1) if m else None


def _filter_processing(rows: list[dict], as_of: datetime.datetime) -> list[dict]:
    result = []
    for row in rows:
        valid_from = datetime.datetime.fromisoformat(row["_from"])
        valid_to = datetime.datetime.fromisoformat(row["_to"]) if row["_to"] else None
        if valid_from <= as_of and (valid_to is None or valid_to > as_of):
            result.append({k: v for k, v in row.items() if not k.startswith("_")})
    return result


def _current_records(rows: list[dict]) -> list[dict]:
    return [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows if r.get("_to") is None]


class _FinanceGraphQLHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))
        query_str = body.get("query", "")
        _last_query[:] = [query_str]

        response_data = {}

        if "accounts" in query_str:
            response_data["accounts"] = _MOCK_ACCOUNTS

        elif "instruments" in query_str:
            as_of_str = _extract_arg(query_str, "asOf")
            response_data["instruments"] = (
                _filter_processing(_INSTRUMENT_HISTORY, datetime.datetime.fromisoformat(as_of_str))
                if as_of_str else _current_records(_INSTRUMENT_HISTORY)
            )

        elif "contractualPositions" in query_str:
            bd_str = _extract_arg(query_str, "businessDate")
            response_data["contractualPositions"] = (
                [r for r in _POSITION_HISTORY if r["businessDate"] == bd_str]
                if bd_str else list(_POSITION_HISTORY)
            )

        elif "trades" in query_str:
            as_of_str = _extract_arg(query_str, "asOf")
            response_data["trades"] = (
                _filter_processing(_TRADE_HISTORY, datetime.datetime.fromisoformat(as_of_str))
                if as_of_str else _current_records(_TRADE_HISTORY)
            )

        payload = json.dumps({"data": response_data}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def finance_server():
    server = HTTPServer(("127.0.0.1", 0), _FinanceGraphQLHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server
    server.shutdown()


@pytest.fixture(scope="module")
def finder_classes():
    """Generate finders from the GraphQL mapping markdown once per module; yield class dict."""
    temp_dir = tempfile.mkdtemp()
    for mod in _GRAPHQL_FINDER_MODULES:
        sys.modules.pop(mod, None)
    sys.path.insert(0, temp_dir)

    mapping = load_graphql_mapping(FIXTURE)
    graphql_generate(mapping, temp_dir)

    from account_finder import AccountFinder
    from instrument_finder import InstrumentFinder
    from trade_finder import TradeFinder
    from contractualposition_finder import ContractualPositionFinder

    yield {
        "AccountFinder":             AccountFinder,
        "InstrumentFinder":          InstrumentFinder,
        "TradeFinder":               TradeFinder,
        "ContractualPositionFinder": ContractualPositionFinder,
    }

    sys.path.remove(temp_dir)
    for mod in _GRAPHQL_FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="module")
def finders(finance_server, finder_classes):
    """Instantiate generated finders backed by the single finance mock server."""
    host, port = finance_server.server_address
    endpoint = GraphQLEndpoint(f"http://{host}:{port}/graphql")
    return {
        "account":    finder_classes["AccountFinder"](endpoint),
        "instrument": finder_classes["InstrumentFinder"](endpoint),
        "trade":      finder_classes["TradeFinder"](endpoint),
        "position":   finder_classes["ContractualPositionFinder"](endpoint),
    }


@pytest.fixture(autouse=True)
def register_runner():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(GraphQLConnect)
    yield
    QueryRunnerBase.clear()


# ---------------------------------------------------------------------------
# Basic query tests
# ---------------------------------------------------------------------------

class TestGraphQLFinanceE2E:

    def test_query_accounts(self, finders):
        af = finders["account"]
        data = af.find_all(None, None, [af.id_(), af.name()]).to_numpy()
        assert_array_equal(data, np.array([[211978, "Trading Account 1"]], dtype="object"))

    def test_query_instruments(self, finders):
        inf = finders["instrument"]
        data = inf.find_all(None, None, [inf.symbol(), inf.price()]).to_numpy()
        assert_array_equal(data, np.array(
            [["IBM", 2304.5], ["GS", 45.7], ["AAPL", 84.11]], dtype="object"
        ))

    def test_query_trades(self, finders):
        tf = finders["trade"]
        data = tf.find_all(None, None, [tf.symbol(), tf.price()]).to_numpy()
        assert_array_equal(data, np.array(
            [["IBM", 3000.5], ["GS", 45.7], ["AAPL", 84.11]], dtype="object"
        ))

    def test_query_contractual_positions(self, finders):
        cpf = finders["position"]
        data = cpf.find_all(None, None, [cpf.quantity(), cpf.npv()]).to_numpy()
        assert_array_equal(data, np.array(
            [[200.0, 1500.0], [1000.0, 7500.0]], dtype="object"
        ))

    def test_query_instruments_to_pandas(self, finders):
        inf = finders["instrument"]
        df = inf.find_all(None, None, [inf.symbol(), inf.price()]).to_pandas()
        assert list(df.columns) == ["symbol", "price"]
        assert df["symbol"].tolist() == ["IBM", "GS", "AAPL"]
        assert df["price"].tolist() == [2304.5, 45.7, 84.11]

    def test_query_accounts_single_column(self, finders):
        af = finders["account"]
        data = af.find_all(None, None, [af.name()]).to_numpy()
        assert_array_equal(data, np.array([["Trading Account 1"]], dtype="object"))

    def test_query_positions_all_columns(self, finders):
        cpf = finders["position"]
        data = cpf.find_all(None, None, [cpf.business_date(), cpf.quantity(), cpf.npv()]).to_numpy()
        assert data.shape == (2, 3)
        assert list(data[:, 1]) == [200.0, 1000.0]

    def test_graphql_mapping_wires_model_to_endpoint(self):
        """Verify the mapping file correctly defines all four class queries."""
        mapping = load_graphql_mapping(FIXTURE)
        by_class = {cm.clazz.name: cm for cm in mapping.mappings}

        account_cm = by_class["Account"]
        assert account_cm.query.name == "accounts"
        assert account_cm.query.milestone is None
        field_names = [pm.target.name for pm in account_cm.property_mappings
                       if not isinstance(pm, GraphQLAssociationMapping)]
        assert field_names == ["id", "name"]


# ---------------------------------------------------------------------------
# Processing-milestone tests (InstrumentFinder — asOf from mapping)
# ---------------------------------------------------------------------------

class TestGraphQLProcessingMilestoneE2E:
    """InstrumentFinder — processing milestone baked in from the mapping markdown."""

    def test_price_before_change(self, finders):
        inf = finders["instrument"]
        data = inf.find_all(None, datetime.datetime(2020, 1, 1, 8, 30, 0),
                            [inf.symbol(), inf.price()]).to_numpy()
        ibm_row = [r for r in data if r[0] == "IBM"]
        assert len(ibm_row) == 1
        assert float(ibm_row[0][1]) == 1203.5

    def test_price_after_change(self, finders):
        inf = finders["instrument"]
        data = inf.find_all(None, datetime.datetime(2020, 1, 1, 10, 0, 0),
                            [inf.symbol(), inf.price()]).to_numpy()
        ibm_row = [r for r in data if r[0] == "IBM"]
        assert len(ibm_row) == 1
        assert float(ibm_row[0][1]) == 2304.5

    def test_no_asof_returns_current_records(self, finders):
        inf = finders["instrument"]
        data = inf.find_all(None, None, [inf.symbol()]).to_numpy()
        symbols = list(data[:, 0])
        assert "IBM" in symbols
        assert "GS" in symbols
        assert "AAPL" in symbols

    def test_milestone_metadata(self, finder_classes):
        inf = finder_classes["InstrumentFinder"](GraphQLEndpoint("http://example.com/graphql"))
        assert isinstance(inf._query.milestone, GraphQLProcessingMilestone)
        assert inf._query.milestone.argument_name == "asOf"


# ---------------------------------------------------------------------------
# Business-date-milestone tests (ContractualPositionFinder — businessDate from mapping)
# ---------------------------------------------------------------------------

class TestGraphQLBusinessDateMilestoneE2E:
    """ContractualPositionFinder — business-date milestone baked in from the mapping markdown."""

    def test_positions_on_first_date(self, finders):
        cpf = finders["position"]
        data = cpf.find_all(datetime.date(2024, 1, 10), None,
                            [cpf.business_date(), cpf.quantity()]).to_numpy()
        assert_array_equal(data, np.array([["2024-01-10", 200.0]], dtype="object"))

    def test_positions_on_second_date(self, finders):
        cpf = finders["position"]
        data = cpf.find_all(datetime.date(2024, 1, 11), None,
                            [cpf.business_date(), cpf.quantity()]).to_numpy()
        assert_array_equal(data, np.array([["2024-01-11", 1000.0]], dtype="object"))

    def test_no_business_date_returns_all(self, finders):
        cpf = finders["position"]
        data = cpf.find_all(None, None, [cpf.quantity()]).to_numpy()
        assert data.shape == (2, 1)

    def test_milestone_metadata(self, finder_classes):
        cpf = finder_classes["ContractualPositionFinder"](GraphQLEndpoint("http://example.com/graphql"))
        assert isinstance(cpf._query.milestone, GraphQLBusinessDateMilestone)
        assert cpf._query.milestone.argument_name == "businessDate"


# ---------------------------------------------------------------------------
# Bi-temporal-milestone tests (TradeFinder — businessDate + asOf from mapping)
# ---------------------------------------------------------------------------

class TestGraphQLBiTemporalMilestoneE2E:
    """TradeFinder — bitemporal milestone baked in from the mapping markdown."""

    def test_trade_price_at_early_time(self, finders):
        tf = finders["trade"]
        data = tf.find_all(None, datetime.datetime(2020, 1, 1, 9, 0, 0),
                           [tf.symbol(), tf.price()]).to_numpy()
        ibm_row = [r for r in data if r[0] == "IBM"]
        assert len(ibm_row) == 1
        assert float(ibm_row[0][1]) == 1203.5

    def test_trade_price_after_amendment(self, finders):
        tf = finders["trade"]
        data = tf.find_all(None, datetime.datetime(2020, 1, 1, 11, 0, 0),
                           [tf.symbol(), tf.price()]).to_numpy()
        ibm_row = [r for r in data if r[0] == "IBM"]
        assert len(ibm_row) == 1
        assert float(ibm_row[0][1]) == 3000.5

    def test_milestone_metadata(self, finder_classes):
        tf = finder_classes["TradeFinder"](GraphQLEndpoint("http://example.com/graphql"))
        assert isinstance(tf._query.milestone, GraphQLBiTemporalMilestone)
        assert tf._query.milestone.business_date_argument == "businessDate"
        assert tf._query.milestone.processing_argument == "asOf"


# ---------------------------------------------------------------------------
# Association mapping tests
# ---------------------------------------------------------------------------

class TestGraphQLAssociationE2E:

    def test_association_mapping_in_loaded_mapping(self):
        """The TradeAccount association is correctly loaded from the mapping fixture."""
        mapping = load_graphql_mapping(FIXTURE)
        by_class = {cm.clazz.name: cm for cm in mapping.mappings}
        trade_cm = by_class["Trade"]

        assoc_pms = [pm for pm in trade_cm.property_mappings if isinstance(pm, GraphQLAssociationMapping)]
        assert len(assoc_pms) == 1
        pm = assoc_pms[0]
        assert pm.association_name == "TradeAccount"
        assert pm.target.name == "account"
        assert pm.property.id == "account"
        assert pm.property.type.name == "Account"

    def test_traverse_account_name(self, finders):
        """tf.account().name() selects the nested name field from the account object."""
        tf = finders["trade"]
        data = tf.find_all(None, None, [tf.symbol(), tf.account().name()]).to_numpy()

        assert data[0][0] == "IBM"
        assert data[0][1] == "Trading Account 1"

    def test_traverse_account_id(self, finders):
        """tf.account().id_() selects the nested id field from the account object."""
        tf = finders["trade"]
        data = tf.find_all(None, None, [tf.symbol(), tf.account().id_()]).to_numpy()

        assert data[0][0] == "IBM"
        assert data[0][1] == 211978

    def test_multiple_account_fields_produce_nested_query(self, finders):
        """Requesting several account sub-fields emits account { id name } in the query."""
        tf = finders["trade"]
        tf.find_all(None, None, [tf.symbol(), tf.account().id_(), tf.account().name()]).to_numpy()

        sent = _last_query[0]
        assert "account" in sent
        assert "name" in sent
        assert "id" in sent
