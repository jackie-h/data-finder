"""
End-to-end test: the finance example model (Account, Instrument, Trade, ContractualPosition)
mapped to GraphQL and queried through a mock GraphQL HTTP server.
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Union

import datetime
import numpy as np
import pytest
from numpy.testing import assert_array_equal

from datafinder import QueryRunnerBase
from datafinder.runner import FinderResult, convert_inputs_and_select
from datafinder.typed_attributes import IntegerAttribute, StringAttribute, FloatAttribute, DateAttribute
import re

from datafinder_graphql.graphql_engine import GraphQLConnect
from model.graphql_mapping import (
    GraphQLEndpoint,
    GraphQLQuery,
    GraphQLField,
    GraphQLPropertyMapping,
    GraphQLClassMapping,
    GraphQLProcessingMilestone,
    GraphQLBusinessDateMilestone,
    GraphQLBiTemporalMilestone,
)
from model.relational import NoOperation, Operation
from model.m3 import Class, Property, String, Float, Integer, Date, DateTime, Package

# ---------------------------------------------------------------------------
# Example finance model (same classes as example/mappings.py)
# ---------------------------------------------------------------------------

def _make_account_class() -> Class:
    return Class('Account', [
        Property('Id', 'id', Integer),
        Property('Name', 'name', String),
    ], Package('finance'))


def _make_instrument_class() -> Class:
    return Class('Instrument', [
        Property('Symbol', 'symbol', String),
        Property('Price', 'price', Float),
    ], Package('finance'))


def _make_trade_class(account: Class, instrument: Class) -> Class:
    return Class('Trade', [
        Property('Symbol', 'symbol', String),
        Property('Price', 'price', Float),
        Property('Account', 'account', account),
        Property('Instrument', 'instrument', instrument),
    ], Package('finance'))


def _make_contractual_position_class(instrument: Class) -> Class:
    return Class('ContractualPosition', [
        Property('Business Date', 'business_date', Date),
        Property('Quantity', 'quantity', Float),
        Property('Counterparty', 'counterparty', Integer),
        Property('Instrument', 'instrument', instrument),
    ], Package('finance'))


# ---------------------------------------------------------------------------
# Mock GraphQL data (mirrors data/accounts.csv, prices.csv, trades.csv, contractualpositions.csv)
# ---------------------------------------------------------------------------

_MOCK_DATA: dict[str, list[dict]] = {
    "accounts": [
        {"id": 211978, "name": "Trading Account 1"},
    ],
    "instruments": [
        {"symbol": "IBM",  "price": 2304.5},
        {"symbol": "GS",   "price": 45.7},
        {"symbol": "AAPL", "price": 84.11},
    ],
    "trades": [
        {"id": 1, "symbol": "IBM",  "price": 3000.5},
        {"id": 2, "symbol": "GS",   "price": 45.7},
        {"id": 3, "symbol": "AAPL", "price": 84.11},
    ],
    "contractualPositions": [
        {"businessDate": "2024-01-10", "instrument": "IBM",  "counterparty": 1256, "quantity": 200.0},
        {"businessDate": "2024-01-11", "instrument": "GS",   "counterparty": 1257, "quantity": 1000.0},
    ],
}


class _FinanceGraphQLHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))
        query = body.get("query", "")

        response_data = {}
        for name, data in _MOCK_DATA.items():
            if name in query:
                response_data[name] = data
                break

        payload = json.dumps({"data": response_data}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


def _start_finance_server() -> HTTPServer:
    server = HTTPServer(("127.0.0.1", 0), _FinanceGraphQLHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


# ---------------------------------------------------------------------------
# GraphQL mappings for the finance model
# ---------------------------------------------------------------------------

def _build_graphql_mappings(endpoint: GraphQLEndpoint):
    account_c = _make_account_class()
    instrument_c = _make_instrument_class()
    trade_c = _make_trade_class(account_c, instrument_c)
    position_c = _make_contractual_position_class(instrument_c)

    account_query = GraphQLQuery("accounts", endpoint)
    account_cm = GraphQLClassMapping(account_c, [
        GraphQLPropertyMapping(account_c.property('id'),   GraphQLField("id")),
        GraphQLPropertyMapping(account_c.property('name'), GraphQLField("name")),
    ], account_query)

    instrument_query = GraphQLQuery("instruments", endpoint)
    instrument_cm = GraphQLClassMapping(instrument_c, [
        GraphQLPropertyMapping(instrument_c.property('symbol'), GraphQLField("symbol")),
        GraphQLPropertyMapping(instrument_c.property('price'),  GraphQLField("price")),
    ], instrument_query)

    trade_query = GraphQLQuery("trades", endpoint)
    trade_cm = GraphQLClassMapping(trade_c, [
        GraphQLPropertyMapping(trade_c.property('symbol'), GraphQLField("symbol")),
        GraphQLPropertyMapping(trade_c.property('price'),  GraphQLField("price")),
    ], trade_query)

    position_query = GraphQLQuery("contractualPositions", endpoint)
    position_cm = GraphQLClassMapping(position_c, [
        GraphQLPropertyMapping(position_c.property('business_date'), GraphQLField("businessDate")),
        GraphQLPropertyMapping(position_c.property('quantity'),      GraphQLField("quantity")),
        GraphQLPropertyMapping(position_c.property('counterparty'),  GraphQLField("counterparty")),
    ], position_query)

    return account_cm, instrument_cm, trade_cm, position_cm


# ---------------------------------------------------------------------------
# Finder classes backed by GraphQL mappings
# ---------------------------------------------------------------------------

class AccountGraphQLFinder:
    def __init__(self, class_mapping: GraphQLClassMapping):
        self._query = class_mapping.query
        self.__id   = IntegerAttribute('Id',   'id',   'INT',    'accounts')
        self.__name = StringAttribute( 'Name', 'name', 'STRING', 'accounts')

    def id_(self)   -> IntegerAttribute: return self.__id
    def name(self)  -> StringAttribute:  return self.__name

    def find_all(self, business_date, processing_valid_at, display_columns,
                 filter_op: Operation = NoOperation()) -> FinderResult:
        return convert_inputs_and_select(business_date, processing_valid_at,
                                         display_columns, self._query, filter_op)


class InstrumentGraphQLFinder:
    def __init__(self, class_mapping: GraphQLClassMapping):
        self._query  = class_mapping.query
        self.__symbol = StringAttribute('Symbol', 'symbol', 'STRING', 'instruments')
        self.__price  = FloatAttribute( 'Price',  'price',  'FLOAT',  'instruments')

    def symbol(self) -> StringAttribute: return self.__symbol
    def price(self)  -> FloatAttribute:  return self.__price

    def find_all(self, business_date, processing_valid_at, display_columns,
                 filter_op: Operation = NoOperation()) -> FinderResult:
        return convert_inputs_and_select(business_date, processing_valid_at,
                                         display_columns, self._query, filter_op)


class TradeGraphQLFinder:
    def __init__(self, class_mapping: GraphQLClassMapping):
        self._query   = class_mapping.query
        self.__symbol = StringAttribute('Symbol', 'symbol', 'STRING', 'trades')
        self.__price  = FloatAttribute( 'Price',  'price',  'FLOAT',  'trades')

    def symbol(self) -> StringAttribute: return self.__symbol
    def price(self)  -> FloatAttribute:  return self.__price

    def find_all(self, business_date, processing_valid_at, display_columns,
                 filter_op: Operation = NoOperation()) -> FinderResult:
        return convert_inputs_and_select(business_date, processing_valid_at,
                                         display_columns, self._query, filter_op)


class ContractualPositionGraphQLFinder:
    def __init__(self, class_mapping: GraphQLClassMapping):
        self._query        = class_mapping.query
        self.__business_date = StringAttribute( 'Business Date', 'businessDate', 'STRING', 'contractualPositions')
        self.__instrument    = StringAttribute( 'Instrument',    'instrument',   'STRING', 'contractualPositions')
        self.__counterparty  = IntegerAttribute('Counterparty',  'counterparty', 'INT',    'contractualPositions')
        self.__quantity      = FloatAttribute(  'Quantity',      'quantity',     'FLOAT',  'contractualPositions')

    def business_date(self)  -> StringAttribute:  return self.__business_date
    def instrument(self)     -> StringAttribute:  return self.__instrument
    def counterparty(self)   -> IntegerAttribute: return self.__counterparty
    def quantity(self)       -> FloatAttribute:   return self.__quantity

    def find_all(self, business_date, processing_valid_at, display_columns,
                 filter_op: Operation = NoOperation()) -> FinderResult:
        return convert_inputs_and_select(business_date, processing_valid_at,
                                         display_columns, self._query, filter_op)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def finance_server():
    server = _start_finance_server()
    yield server
    server.shutdown()


@pytest.fixture(scope="module")
def finders(finance_server):
    host, port = finance_server.server_address
    endpoint = GraphQLEndpoint(f"http://{host}:{port}/graphql")
    account_cm, instrument_cm, trade_cm, position_cm = _build_graphql_mappings(endpoint)
    return {
        "account":   AccountGraphQLFinder(account_cm),
        "instrument": InstrumentGraphQLFinder(instrument_cm),
        "trade":     TradeGraphQLFinder(trade_cm),
        "position":  ContractualPositionGraphQLFinder(position_cm),
    }


@pytest.fixture(autouse=True)
def register_runner():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(GraphQLConnect)
    yield
    QueryRunnerBase.clear()


# ---------------------------------------------------------------------------
# Tests
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
        data = cpf.find_all(None, None, [cpf.instrument(), cpf.quantity()]).to_numpy()
        assert_array_equal(data, np.array(
            [["IBM", 200.0], ["GS", 1000.0]], dtype="object"
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
        data = cpf.find_all(None, None,
                             [cpf.business_date(), cpf.instrument(),
                              cpf.counterparty(), cpf.quantity()]).to_numpy()
        assert data.shape == (2, 4)
        assert list(data[:, 1]) == ["IBM", "GS"]
        assert list(data[:, 3]) == [200.0, 1000.0]

    def test_graphql_mapping_wires_model_to_endpoint(self, finance_server):
        """Verify the mapping metadata connects the domain model to the GraphQL endpoint."""
        host, port = finance_server.server_address
        endpoint = GraphQLEndpoint(f"http://{host}:{port}/graphql")
        account_cm, _, _, _ = _build_graphql_mappings(endpoint)

        assert account_cm.clazz.name == "Account"
        assert account_cm.query.name == "accounts"
        assert account_cm.query.endpoint.url == f"http://{host}:{port}/graphql"
        field_names = [pm.target.name for pm in account_cm.property_mappings]
        assert field_names == ["id", "name"]


# ===========================================================================
# Temporal E2E tests
# ===========================================================================
#
# These tests use a mock server that actually interprets temporal arguments
# and filters data, mirroring the behaviour of a real bi-temporal GraphQL API.
#
# Data mirrors the CSV fixtures:
#   instruments  → processing-temporal (price changes over time)
#   contractualPositions → business-date-temporal (snapshot per business date)
#   trades       → bi-temporal (both dimensions)
# ---------------------------------------------------------------------------

def _extract_arg(query: str, arg_name: str) -> str | None:
    m = re.search(rf'{arg_name}:\s*"([^"]+)"', query)
    return m.group(1) if m else None


# Instrument price history (processing-temporal)
# Mirrors data/prices.csv: IBM had price 1203.5 until 09:00:05, then 2304.5
_INSTRUMENT_HISTORY = [
    {"symbol": "IBM",  "price": 1203.5,
     "_from": "2020-01-01T08:00:00", "_to": "2020-01-01T09:00:05"},
    {"symbol": "IBM",  "price": 2304.5,
     "_from": "2020-01-01T09:00:05", "_to": None},
    {"symbol": "GS",   "price": 45.7,
     "_from": "2020-01-01T08:00:00", "_to": None},
    {"symbol": "AAPL", "price": 84.11,
     "_from": "2020-01-01T08:00:06", "_to": None},
]

# Contractual positions keyed by business date (mirrors data/contractualpositions.csv)
_POSITIONS_BY_DATE: dict[str, list[dict]] = {
    "2024-01-10": [{"instrument": "IBM", "quantity": 200.0,  "counterparty": 1256}],
    "2024-01-11": [{"instrument": "GS",  "quantity": 1000.0, "counterparty": 1257}],
}

# Trade history (bi-temporal): IBM trade had price 1203.5, then 3000.5 after 10:30
_TRADE_HISTORY = [
    {"symbol": "IBM",  "price": 1203.5,
     "_from": "2020-01-01T08:00:05", "_to": "2020-01-01T10:30:00"},
    {"symbol": "IBM",  "price": 3000.5,
     "_from": "2020-01-01T10:30:00", "_to": None},
    {"symbol": "GS",   "price": 45.7,
     "_from": "2022-01-01T10:30:00", "_to": None},
    {"symbol": "AAPL", "price": 84.11,
     "_from": "2022-01-01T10:30:00", "_to": None},
]


def _filter_processing(rows: list[dict], as_of: datetime.datetime) -> list[dict]:
    result = []
    for row in rows:
        valid_from = datetime.datetime.fromisoformat(row["_from"])
        valid_to = (datetime.datetime.fromisoformat(row["_to"])
                    if row["_to"] else None)
        if valid_from <= as_of and (valid_to is None or valid_to > as_of):
            result.append({k: v for k, v in row.items() if not k.startswith("_")})
    return result


class _TemporalGraphQLHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))
        query = body.get("query", "")

        response_data = {}

        if "instruments" in query:
            as_of_str = _extract_arg(query, "asOf")
            if as_of_str:
                as_of = datetime.datetime.fromisoformat(as_of_str)
                rows = _filter_processing(_INSTRUMENT_HISTORY, as_of)
            else:
                rows = [
                    {k: v for k, v in r.items() if not k.startswith("_")}
                    for r in _INSTRUMENT_HISTORY if r["_to"] is None
                ]
            response_data["instruments"] = rows

        elif "contractualPositions" in query:
            bd_str = _extract_arg(query, "businessDate")
            if bd_str:
                rows = _POSITIONS_BY_DATE.get(bd_str, [])
            else:
                rows = [r for day in _POSITIONS_BY_DATE.values() for r in day]
            response_data["contractualPositions"] = rows

        elif "trades" in query:
            as_of_str = _extract_arg(query, "asOf")
            if as_of_str:
                as_of = datetime.datetime.fromisoformat(as_of_str)
                rows = _filter_processing(_TRADE_HISTORY, as_of)
            else:
                rows = [
                    {k: v for k, v in r.items() if not k.startswith("_")}
                    for r in _TRADE_HISTORY if r["_to"] is None
                ]
            response_data["trades"] = rows

        payload = json.dumps({"data": response_data}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def temporal_server():
    server = HTTPServer(("127.0.0.1", 0), _TemporalGraphQLHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server
    server.shutdown()


def _temporal_endpoint(server) -> GraphQLEndpoint:
    host, port = server.server_address
    return GraphQLEndpoint(f"http://{host}:{port}/graphql")


class TestGraphQLProcessingMilestoneE2E:
    """instruments query with asOf — server returns the record valid at that instant."""

    def test_price_before_change(self, temporal_server):
        endpoint = _temporal_endpoint(temporal_server)
        # At 08:30, IBM's first price record (1203.5) is active
        q = GraphQLQuery("instruments", endpoint,
                          milestone=GraphQLProcessingMilestone("asOf"))
        sym = StringAttribute("Symbol", "symbol", "STRING", "instruments")
        price = FloatAttribute("Price", "price", "FLOAT", "instruments")

        data = FinderResult(None, datetime.datetime(2020, 1, 1, 8, 30, 0),
                             [sym, price], q, None).to_numpy()

        ibm_row = [r for r in data if r[0] == "IBM"]
        assert len(ibm_row) == 1
        assert float(ibm_row[0][1]) == 1203.5

    def test_price_after_change(self, temporal_server):
        endpoint = _temporal_endpoint(temporal_server)
        # At 10:00, IBM's second price record (2304.5) is active
        q = GraphQLQuery("instruments", endpoint,
                          milestone=GraphQLProcessingMilestone("asOf"))
        sym = StringAttribute("Symbol", "symbol", "STRING", "instruments")
        price = FloatAttribute("Price", "price", "FLOAT", "instruments")

        data = FinderResult(None, datetime.datetime(2020, 1, 1, 10, 0, 0),
                             [sym, price], q, None).to_numpy()

        ibm_row = [r for r in data if r[0] == "IBM"]
        assert len(ibm_row) == 1
        assert float(ibm_row[0][1]) == 2304.5

    def test_no_asof_returns_open_ended_records(self, temporal_server):
        endpoint = _temporal_endpoint(temporal_server)
        q = GraphQLQuery("instruments", endpoint,
                          milestone=GraphQLProcessingMilestone("asOf"))
        sym = StringAttribute("Symbol", "symbol", "STRING", "instruments")

        # No processing datetime → no asOf argument → server returns open-ended records only
        data = FinderResult(None, None, [sym], q, None).to_numpy()
        symbols = list(data[:, 0])
        assert "IBM" in symbols   # open-ended record
        assert "GS" in symbols
        assert "AAPL" in symbols


class TestGraphQLBusinessDateMilestoneE2E:
    """contractualPositions query with businessDate — server filters by date."""

    def test_positions_on_first_date(self, temporal_server):
        endpoint = _temporal_endpoint(temporal_server)
        q = GraphQLQuery("contractualPositions", endpoint,
                          milestone=GraphQLBusinessDateMilestone("businessDate"))
        instrument = StringAttribute("Instrument", "instrument", "STRING", "contractualPositions")
        quantity = FloatAttribute("Quantity", "quantity", "FLOAT", "contractualPositions")

        data = FinderResult(datetime.date(2024, 1, 10), None,
                             [instrument, quantity], q, None).to_numpy()

        assert_array_equal(data, np.array([["IBM", 200.0]], dtype="object"))

    def test_positions_on_second_date(self, temporal_server):
        endpoint = _temporal_endpoint(temporal_server)
        q = GraphQLQuery("contractualPositions", endpoint,
                          milestone=GraphQLBusinessDateMilestone("businessDate"))
        instrument = StringAttribute("Instrument", "instrument", "STRING", "contractualPositions")
        quantity = FloatAttribute("Quantity", "quantity", "FLOAT", "contractualPositions")

        data = FinderResult(datetime.date(2024, 1, 11), None,
                             [instrument, quantity], q, None).to_numpy()

        assert_array_equal(data, np.array([["GS", 1000.0]], dtype="object"))

    def test_no_business_date_returns_all(self, temporal_server):
        endpoint = _temporal_endpoint(temporal_server)
        q = GraphQLQuery("contractualPositions", endpoint,
                          milestone=GraphQLBusinessDateMilestone("businessDate"))
        instrument = StringAttribute("Instrument", "instrument", "STRING", "contractualPositions")

        data = FinderResult(None, None, [instrument], q, None).to_numpy()
        assert data.shape == (2, 1)


class TestGraphQLBiTemporalMilestoneE2E:
    """trades query with asOf — bi-temporal, processing dimension only in this model."""

    def test_trade_price_at_early_time(self, temporal_server):
        endpoint = _temporal_endpoint(temporal_server)
        q = GraphQLQuery("trades", endpoint,
                          milestone=GraphQLBiTemporalMilestone(
                              business_date_argument="businessDate",
                              processing_argument="asOf"))
        sym = StringAttribute("Symbol", "symbol", "STRING", "trades")
        price = FloatAttribute("Price", "price", "FLOAT", "trades")

        # At 09:00, IBM's original price (1203.5) is the active record
        data = FinderResult(None, datetime.datetime(2020, 1, 1, 9, 0, 0),
                             [sym, price], q, None).to_numpy()

        ibm_row = [r for r in data if r[0] == "IBM"]
        assert len(ibm_row) == 1
        assert float(ibm_row[0][1]) == 1203.5

    def test_trade_price_after_amendment(self, temporal_server):
        endpoint = _temporal_endpoint(temporal_server)
        q = GraphQLQuery("trades", endpoint,
                          milestone=GraphQLBiTemporalMilestone(
                              business_date_argument="businessDate",
                              processing_argument="asOf"))
        sym = StringAttribute("Symbol", "symbol", "STRING", "trades")
        price = FloatAttribute("Price", "price", "FLOAT", "trades")

        # At 11:00, IBM's amended price (3000.5) is the active record
        data = FinderResult(None, datetime.datetime(2020, 1, 1, 11, 0, 0),
                             [sym, price], q, None).to_numpy()

        ibm_row = [r for r in data if r[0] == "IBM"]
        assert len(ibm_row) == 1
        assert float(ibm_row[0][1]) == 3000.5

    def test_milestone_metadata(self):
        endpoint = GraphQLEndpoint("http://example.com/graphql")
        m = GraphQLBiTemporalMilestone("businessDate", "asOf")
        q = GraphQLQuery("trades", endpoint, milestone=m)

        assert q.milestone.business_date_argument == "businessDate"
        assert q.milestone.processing_argument == "asOf"
