"""
Runs ACCOUNT_FINDER_SPECS, TRADE_FINDER_SPECS, and CONTRACTUAL_POSITION_FINDER_SPECS
against the GraphQL backend using a mock server seeded from the example CSV files.
"""
from __future__ import annotations

import csv
import datetime
import json
import re
import shutil
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

from datafinder import QueryRunnerBase
from datafinder_examples import example_path
from datafinder_graphql.generator import generate as graphql_generate
from datafinder_graphql.graphql_engine import GraphQLConnect
from mapping_markdown.graphql_mapping_markdown import load as load_graphql_mapping
from model.graphql_mapping import GraphQLClassMapping, GraphQLEndpoint

from datafinder_examples_tests.finance_specs import (
    ACCOUNT_FINDER_SPECS,
    TRADE_FINDER_SPECS,
    CONTRACTUAL_POSITION_FINDER_SPECS,
)

FIXTURE = str(example_path("finance_graphql_mapping.md"))

_GRAPHQL_FINDER_MODULES = [
    "account_finder", "trade_finder", "contractualposition_finder",
]


# ---------------------------------------------------------------------------
# Load CSV data
# ---------------------------------------------------------------------------

def _read_csv(name: str) -> list[dict]:
    return list(csv.DictReader(example_path(name).open()))


def _parse_dt(s: str) -> datetime.datetime:
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f"Cannot parse datetime: {s!r}")


def _filter_processing(rows: list[dict], as_of: datetime.datetime,
                        in_col: str, out_col: str) -> list[dict]:
    result = []
    for row in rows:
        in_z = _parse_dt(row[in_col])
        out_z = _parse_dt(row[out_col])
        if in_z <= as_of < out_z:
            result.append(row)
    return result


def _extract_arg(query: str, arg_name: str) -> str | None:
    m = re.search(rf'{arg_name}:\s*"([^"]+)"', query)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Build in-memory data, shaped as GraphQL responses
# ---------------------------------------------------------------------------

def _build_accounts() -> list[dict]:
    rows = _read_csv("finance_accounts.csv")
    return [{"id": int(r["ID"]), "name": r["ACCT_NAME"]} for r in rows]


def _build_account_index(accounts: list[dict]) -> dict[int, dict]:
    return {a["id"]: a for a in accounts}


def _build_trades(account_index: dict[int, dict]) -> list[dict]:
    rows = _read_csv("finance_trades.csv")
    return [
        {
            "symbol": r["sym"],
            "price": float(r["price"]),
            "isSettled": r["is_settled"].lower() == "true",
            "validFrom": r["in_z"],
            "validTo": r["out_z"],
            "account": account_index.get(int(r["account_id"]), {"id": int(r["account_id"]), "name": ""}),
            "_in_z": r["in_z"],
            "_out_z": r["out_z"],
        }
        for r in rows
    ]


def _build_positions() -> list[dict]:
    rows = _read_csv("finance_positions.csv")
    return [
        {
            "businessDate": r["DATE"],
            "quantity": float(r["QUANTITY"]),
            "npv": float(r["NPV"]),
            "_in_z": r["in_z"],
            "_out_z": r["out_z"],
        }
        for r in rows
    ]


def _strip_internal(rows: list[dict]) -> list[dict]:
    return [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]


# ---------------------------------------------------------------------------
# Mock HTTP handler
# ---------------------------------------------------------------------------

class _FinanceHandler(BaseHTTPRequestHandler):

    _accounts: list[dict] = []
    _trades: list[dict] = []
    _positions: list[dict] = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))
        query_str = body.get("query", "")

        response_data: dict = {}

        if "accounts" in query_str:
            response_data["accounts"] = _FinanceHandler._accounts

        elif "trades" in query_str:
            as_of_str = _extract_arg(query_str, "asOf")
            rows = _FinanceHandler._trades
            if as_of_str:
                as_of = _parse_dt(as_of_str)
                rows = _filter_processing(rows, as_of, "_in_z", "_out_z")
            response_data["trades"] = _strip_internal(rows)

        elif "contractualPositions" in query_str:
            bd_str = _extract_arg(query_str, "businessDate")
            as_of_str = _extract_arg(query_str, "asOf")
            rows = _FinanceHandler._positions
            if bd_str:
                rows = [r for r in rows if r["businessDate"] == bd_str]
            if as_of_str:
                as_of = _parse_dt(as_of_str)
                rows = _filter_processing(rows, as_of, "_in_z", "_out_z")
            response_data["contractualPositions"] = _strip_internal(rows)

        payload = json.dumps({"data": response_data}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):  # type: ignore[override]
        pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def finance_server():
    accounts = _build_accounts()
    account_index = _build_account_index(accounts)
    _FinanceHandler._accounts = accounts
    _FinanceHandler._trades = _build_trades(account_index)
    _FinanceHandler._positions = _build_positions()

    server = HTTPServer(("127.0.0.1", 0), _FinanceHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server
    server.shutdown()


@pytest.fixture(scope="module")
def finance_finders(finance_server):
    tmp = tempfile.mkdtemp()
    for mod in _GRAPHQL_FINDER_MODULES:
        sys.modules.pop(mod, None)
    sys.path.insert(0, tmp)

    host, port = finance_server.server_address
    endpoint_url = f"http://{host}:{port}/graphql"

    # Load mapping and patch endpoint URL so generated finders hit our mock server
    mapping = load_graphql_mapping(FIXTURE)
    for cm in mapping.mappings:
        if isinstance(cm, GraphQLClassMapping):
            cm.query.endpoint.url = endpoint_url

    graphql_generate(mapping, tmp)

    from account_finder import AccountFinder  # type: ignore[import]
    from trade_finder import TradeFinder  # type: ignore[import]
    from contractualposition_finder import ContractualPositionFinder  # type: ignore[import]

    endpoint = GraphQLEndpoint(endpoint_url)
    yield AccountFinder(endpoint), TradeFinder(endpoint), ContractualPositionFinder(endpoint)

    if tmp in sys.path:
        sys.path.remove(tmp)
    for mod in _GRAPHQL_FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(autouse=True)
def register_runner():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(GraphQLConnect)
    yield
    QueryRunnerBase.clear()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "expectation", ACCOUNT_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_account(expectation, finance_finders):
    expectation.run(finance_finders[0])


@pytest.mark.parametrize(
    "expectation", TRADE_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_trade(expectation, finance_finders):
    expectation.run(finance_finders[1])


@pytest.mark.parametrize(
    "expectation", CONTRACTUAL_POSITION_FINDER_SPECS.expectations, ids=lambda e: e.name
)
def test_position(expectation, finance_finders):
    expectation.run(finance_finders[2])
