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
# Hasura where / order_by / limit parsing for mock server
# ---------------------------------------------------------------------------

def _extract_balanced(s: str, arg_name: str, open_char: str) -> str | None:
    """Extract a balanced {…} or […] value for a named arg from a GraphQL query string."""
    close_char = '}' if open_char == '{' else ']'
    m = re.search(rf'{re.escape(arg_name)}:\s*{re.escape(open_char)}', s)
    if not m:
        return None
    start = m.end() - 1  # rewind to include the open char
    depth = 0
    for i in range(start, len(s)):
        if s[i] == open_char:
            depth += 1
        elif s[i] == close_char:
            depth -= 1
            if depth == 0:
                return s[start:i + 1]
    return None


def _tokenize_gql(s: str) -> list:
    tokens: list = []
    i = 0
    while i < len(s):
        c = s[i]
        if c in ' \t\n\r,':
            i += 1
        elif c in '{}[]:':
            tokens.append(c)
            i += 1
        elif c == '"':
            j = i + 1
            while j < len(s) and s[j] != '"':
                if s[j] == '\\':
                    j += 1
                j += 1
            tokens.append(('str', s[i + 1:j]))
            i = j + 1
        elif c.isdigit() or (c == '-' and i + 1 < len(s) and s[i + 1].isdigit()):
            j = i
            while j < len(s) and s[j] in '0123456789.-':
                j += 1
            raw = s[i:j]
            tokens.append(('num', float(raw) if '.' in raw else int(raw)))
            i = j
        elif c.isalpha() or c == '_':
            j = i
            while j < len(s) and (s[j].isalnum() or s[j] == '_'):
                j += 1
            word = s[i:j]
            if word == 'true':
                tokens.append(('bool', True))
            elif word == 'false':
                tokens.append(('bool', False))
            elif word == 'null':
                tokens.append(('null', None))
            else:
                tokens.append(('id', word))
            i = j
        else:
            i += 1
    return tokens


def _parse_gql_value(tokens: list, pos: int):
    if pos >= len(tokens):
        return None, pos
    tok = tokens[pos]
    if tok == '{':
        return _parse_gql_object(tokens, pos)
    if tok == '[':
        return _parse_gql_array(tokens, pos)
    if isinstance(tok, tuple):
        return tok[1], pos + 1
    return None, pos + 1


def _parse_gql_object(tokens: list, pos: int):
    pos += 1  # skip '{'
    obj: dict = {}
    while pos < len(tokens) and tokens[pos] != '}':
        tok = tokens[pos]
        if not (isinstance(tok, tuple) and tok[0] == 'id'):
            break
        key = tok[1]
        pos += 1
        if pos < len(tokens) and tokens[pos] == ':':
            pos += 1
        val, pos = _parse_gql_value(tokens, pos)
        obj[key] = val
    if pos < len(tokens) and tokens[pos] == '}':
        pos += 1
    return obj, pos


def _parse_gql_array(tokens: list, pos: int):
    pos += 1  # skip '['
    arr: list = []
    while pos < len(tokens) and tokens[pos] != ']':
        val, pos = _parse_gql_value(tokens, pos)
        arr.append(val)
    if pos < len(tokens) and tokens[pos] == ']':
        pos += 1
    return arr, pos


_DT_FMTS_CMP = ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d')


def _parse_dt_cmp(s: str) -> datetime.datetime | None:
    for fmt in _DT_FMTS_CMP:
        try:
            return datetime.datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def _coerce(raw, target):
    """Coerce raw (row value) to the same type as target (where clause value)."""
    if isinstance(target, bool):
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str):
            return raw.lower() == 'true'
        return bool(raw)
    if isinstance(target, (int, float)) and not isinstance(target, bool):
        try:
            return type(target)(raw)
        except (TypeError, ValueError):
            return raw
    if isinstance(target, str):
        dt_target = _parse_dt_cmp(target)
        if dt_target is not None and isinstance(raw, str):
            dt_raw = _parse_dt_cmp(raw)
            if dt_raw is not None:
                return dt_raw
    return raw


def _eval_hasura_where(where: dict, row: dict) -> bool:
    if '_and' in where:
        return all(_eval_hasura_where(sub, row) for sub in where['_and'])
    if '_or' in where:
        return any(_eval_hasura_where(sub, row) for sub in where['_or'])
    if '_not' in where:
        return not _eval_hasura_where(where['_not'], row)
    for field, ops in where.items():
        if not isinstance(ops, dict):
            continue
        raw = row.get(field)
        for op_name, target in ops.items():
            coerced = _coerce(raw, target)
            if op_name == '_is_null':
                ok = (raw is None) == target
            elif op_name == '_eq':
                ok = coerced == target
            elif op_name == '_neq':
                ok = coerced != target
            elif op_name == '_lt':
                ok = coerced < target
            elif op_name == '_lte':
                ok = coerced <= target
            elif op_name == '_gt':
                ok = coerced > target
            elif op_name == '_gte':
                ok = coerced >= target
            else:
                ok = True
            if not ok:
                return False
    return True


def _apply_hasura_where(rows: list[dict], query_str: str) -> list[dict]:
    where_str = _extract_balanced(query_str, 'where', '{')
    if not where_str:
        return rows
    tokens = _tokenize_gql(where_str)
    where, _ = _parse_gql_object(tokens, 0)
    return [r for r in rows if _eval_hasura_where(where, r)]


def _apply_hasura_order_by(rows: list[dict], query_str: str) -> list[dict]:
    ob_str = _extract_balanced(query_str, 'order_by', '[')
    if not ob_str:
        return rows
    tokens = _tokenize_gql(ob_str)
    order_list, _ = _parse_gql_array(tokens, 0)
    for item in reversed(order_list):
        if isinstance(item, dict):
            for field, direction in item.items():
                rows = sorted(
                    rows,
                    key=lambda r, f=field: (r.get(f) is None, r.get(f)),
                    reverse=(direction == 'desc'),
                )
    return rows


def _apply_hasura_limit(rows: list[dict], query_str: str) -> list[dict]:
    m = re.search(r'\blimit:\s*(\d+)', query_str)
    return rows[:int(m.group(1))] if m else rows


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
            rows = _FinanceHandler._accounts
            rows = _apply_hasura_where(rows, query_str)
            rows = _apply_hasura_order_by(rows, query_str)
            rows = _apply_hasura_limit(rows, query_str)
            response_data["accounts"] = rows

        elif "trades" in query_str:
            as_of_str = _extract_arg(query_str, "asOf")
            rows = _FinanceHandler._trades
            if as_of_str:
                as_of = _parse_dt(as_of_str)
                rows = _filter_processing(rows, as_of, "_in_z", "_out_z")
            rows = _apply_hasura_where(rows, query_str)
            rows = _apply_hasura_order_by(rows, query_str)
            rows = _apply_hasura_limit(rows, query_str)
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
            rows = _apply_hasura_where(rows, query_str)
            rows = _apply_hasura_order_by(rows, query_str)
            rows = _apply_hasura_limit(rows, query_str)
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
