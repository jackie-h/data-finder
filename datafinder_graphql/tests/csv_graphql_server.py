"""
A mock GraphQL server that serves rows from in-memory tables, so the shared specs can run against
the GraphQL backend. It understands the Hasura-style `where`, `order_by` and `limit` arguments plus
the `asOf` and `businessDate` milestoning arguments.
"""
from __future__ import annotations

import csv
import datetime
import json
import re
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from datafinder_examples import example_path


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
        # a missing end means the row is open-ended
        out_z = _parse_dt(row[out_col]) if row[out_col] else datetime.datetime.max
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


def _comparable(raw: Any, target: Any) -> tuple[Any, Any]:
    """Bring a row value and a where-clause value to the same type so they can be compared."""
    if isinstance(target, bool):
        if isinstance(raw, str):
            raw = raw.lower() == 'true'
        return bool(raw), target
    if isinstance(target, (int, float)):
        try:
            return type(target)(raw), target
        except (TypeError, ValueError):
            return raw, target
    if isinstance(target, str) and isinstance(raw, str):
        dt_target = _parse_dt_cmp(target)
        dt_raw = _parse_dt_cmp(raw)
        if dt_target is not None and dt_raw is not None:
            return dt_raw, dt_target
    return raw, target


class GraphQLServerError(Exception):
    """A request the mock server rejects, as a real server would with a GraphQL error."""


def _like_to_regex(pattern: str) -> re.Pattern:
    out = []
    for ch in pattern:
        out.append(".*" if ch == "%" else "." if ch == "_" else re.escape(ch))
    return re.compile("".join(out), re.DOTALL)


def _eval_leaf(op_name: str, raw: Any, target: Any) -> bool:
    if op_name == '_is_null':
        return (raw is None) == target
    if raw is None:
        return False  # comparing null is never true, as in SQL
    if op_name in ('_like', '_nlike'):
        coerced = raw
    else:
        coerced, target = _comparable(raw, target)
    if op_name == '_eq':
        return coerced == target
    if op_name == '_neq':
        return coerced != target
    if op_name == '_lt':
        return coerced < target
    if op_name == '_lte':
        return coerced <= target
    if op_name == '_gt':
        return coerced > target
    if op_name == '_gte':
        return coerced >= target
    if op_name == '_like':
        return _like_to_regex(str(target)).fullmatch(str(raw)) is not None
    if op_name == '_nlike':
        return _like_to_regex(str(target)).fullmatch(str(raw)) is None
    raise GraphQLServerError(f"field '{op_name}' not found in type: 'Boolean_comparison_exp'")


def _eval_where(where: dict, row: dict) -> bool:
    if '_and' in where:
        return all(_eval_where(sub, row) for sub in where['_and'])
    if '_or' in where:
        return any(_eval_where(sub, row) for sub in where['_or'])
    if '_not' in where:
        return not _eval_where(where['_not'], row)
    for field, cond in where.items():
        if not isinstance(cond, dict):
            raise GraphQLServerError(f"expected an object for '{field}'")
        raw = row.get(field)
        if cond and all(k.startswith('_') for k in cond):
            if not all(_eval_leaf(op_name, raw, target) for op_name, target in cond.items()):
                return False
        else:
            # a condition on a nested object matches only rows that have one, as a join would
            if raw is None or not _eval_where(cond, raw):
                return False
    return True


def _apply_where(rows: list[dict], query_str: str) -> list[dict]:
    where_str = _extract_balanced(query_str, 'where', '{')
    if not where_str:
        return rows
    tokens = _tokenize_gql(where_str)
    where, _ = _parse_gql_object(tokens, 0)
    return [r for r in rows if _eval_where(where, r)]


def _order_path(item: dict) -> tuple[list[str], str]:
    """{ a: { b: asc } } gives (["a", "b"], "asc")."""
    path: list[str] = []
    while isinstance(item, dict) and len(item) == 1:
        (field, item), = item.items()
        path.append(field)
    if not path or item not in ('asc', 'desc'):
        raise GraphQLServerError("invalid order_by expression")
    return path, item


def _value_at(row: dict, path: list[str]):
    value = row
    for part in path:
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def _apply_order_by(rows: list[dict], query_str: str) -> list[dict]:
    ob_str = _extract_balanced(query_str, 'order_by', '[')
    if not ob_str:
        return rows
    tokens = _tokenize_gql(ob_str)
    order_list, _ = _parse_gql_array(tokens, 0)
    for item in reversed(order_list):
        path, direction = _order_path(item)
        rows = sorted(
            rows,
            key=lambda r, p=path: (_value_at(r, p) is None, _value_at(r, p)),
            reverse=(direction == 'desc'),
        )
    return rows


def _apply_limit(rows: list[dict], query_str: str) -> list[dict]:
    m = re.search(r'\blimit:\s*(\d+)', query_str)
    return rows[:int(m.group(1))] if m else rows



# ---------------------------------------------------------------------------
# Tables and server
# ---------------------------------------------------------------------------

@dataclass
class NestedVersions:
    """A nested object resolved from a milestoned set of rows, as of the query's asOf (null if none is live)."""

    foreign_key: str  # row key holding the id of the nested object
    versions: list[dict]  # each has an "id", plus _in_z / _out_z when the nested object is milestoned


@dataclass
class GraphQLTable:
    """Rows served for one root query field.

    Keys starting with "_" are internal: they are used for milestoning and never returned.
    """

    rows: list[dict]
    # the root query accepts asOf, filtering on the rows' _in_z / _out_z
    processing_milestoned: bool = False
    # the root query accepts businessDate, matching this row key
    business_date_key: str | None = None
    # nested object fields, keyed by field name
    nested: dict[str, NestedVersions] | None = None


def _strip_internal_value(value):
    if isinstance(value, dict):
        return {k: _strip_internal_value(v) for k, v in value.items() if not k.startswith("_")}
    return value


def _strip_internal(rows: list[dict]) -> list[dict]:
    return [_strip_internal_value(r) for r in rows]


def _resolve_nested(rows: list[dict], nested: dict[str, NestedVersions], as_of_str: str | None) -> list[dict]:
    as_of = _parse_dt(as_of_str) if as_of_str else None

    def resolve(spec: NestedVersions, row: dict) -> dict | None:
        candidates = [v for v in spec.versions if v["id"] == row[spec.foreign_key]]
        if as_of is not None:
            candidates = [v for v in candidates if "_in_z" not in v] + \
                _filter_processing([v for v in candidates if "_in_z" in v], as_of, "_in_z", "_out_z")
        if not candidates:
            return None
        return max(candidates, key=lambda v: v.get("_in_z", ""))

    return [
        {**row, **{name: resolve(spec, row) for name, spec in nested.items()}}
        for row in rows
    ]


def _serve_query(tables: dict[str, GraphQLTable], query_str: str) -> dict:
    m = re.match(r'\s*\{\s*(\w+)', query_str)
    if not m or m.group(1) not in tables:
        return {}
    name = m.group(1)
    table = tables[name]
    rows = table.rows
    if table.business_date_key:
        bd_str = _extract_arg(query_str, "businessDate")
        if bd_str:
            rows = [r for r in rows if r[table.business_date_key] == bd_str]
    if table.processing_milestoned:
        as_of_str = _extract_arg(query_str, "asOf")
        if as_of_str:
            rows = _filter_processing(rows, _parse_dt(as_of_str), "_in_z", "_out_z")
    if table.nested:
        rows = _resolve_nested(rows, table.nested, _extract_arg(query_str, "asOf"))
    rows = _apply_where(rows, query_str)
    rows = _apply_order_by(rows, query_str)
    rows = _apply_limit(rows, query_str)
    return {name: _strip_internal(rows)}


def start_graphql_server(tables: dict[str, GraphQLTable]) -> HTTPServer:
    """Start a mock GraphQL server on a free port in a background thread."""

    class Handler(BaseHTTPRequestHandler):

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length))
            try:
                response = {"data": _serve_query(tables, body.get("query", ""))}
            except GraphQLServerError as e:
                response = {"errors": [{"message": str(e)}]}
            payload = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):  # type: ignore[override]
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def endpoint_url(server: HTTPServer) -> str:
    host, port = server.server_address[:2]
    return f"http://{host}:{port}/graphql"


# ---------------------------------------------------------------------------
# Tables built from CSV files
# ---------------------------------------------------------------------------

def csv_table(csv_name: str, fields: dict[str, str | tuple[str, type]],
              internal: dict[str, tuple[str, type]] | None = None, **table_kwargs) -> GraphQLTable:
    """Build a table from a CSV file.

    fields maps each GraphQL field to its CSV column, or to (column, type) when the value is not text.
    internal does the same for the keys that are never returned (foreign keys).
    An empty CSV cell is null.
    """
    def convert(raw: str, to: type):
        return None if raw == "" else to(raw)

    def pick(row: dict, spec: str | tuple[str, type]):
        column, to = (spec, str) if isinstance(spec, str) else spec
        return convert(row[column], to)

    rows = []
    for r in _read_csv(csv_name):
        row = {field: pick(r, spec) for field, spec in fields.items()}
        row.update({key: pick(r, spec) for key, spec in (internal or {}).items()})
        rows.append(row)
    return GraphQLTable(rows, **table_kwargs)


# ---------------------------------------------------------------------------
# Finance data, shaped as GraphQL responses
# ---------------------------------------------------------------------------

def finance_tables(accounts_csv: str = "finance_accounts.csv",
                   trades_csv: str = "finance_trades.csv",
                   branches_csv: str = "finance_branches.csv") -> dict[str, GraphQLTable]:
    branches = [{"id": int(r["ID"]), "city": r["CITY"]} for r in _read_csv(branches_csv)]
    branch_index = {b["id"]: b for b in branches}
    versions = [
        {"id": int(r["ID"]), "name": r["ACCT_NAME"], "branch": branch_index[int(r["BRANCH_ID"])],
         "_in_z": r.get("in_z", "1900-01-01 00:00:00"), "_out_z": r.get("out_z", "9999-12-31 23:59:59")}
        for r in _read_csv(accounts_csv)
    ]
    # the account query is not milestoned: GraphQL serves only the current (latest) version of each
    current = {v["id"]: v for v in sorted(versions, key=lambda v: v["_in_z"])}
    accounts = [{"id": v["id"], "name": v["name"], "branch": v["branch"]} for v in current.values()]

    trades = [
        {
            "symbol": r["sym"],
            "price": float(r["price"]),
            "isSettled": r["is_settled"].lower() == "true",
            "validFrom": r["in_z"],
            "validTo": r["out_z"],
            "_account_id": int(r["account_id"]),
            "_in_z": r["in_z"],
            "_out_z": r["out_z"],
        }
        for r in _read_csv(trades_csv)
    ]

    positions = [
        {
            "businessDate": r["DATE"],
            "quantity": float(r["QUANTITY"]),
            "npv": float(r["NPV"]),
            "_in_z": r["in_z"],
            "_out_z": r["out_z"],
        }
        for r in _read_csv("finance_positions.csv")
    ]

    return {
        "accounts": GraphQLTable(accounts),
        "branches": GraphQLTable(branches),
        "trades": GraphQLTable(trades, processing_milestoned=True,
                               nested={"account": NestedVersions("_account_id", versions)}),
        "contractualPositions": GraphQLTable(positions, processing_milestoned=True,
                                             business_date_key="businessDate"),
    }
