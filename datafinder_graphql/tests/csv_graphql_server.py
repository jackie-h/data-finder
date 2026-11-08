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
    if op_name in ('_in', '_nin'):
        member = any(_comparable(raw, t)[0] == _comparable(raw, t)[1] for t in target)
        return member if op_name == '_in' else not member
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
            # a condition on a nested object matches only rows that have one, as a join would;
            # on a list of objects it matches a row when any of them does
            if isinstance(raw, list):
                if not any(_eval_where(cond, item) for item in raw):
                    return False
            elif raw is None or not _eval_where(cond, raw):
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
        if isinstance(value, list):
            raise GraphQLServerError("order_by on an array relationship needs an aggregate")
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    if isinstance(value, list):
        raise GraphQLServerError("order_by on an array relationship needs an aggregate")
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
class NestedRows:
    """A list of nested objects: the rows whose ``foreign_key`` holds the ``key`` of the row they are nested in.

    They are milestoned if they have _in_z / _out_z, and have a businessDate if ``business_date_key`` names it."""

    key: str  # key of the row they are nested in
    foreign_key: str  # key of each nested row that holds it
    rows: list[dict]
    business_date_key: str | None = None


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
    nested: dict[str, NestedVersions | NestedRows] | None = None


def _parse_selection(s: str, pos: int = 0) -> tuple[dict, int]:
    """Parse "{ a b { c } d(arg: 1) { e } }" into {"a": (None, ""), "b": ({"c": (None, "")}, ""), "d": ({"e": ...}, "arg: 1")}:
    each field maps to its sub-selection and the text of its arguments."""
    fields: dict = {}
    pos += 1  # skip '{'
    while pos < len(s) and s[pos] != '}':
        if s[pos] in ' \t\n\r,':
            pos += 1
            continue
        m = re.compile(r'\w+').match(s, pos)
        if not m:
            raise GraphQLServerError(f"cannot parse the selection at {pos}")
        name, pos = m.group(), m.end()
        while pos < len(s) and s[pos] in ' \t\n\r':
            pos += 1
        args = ""
        if pos < len(s) and s[pos] == '(':
            args_start = pos + 1
            depth = 0
            while pos < len(s):
                if s[pos] == '"':
                    pos = s.index('"', pos + 1)
                elif s[pos] == '(':
                    depth += 1
                elif s[pos] == ')':
                    depth -= 1
                    if depth == 0:
                        args = s[args_start:pos]
                        pos += 1
                        break
                pos += 1
            while pos < len(s) and s[pos] in ' \t\n\r':
                pos += 1
        sub = None
        if pos < len(s) and s[pos] == '{':
            sub, pos = _parse_selection(s, pos)
        fields[name] = (sub, args)
    return fields, pos + 1


def _select(value, selection: dict | None, nested: dict | None = None, as_of: datetime.datetime | None = None,
            business_date: str | None = None):
    """Return only the requested fields, as a server does. A nested relation is read at the asOf and businessDate of its
    own arguments, else of the field it is in."""
    if selection is None:
        return value
    if isinstance(value, list):
        return [_select(item, selection, nested, as_of, business_date) for item in value]
    if value is None:
        return None
    return {name: _select_field(value, name, sub, args, nested, as_of, business_date)
            for name, (sub, args) in selection.items()}


def _select_field(row: dict, name: str, selection: dict | None, args: str, nested: dict | None,
                  as_of: datetime.datetime | None, business_date: str | None):
    own_as_of, own_business_date = _extract_arg(args, "asOf"), _extract_arg(args, "businessDate")
    as_of = _parse_dt(own_as_of) if own_as_of else as_of
    business_date = own_business_date or business_date
    if nested and name in nested:
        value = _resolve_relation(nested[name], row, as_of, business_date)
    else:
        value = row.get(name)
        if own_as_of or own_business_date:
            raise GraphQLServerError("field does not accept a date")
    if args and (_extract_balanced(args, "where", "{") or "order_by" in args or re.search(r"\blimit:", args)):
        # a list relationship takes where, order_by and limit; an object relationship takes no arguments
        if not isinstance(value, list):
            raise GraphQLServerError("field does not accept arguments")
        value = _apply_limit(_apply_order_by(_apply_where(value, args), args), args)
    return _select(value, selection, None, as_of, business_date)


def _resolve_relation(spec: NestedVersions | NestedRows, row: dict, as_of: datetime.datetime | None,
                      business_date: str | None):
    """A nested object (the latest version live at as_of, null if none) or a list of them."""
    if isinstance(spec, NestedVersions):
        candidates = [v for v in spec.versions if v["id"] == row[spec.foreign_key]]
        if as_of is not None:
            candidates = [v for v in candidates if "_in_z" not in v] + \
                _filter_processing([v for v in candidates if "_in_z" in v], as_of, "_in_z", "_out_z")
        if not candidates:
            return None
        return max(candidates, key=lambda v: v.get("_in_z", ""))
    rows = [r for r in spec.rows if r[spec.foreign_key] == row[spec.key]]
    if as_of is not None:
        rows = [r for r in rows if "_in_z" not in r] + _filter_processing(
            [r for r in rows if "_in_z" in r], as_of, "_in_z", "_out_z")
    if business_date and spec.business_date_key:
        rows = [r for r in rows if r[spec.business_date_key] == business_date]
    return rows


def _serve_query(tables: dict[str, GraphQLTable], query_str: str) -> dict:
    selection, _ = _parse_selection(query_str[query_str.index('{'):])
    if len(selection) != 1:
        raise GraphQLServerError("expected a single root field")
    (name, (sub_selection, args)), = selection.items()
    if name not in tables:
        return {}
    table = tables[name]
    rows = table.rows
    # the arguments of the root field alone: a nested field's where must not filter the root rows
    if table.business_date_key:
        bd_str = _extract_arg(args, "businessDate")
        if bd_str:
            rows = [r for r in rows if r[table.business_date_key] == bd_str]
    if table.processing_milestoned:
        as_of_str = _extract_arg(args, "asOf")
        if as_of_str:
            rows = _filter_processing(rows, _parse_dt(as_of_str), "_in_z", "_out_z")
    # a nested relation is read at the root's asOf and businessDate unless it is given its own
    as_of_str = _extract_arg(args, "asOf") if table.processing_milestoned else None
    as_of = _parse_dt(as_of_str) if as_of_str else None
    business_date = _extract_arg(args, "businessDate") if table.business_date_key else None
    if table.nested:
        # the root's where and order_by can name a nested field, which they see as of the root's dates
        seen = [{**row, **{key: _resolve_relation(spec, row, as_of, business_date) for key, spec in table.nested.items()},
                 "__row": row} for row in rows]
        seen = _apply_limit(_apply_order_by(_apply_where(seen, args), args), args)
        rows = [row["__row"] for row in seen]
    else:
        rows = _apply_limit(_apply_order_by(_apply_where(rows, args), args), args)
    return {name: _select(rows, sub_selection, table.nested, as_of, business_date)}


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
                   branches_csv: str = "finance_branches.csv",
                   desks_csv: str = "finance_desks.csv") -> dict[str, GraphQLTable]:
    branches = [{"id": int(r["ID"]), "city": r["CITY"]} for r in _read_csv(branches_csv)]
    branch_index = {b["id"]: b for b in branches}
    desks = [{"id": int(r["ID"]), "name": r["NAME"]} for r in _read_csv(desks_csv)]
    desk_index = {d["id"]: d for d in desks}
    # an account is milestoned in processing time: every version is served, as of the query's asOf
    versions = [
        {"id": int(r["ID"]), "name": r["ACCT_NAME"], "branch": branch_index[int(r["BRANCH_ID"])],
         "desk": desk_index.get(int(r["DESK_ID"])) if r.get("DESK_ID") else None,
         "_branch_id": int(r["BRANCH_ID"]), "_desk_id": int(r["DESK_ID"]) if r.get("DESK_ID") else None,
         "_in_z": r.get("in_z", "1900-01-01 00:00:00"), "_out_z": r.get("out_z", "9999-12-31 23:59:59")}
        for r in _read_csv(accounts_csv)
    ]

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
            "_account_id": int(r["ACCOUNT_ID"]),
            "_in_z": r["in_z"],
            "_out_z": r["out_z"],
        }
        for r in _read_csv("finance_positions.csv")
    ]

    return {
        "accounts": GraphQLTable(versions, processing_milestoned=True,
                                 nested={"positions": NestedRows("id", "_account_id", positions, "businessDate")}),
        "branches": GraphQLTable(branches, nested={"accounts": NestedRows("id", "_branch_id", versions)}),
        "desks": GraphQLTable(desks, nested={"accounts": NestedRows("id", "_desk_id", versions)}),
        "trades": GraphQLTable(trades, processing_milestoned=True,
                               nested={"account": NestedVersions("_account_id", versions)}),
        "contractualPositions": GraphQLTable(positions, processing_milestoned=True,
                                             business_date_key="businessDate",
                                             nested={"account": NestedVersions("_account_id", versions)}),
    }
