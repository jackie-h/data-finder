"""
A finder query as JSON, and back.

``record`` runs the lambda of a spec against a recording stand-in for the finder, so every call, operator, index and nested lambda
that builds the query is written down as a node of a tree, not run. ``replay`` walks that tree against a real finder and gives the query.
The tree is the calls of the finder API, so a runner in another language reads it as a chain of method calls on its own finder.

A node is a JSON object with a ``kind``:

    finder                                the finder the query reads
    global     name                       a function of the API that is not a method: constant, count_all, when, today, ...
    param      name                       a parameter of a lambda in the query
    attr       on, name                   ``on.name``
    call       fn, args, kwargs           ``fn(*args, **kwargs)``; a method call is a call of an attr
    binary     op, left, right            == != < <= > >= & | + - * / // % **
    unary      op, operand                ~ - +
    index      on, key                    ``on[key]``
    lambda     params, body               a function of the query, for filter and the like
    list       items
    slice      start, stop, step
    dict       items
    decimal | date | datetime | interval  value   a typed literal (an interval is in microseconds)
    enum       type, name                 a member of an enum of the API, such as a day of the week
    let        id, value                  a value that the query uses more than once, made here, the first time it is met
    ref        id                         the value of the ``let`` with that id: the same object, not another like it

A plain JSON string, number, boolean or null is that literal.
"""
from __future__ import annotations

import contextlib
import datetime
import decimal
import enum
import inspect
import operator
import sys
from typing import Any, Callable, Iterator

import numpy as np

# The functions of the API that a spec names without a finder: while a query is recorded they are recorded, not run.
GLOBAL_FUNCTIONS = ["constant", "count_all", "today", "now", "first_day_of_this_month", "first_day_of_this_quarter",
                    "first_day_of_this_year", "most_recent_day_of_week", "previous_day_of_week", "when", "exists"]

_BINARY = {"==": operator.eq, "!=": operator.ne, "<": operator.lt, "<=": operator.le, ">": operator.gt, ">=": operator.ge,
           "&": operator.and_, "|": operator.or_, "+": operator.add, "-": operator.sub, "*": operator.mul,
           "/": operator.truediv, "//": operator.floordiv, "%": operator.mod, "**": operator.pow}
_UNARY = {"~": operator.invert, "-": operator.neg, "+": operator.pos}


class _Sym:
    """What the finder, and everything read from it, is while a query is recorded: it holds the node that says how it was made."""

    def __init__(self, node: dict):
        object.__setattr__(self, "_node", node)

    def __getattr__(self, name: str) -> _Sym:
        if name.startswith("__"):  # numpy, pandas and the like probe for these
            raise AttributeError(name)
        return _Sym({"kind": "attr", "on": self._node, "name": name})

    def __call__(self, *args: Any, **kwargs: Any) -> _Sym:
        return _Sym({"kind": "call", "fn": self._node, "args": [_encode(a) for a in args],
                     "kwargs": {k: _encode(v) for k, v in kwargs.items()}})

    def __getitem__(self, key: Any) -> _Sym:
        return _Sym({"kind": "index", "on": self._node, "key": _encode(key)})

    def __bool__(self) -> bool:
        raise TypeError("A recorded query cannot branch on a value of the query: write it with the operators of the API")

    __hash__ = object.__hash__


def _binary(symbol: str, reflected: bool = False) -> Callable[[_Sym, Any], _Sym]:
    def method(self: _Sym, other: Any) -> _Sym:
        left, right = (_encode(other), self._node) if reflected else (self._node, _encode(other))
        return _Sym({"kind": "binary", "op": symbol, "left": left, "right": right})
    return method


for _name, _symbol in [("eq", "=="), ("ne", "!="), ("lt", "<"), ("le", "<="), ("gt", ">"), ("ge", ">="), ("and", "&"), ("or", "|"),
                       ("add", "+"), ("sub", "-"), ("mul", "*"), ("truediv", "/"), ("floordiv", "//"), ("mod", "%"), ("pow", "**")]:
    setattr(_Sym, f"__{_name}__", _binary(_symbol))
for _name, _symbol in [("and", "&"), ("or", "|"), ("add", "+"), ("sub", "-"), ("mul", "*"), ("truediv", "/"), ("floordiv", "//"),
                       ("mod", "%"), ("pow", "**")]:
    setattr(_Sym, f"__r{_name}__", _binary(_symbol, reflected=True))
for _name, _symbol in [("invert", "~"), ("neg", "-"), ("pos", "+")]:
    setattr(_Sym, f"__{_name}__", lambda self, symbol=_symbol: _Sym({"kind": "unary", "op": symbol, "operand": self._node}))


def _encode(value: Any) -> Any:
    """The JSON for a value in a query: a node for what was recorded, a literal for what is a value."""
    if isinstance(value, _Sym):
        return value._node
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, (np.integer, np.floating, np.bool_)):
        return _encode(value.item())
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError(f"A query cannot hold the float {value}")
        return value
    if isinstance(value, decimal.Decimal):
        return {"kind": "decimal", "value": str(value)}
    if isinstance(value, datetime.datetime):
        return {"kind": "datetime", "value": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"kind": "date", "value": value.isoformat()}
    if isinstance(value, datetime.timedelta):
        return {"kind": "interval", "value": value // datetime.timedelta(microseconds=1)}
    if isinstance(value, enum.Enum):
        return {"kind": "enum", "type": type(value).__name__, "name": value.name}
    if isinstance(value, (list, tuple, np.ndarray)):
        return {"kind": "list", "items": [_encode(v) for v in value]}
    if isinstance(value, slice):
        return {"kind": "slice", "start": _encode(value.start), "stop": _encode(value.stop), "step": _encode(value.step)}
    if isinstance(value, dict):
        return {"kind": "dict", "items": {str(k): _encode(v) for k, v in value.items()}}
    if callable(value):
        names = list(inspect.signature(value).parameters)
        return {"kind": "lambda", "params": names, "body": _encode(value(*[_Sym({"kind": "param", "name": n}) for n in names]))}
    raise TypeError(f"A query cannot hold a {type(value).__name__}: {value!r}")


@contextlib.contextmanager
def _recording_globals() -> Iterator[None]:
    """Replace, in every module of the specs, each function of the API a spec names without a finder by its recorded stand-in."""
    import datafinder
    real = {name: getattr(datafinder, name) for name in GLOBAL_FUNCTIONS}
    swapped: list[tuple[Any, str, Any]] = []
    for module in list(sys.modules.values()):
        if module is None or not getattr(module, "__name__", "").startswith("datafinder_examples_tests"):
            continue
        for name, function in real.items():
            if getattr(module, name, None) is function:
                swapped.append((module, name, function))
                setattr(module, name, _Sym({"kind": "global", "name": name}))
    try:
        yield
    finally:
        for module, name, function in swapped:
            setattr(module, name, function)


_SHARED_KINDS = {"call", "attr", "binary", "unary", "index"}


def _share(tree: Any) -> Any:
    """The tree with each node that the query uses more than once made once, as a ``let``, and named by a ``ref`` where it is used again.

    A table that a query reads twice must be the same table each time: ``result.order_by(result["a"])`` orders by a column of the
    result it is ordering, and the same tree made twice would be two tables.
    """
    counts: dict[int, int] = {}

    def count(n: Any) -> None:
        if isinstance(n, list):
            for item in n:
                count(item)
        elif isinstance(n, dict):
            key = id(n)
            counts[key] = counts.get(key, 0) + 1
            if counts[key] == 1:
                for value in n.values():
                    count(value)

    count(tree)
    names: dict[int, int] = {}
    inside_lambda = [0]

    def build(n: Any) -> Any:
        if isinstance(n, list):
            return [build(item) for item in n]
        if not isinstance(n, dict):
            return n
        key = id(n)
        if n.get("kind") in _SHARED_KINDS and counts[key] > 1:
            if key in names:
                return {"kind": "ref", "id": names[key]}
            if inside_lambda[0]:
                raise ValueError("A value used more than once is first met inside a lambda: make it before the lambda")
            names[key] = len(names) + 1
            return {"kind": "let", "id": names[key], "value": rebuild(n)}
        return rebuild(n)

    def rebuild(n: dict) -> dict:
        if n.get("kind") == "lambda":
            inside_lambda[0] += 1
            try:
                return {k: build(v) for k, v in n.items()}
            finally:
                inside_lambda[0] -= 1
        return {k: build(v) for k, v in n.items()}

    return build(tree)


def record(query: Callable[[Any], Any]) -> dict:
    """The tree of the query a spec's lambda builds from the finder."""
    with _recording_globals():
        body = query(_Sym({"kind": "finder"}))
    return _share(_encode(body))


def _enum_type(name: str) -> type[enum.Enum]:
    import datafinder
    import model.relational
    for module in (datafinder, model.relational):
        found = getattr(module, name, None)
        if isinstance(found, type) and issubclass(found, enum.Enum):
            return found
    raise ValueError(f"No enum {name} in the API")


def replay(node: Any, finder: Any, env: dict[str, Any] | None = None, made: dict[int, Any] | None = None) -> Any:
    """Run a recorded query against a finder and give the query."""
    import datafinder
    env = env or {}
    made = {} if made is None else made

    def run(n: Any) -> Any:
        if not isinstance(n, dict):
            return n
        kind = n["kind"]
        if kind == "finder":
            return finder
        if kind == "global":
            return getattr(datafinder, n["name"])
        if kind == "param":
            return env[n["name"]]
        if kind == "attr":
            return getattr(run(n["on"]), n["name"])
        if kind == "call":
            return run(n["fn"])(*[run(a) for a in n["args"]], **{k: run(v) for k, v in n["kwargs"].items()})
        if kind == "binary":
            return _BINARY[n["op"]](run(n["left"]), run(n["right"]))
        if kind == "unary":
            return _UNARY[n["op"]](run(n["operand"]))
        if kind == "index":
            return run(n["on"])[run(n["key"])]
        if kind == "lambda":
            return lambda *args: replay(n["body"], finder, {**env, **dict(zip(n["params"], args))}, dict(made))
        if kind == "let":
            made[n["id"]] = run(n["value"])
            return made[n["id"]]
        if kind == "ref":
            return made[n["id"]]
        if kind == "list":
            return [run(i) for i in n["items"]]
        if kind == "slice":
            return slice(run(n["start"]), run(n["stop"]), run(n["step"]))
        if kind == "dict":
            return {k: run(v) for k, v in n["items"].items()}
        if kind == "decimal":
            return decimal.Decimal(n["value"])
        if kind == "date":
            return datetime.date.fromisoformat(n["value"])
        if kind == "datetime":
            return datetime.datetime.fromisoformat(n["value"])
        if kind == "interval":
            return datetime.timedelta(microseconds=n["value"])
        if kind == "enum":
            return _enum_type(n["type"])[n["name"]]
        raise ValueError(f"Unknown node kind {kind!r}")

    return run(node)


def render(node: Any) -> str:
    """The query as the Python that builds it, for a person to read: the tree is what is run, this is only a way to see it."""
    if not isinstance(node, dict):
        return repr(node) if not isinstance(node, str) else json_string(node)
    kind = node["kind"]
    if kind == "finder":
        return "f"
    if kind in ("global", "param"):
        return node["name"]
    if kind == "attr":
        return f"{render(node['on'])}.{node['name']}"
    if kind == "call":
        arguments = [render(a) for a in node["args"]] + [f"{k}={render(v)}" for k, v in node["kwargs"].items()]
        return f"{render(node['fn'])}({', '.join(arguments)})"
    if kind == "binary":
        return f"({render(node['left'])} {node['op']} {render(node['right'])})"
    if kind == "unary":
        return f"{node['op']}{render(node['operand'])}"
    if kind == "index":
        return f"{render(node['on'])}[{render(node['key'])}]"
    if kind == "slice":
        parts = [render(node[k]) if node[k] is not None else "" for k in ("start", "stop", "step")]
        return ":".join(parts).rstrip(":")
    if kind == "lambda":
        return f"lambda {', '.join(node['params'])}: {render(node['body'])}"
    if kind == "list":
        return f"[{', '.join(render(i) for i in node['items'])}]"
    if kind == "dict":
        return "{" + ", ".join(f"{json_string(k)}: {render(v)}" for k, v in node["items"].items()) + "}"
    if kind == "enum":
        return f"{node['type']}.{node['name']}"
    if kind == "let":
        return f"(v{node['id']} := {render(node['value'])})"
    if kind == "ref":
        return f"v{node['id']}"
    return f"{kind}({node['value']!r})"


def json_string(text: str) -> str:
    import json
    return json.dumps(text, ensure_ascii=False)
