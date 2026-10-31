"""Strict structural comparison of SQL produced by different generators.

Both statements are parsed. Within each SELECT the table instances are paired up in order of appearance (FROM, then
each JOIN) and renamed to a shared alias, which must refer to the same table on both sides. The expressions are then
compared as trees: columns, join type and condition, predicates, grouping, ordering and literals must all match,
operand order and the order of AND/OR operands included, except for the two sides of an equality in a join condition.

Only these are ignored, because they name things rather than say what the query does:
  - output column names (the alias after AS in the select list); an ORDER BY, GROUP BY or HAVING that names one is
    compared as the expression it stands for
  - table aliases (paired up as above)
  - identifier quoting and case
  - the ``default`` or ``main`` schema qualifier
  - parentheses around a single predicate, e.g. ON (a = b), or around a whole ON or WHERE condition
  - parentheses around an AND directly inside an OR, which binds tighter anyway: x OR (y AND z) is x OR y AND z
  - how a run of the same operator is grouped: AND and OR are associative, so (a AND b) AND c is a AND (b AND c) is
    a AND b AND c. The order of the operands still matters.
  - which side of an equality in a join condition comes first: ON a.x = b.y is ON b.y = a.x. The same equality in a
    WHERE is compared as written, and so is any other comparison.
"""
from __future__ import annotations

from functools import reduce

import sqlglot
import sqlglot.errors
from sqlglot import exp

# Schema that carries no meaning: Legend tables live in the default schema and are written unqualified.
_DEFAULT_SCHEMAS = {"default", "main"}


def _own(select: exp.Select, node: exp.Expr) -> bool:
    return node.find_ancestor(exp.Select) is select


def _scope_tables(select: exp.Select) -> list[exp.Table]:
    """The table instances of one SELECT in the order they appear: FROM first, then each JOIN."""
    tables = [t for t in select.find_all(exp.Table) if _own(select, t)]
    order = {id(t): i for i, t in enumerate(select.find_all(exp.Table))}
    return sorted(tables, key=lambda t: order[id(t)])


def _canonical_aliases(tree: exp.Expr) -> None:
    """Rename the table aliases of each SELECT to t0, t1, ... in order of appearance."""
    for select in tree.find_all(exp.Select):
        mapping: dict[str, str] = {}
        tables = _scope_tables(select)
        for table in tables:
            mapping.setdefault(table.alias_or_name.lower(), f"t{len(mapping)}")
        for table in tables:
            table.set("alias", exp.TableAlias(this=exp.to_identifier(mapping[table.alias_or_name.lower()])))
        for column in select.find_all(exp.Column):
            if _own(select, column) and column.table:
                column.set("table", exp.to_identifier(mapping.get(column.table.lower(), column.table.lower())))


def _operands(node: exp.Expr, connector: type[exp.Connector]) -> list[exp.Expr]:
    """The operands of a run of the same connector, through nesting and parentheses, in order."""
    if isinstance(node, exp.Paren) and isinstance(node.this, connector):
        return _operands(node.this, connector)
    if isinstance(node, connector):
        return _operands(node.left, connector) + _operands(node.right, connector)
    return [node]


def _connect(connector: type[exp.Connector], left: exp.Expr, right: exp.Expr) -> exp.Expr:
    return connector(this=left, expression=right)


def _order_join_equalities(tree: exp.Expr) -> None:
    """Write the two sides of every equality in a join condition in one order, since a = b is b = a: Legend writes a
    join as it was defined and the finder writes the key first, and both are the same join."""
    for join in tree.find_all(exp.Join):
        condition = join.args.get("on")
        if condition is None:
            continue
        for equality in condition.find_all(exp.EQ):
            if equality.left.sql() > equality.right.sql():
                left, right = equality.left.copy(), equality.right.copy()
                equality.set("this", right)
                equality.set("expression", left)


def _flatten_connectors(tree: exp.Expr) -> None:
    """Rewrite every run of AND (or OR) as a left-nested chain of its operands, whatever its original grouping."""
    for connector in (exp.And, exp.Or):
        for node in reversed(list(tree.find_all(connector))):  # innermost first
            parent = node.parent
            if isinstance(parent, connector) or (isinstance(parent, exp.Paren) and isinstance(parent.parent, connector)):
                continue  # inside a run that is flattened from its outermost node
            operands = [operand.copy() for operand in _operands(node, connector)]
            node.replace(reduce(lambda left, right: _connect(connector, left, right), operands[1:], operands[0]))


# Legend writes row limits as ``select top N``, which only the T-SQL grammar reads.
_FALLBACK_DIALECT = "tsql"


def parse(sql: str, dialect: str | None = None) -> exp.Expr:
    """Parse ``sql`` in ``dialect``; if that fails, in the T-SQL grammar (for ``select top N``)."""
    try:
        return sqlglot.parse_one(sql, read=dialect)
    except sqlglot.errors.ParseError:
        return sqlglot.parse_one(sql, read=_FALLBACK_DIALECT)


def _resolve_output_aliases(select: exp.Select) -> None:
    """ORDER BY / GROUP BY / HAVING can name a select-list alias; replace it with the expression it stands for, so
    ``ORDER BY age`` and ``ORDER BY t0.AGE`` compare equal when ``age`` is ``t0.AGE AS age``."""
    aliased = {e.alias.lower(): e.this for e in select.expressions if isinstance(e, exp.Alias)}
    if not aliased:
        return
    for clause in (select.args.get("order"), select.args.get("group"), select.args.get("having")):
        if clause is None:
            continue
        for column in list(clause.find_all(exp.Column)):
            if not column.table and column.name.lower() in aliased:
                column.replace(aliased[column.name.lower()].copy())


def normalize(sql: str, dialect: str | None = None) -> exp.Expr:
    tree = parse(sql, dialect)
    # x IN (a) is x = a
    for node in list(tree.find_all(exp.In)):
        values = node.args.get("expressions") or []
        if len(values) == 1 and not node.args.get("query") and not node.args.get("unnest"):
            node.replace(exp.EQ(this=node.this.copy(), expression=values[0].copy()))
    # LISTAGG(x, sep) is the string aggregate that STRING_AGG(x, sep) is
    for call in list(tree.find_all(exp.Anonymous)):
        if call.name.lower() == "listagg" and len(call.expressions) == 2:
            call.replace(exp.GroupConcat(this=call.expressions[0].copy(), separator=call.expressions[1].copy()))
    # ``FETCH NEXT n ROWS ONLY`` is ``LIMIT n``
    for fetch in list(tree.find_all(exp.Fetch)):
        if fetch.args.get("count") is not None:
            fetch.replace(exp.Limit(expression=fetch.args["count"].copy()))
    for select in tree.find_all(exp.Select):
        _resolve_output_aliases(select)
    for table in tree.find_all(exp.Table):
        if table.args.get("db") and table.args["db"].name.lower() in _DEFAULT_SCHEMAS:
            table.set("db", None)
    for select in tree.find_all(exp.Select):
        select.set("expressions", [e.this if isinstance(e, exp.Alias) else e for e in select.expressions])
    _canonical_aliases(tree)
    for ident in tree.find_all(exp.Identifier):
        ident.set("this", ident.name.lower())
        ident.set("quoted", False)
    for paren in list(tree.find_all(exp.Paren)):
        if isinstance(paren.this, exp.Predicate | exp.Binary) and not isinstance(paren.this, exp.Connector):
            paren.replace(paren.this)
    # AND binds tighter than OR, so parentheses around an AND directly inside an OR group nothing.
    for paren in list(tree.find_all(exp.Paren)):
        if isinstance(paren.this, exp.And) and isinstance(paren.parent, exp.Or):
            paren.replace(paren.this)
    _flatten_connectors(tree)
    _order_join_equalities(tree)
    # Parentheses around the whole ON or WHERE condition group nothing.
    for clause in list(tree.find_all(exp.Join, exp.Where)):
        key = "on" if isinstance(clause, exp.Join) else "this"
        condition = clause.args.get(key)
        if isinstance(condition, exp.Paren):
            clause.set(key, condition.this)
    return tree


def normalize_sql(sql: str, dialect: str | None = None) -> str:
    """The normalized form as text, for failure messages."""
    return normalize(sql, dialect).sql()


def sql_equivalent(actual: str, expected: str, dialect: str | None = None) -> bool:
    return normalize(actual) == normalize(expected, dialect)
