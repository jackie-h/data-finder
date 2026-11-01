"""Check that a known gap is only the difference it claims, so it cannot hide another one.

A known gap makes a failing SQL comparison an expected failure. To be sure the failure is for the stated reason, the
two statements are compared again with just that difference set aside: they must then match.
"""
from __future__ import annotations

from collections.abc import Callable
from functools import reduce

from sqlglot import exp

from datafinder_examples_tests.sql_compare import (
    _canonical_aliases, _connect, _flatten_connectors, _operands, _order_join_equalities, _own, normalize,
)

from datafinder_legend_tests.legend import (
    CONCAT_FUNCTION, DATETIME_MILESTONING_DATE, EXISTS_SUB_SELECT_ORDER, ISOLATED_MILESTONED_JOIN, LEFT_JOIN_ORDER, NOT_EQUAL_NULL, FLOAT_AVERAGE, FLOAT_DIVISION, TDS_FILTER_OPTIONAL_PROPERTY, TYPED_DATE_LITERAL,
)



def _is_not_null(node: exp.Expr) -> bool:
    return isinstance(node, exp.Not) and isinstance(node.this, exp.Is) and isinstance(node.this.expression, exp.Null)


def _drop_not_null_guards(tree: exp.Expr) -> exp.Expr:
    """``col IS NOT NULL AND col > 30`` is ``col > 30``: a comparison is never true for NULL."""
    for node in reversed(list(tree.find_all(exp.And))):
        parent = node.parent
        if isinstance(parent, exp.And):
            continue
        operands = _operands(node, exp.And)
        guarded = {o.this.this.sql() for o in operands if _is_not_null(o)}
        kept = [o for o in operands
                if not (_is_not_null(o) and any(o2 is not o and not _is_not_null(o2)
                                                and o.this.this.sql() in o2.sql() for o2 in operands))]
        if len(kept) != len(operands):
            node.replace(reduce(lambda left, right: _connect(exp.And, left, right), kept[1:], kept[0]))
    # parentheses that grouped the guard with its comparison now group one predicate
    for paren in list(tree.find_all(exp.Paren)):
        if isinstance(paren.this, exp.Predicate | exp.Binary) and not isinstance(paren.this, exp.Connector):
            paren.replace(paren.this)
    return tree


def _untype_date_literals(tree: exp.Expr) -> exp.Expr:
    """``DATE '2016-08-10'`` is ``'2016-08-10'``: a date literal without its type."""
    for cast in list(tree.find_all(exp.Cast)):
        if isinstance(cast.this, exp.Literal) and cast.this.is_string and cast.to.this in (
                exp.DataType.Type.DATE, exp.DataType.Type.TIMESTAMP, exp.DataType.Type.DATETIME):
            cast.replace(cast.this.copy())
    return tree


def _truncate_datetime_date(tree: exp.Expr) -> exp.Expr:
    """``cast(truncate('2015-08-26 02:00:00') as date)`` is ``'2015-08-26'``: the date of a DateTime literal."""
    for cast in list(tree.find_all(exp.Cast)):
        call = cast.this
        if cast.to.this == exp.DataType.Type.DATE and isinstance(call, exp.Anonymous | exp.Trunc) \
                and call.name.lower() in ("truncate", "trunc") or isinstance(call, exp.Trunc):
            arg = call.this if isinstance(call, exp.Trunc) else call.expressions[0]
            if isinstance(arg, exp.Literal) and arg.is_string:
                cast.replace(exp.Literal.string(arg.name[:10]))
    return tree


def _concat_function_to_pipes(tree: exp.Expr) -> exp.Expr:
    """``concat(a, b, c)`` is ``a || b || c``."""
    for call in list(tree.find_all(exp.Concat)):
        parts = [e.copy() for e in call.expressions]
        call.replace(reduce(lambda left, right: exp.DPipe(this=left, expression=right, safe=True), parts[1:], parts[0]))
    return tree


def _drop_float_cast_in_average(tree: exp.Expr) -> exp.Expr:
    """``avg(1.0 * x)`` is ``avg(x)`` for a column that is already a float."""
    for avg in tree.find_all(exp.Avg):
        arg = avg.this
        if isinstance(arg, exp.Mul) and isinstance(arg.left, exp.Literal) and arg.left.name == "1.0":
            arg.replace(arg.right.copy())
    return tree


def _drop_float_cast_in_division(tree: exp.Expr) -> exp.Expr:
    """``(1.0 * x) / y`` is ``x / y`` for a column that is already a float."""
    for div in list(tree.find_all(exp.Div)):
        left = div.this.this if isinstance(div.this, exp.Paren) else div.this
        if isinstance(left, exp.Mul) and isinstance(left.left, exp.Literal) and left.left.name == "1.0":
            div.set("this", left.right.copy())
    return tree


def _drop_null_alternative_to_not_equal(tree: exp.Expr) -> exp.Expr:
    """``col <> x OR col IS NULL`` is ``col <> x``."""
    for node in reversed(list(tree.find_all(exp.Or))):
        if isinstance(node.parent, exp.Or):
            continue
        operands = _operands(node, exp.Or)
        unequal = {o.this.sql() for o in operands if isinstance(o, exp.NEQ)}
        kept = [o for o in operands
                if not (isinstance(o, exp.Is) and isinstance(o.expression, exp.Null) and o.this.sql() in unequal)]
        if len(kept) != len(operands):
            node.replace(reduce(lambda left, right: _connect(exp.Or, left, right), kept[1:], kept[0]))
    return tree


def _inline_isolated_join_filter(tree: exp.Expr) -> exp.Expr:
    """``LEFT JOIN (SELECT c FROM t WHERE p) AS x ON j`` is ``LEFT JOIN t AS x ON j AND p``.

    Only a derived table of one plain table that selects bare columns is inlined, and only under a left join, where a
    row of ``t`` that fails ``p`` and a row that is missing both leave the joined columns NULL."""
    for join in list(tree.find_all(exp.Join)):
        derived = join.this
        if not isinstance(derived, exp.Subquery) or (join.side or "").upper() != "LEFT" or not derived.alias:
            continue
        inner = derived.this
        source = inner.args.get("from_") or inner.args.get("from")
        if not isinstance(inner, exp.Select) or source is None or not isinstance(source.this, exp.Table) \
                or inner.args.get("joins") or inner.args.get("group") or inner.args.get("distinct") \
                or inner.args.get("limit") or inner.args.get("where") is None \
                or not all(isinstance(e, exp.Column) for e in inner.expressions):
            continue
        outer_alias, inner_alias = derived.alias, source.this.alias_or_name
        predicate = inner.args["where"].this.copy()
        for column in predicate.find_all(exp.Column):
            if column.table == inner_alias:
                column.set("table", exp.to_identifier(outer_alias))
        table = source.this.copy()
        table.set("alias", exp.TableAlias(this=exp.to_identifier(outer_alias)))
        condition = join.args.get("on")
        join.set("this", table)
        join.set("on", _connect(exp.And, condition.copy(), predicate) if condition is not None else predicate)
    _canonical_aliases(tree)
    _flatten_connectors(tree)
    _order_join_equalities(tree)
    return tree


def _on_columns(join: exp.Join) -> list[exp.Column]:
    """The columns of a join's ON condition: what it depends on, not the columns inside a derived table it joins."""
    condition = join.args.get("on")
    return list(condition.find_all(exp.Column)) if condition is not None else []


def _order_sub_select_conditions(tree: exp.Expr) -> exp.Expr:
    """Write the and-ed conditions of a derived table's WHERE in one order, since a AND b is b AND a."""
    for derived in list(tree.find_all(exp.Subquery)):
        inner = derived.this
        where = inner.args.get("where") if isinstance(inner, exp.Select) else None
        if where is None:
            continue
        operands = sorted((o.copy() for o in _operands(where.this, exp.And)), key=lambda o: o.sql())
        where.set("this", reduce(lambda left, right: _connect(exp.And, left, right), operands[1:], operands[0]))
    return tree


def _join_source_key(join: exp.Join) -> tuple[str, str]:
    """What a join reads from, without its alias: a table's name, or a derived table's text."""
    source = join.this
    if isinstance(source, exp.Table):
        return ("table", source.name)
    inner = source.this
    return ("derived", inner.sql())


def _canonical_derived_aliases(tree: exp.Expr) -> None:
    """Rename the derived tables a SELECT joins to d0, d1, ... in order of appearance."""
    for select in tree.find_all(exp.Select):
        mapping: dict[str, str] = {}
        for join in select.args.get("joins") or []:
            if isinstance(join.this, exp.Subquery) and join.this.alias:
                mapping.setdefault(join.this.alias.lower(), f"d{len(mapping)}")
        if not mapping:
            continue
        for join in select.args.get("joins") or []:
            if isinstance(join.this, exp.Subquery) and join.this.alias:
                join.this.set("alias", exp.TableAlias(this=exp.to_identifier(mapping[join.this.alias.lower()])))
        for column in select.find_all(exp.Column):
            if _own(select, column) and column.table.lower() in mapping:
                column.set("table", exp.to_identifier(mapping[column.table.lower()]))


def _order_left_joins(tree: exp.Expr) -> exp.Expr:
    """Write the left joins of each SELECT in one order: by what they read from, with a join always after the joins it
    depends on, and name the derived tables by position."""
    for select in tree.find_all(exp.Select):
        joins = select.args.get("joins") or []
        source = select.args.get("from_") or select.args.get("from")
        if source is None or len(joins) < 2 or any((j.side or "").upper() != "LEFT" for j in joins):
            continue
        provided = {source.this.alias_or_name}
        remaining, ordered = list(joins), []
        while remaining:
            ready = [j for j in remaining
                     if {c.table for c in _on_columns(j)} <= provided | {j.this.alias_or_name}]
            if not ready:
                break
            pick = min(ready, key=_join_source_key)
            ordered.append(pick)
            remaining.remove(pick)
            provided.add(pick.this.alias_or_name)
        if not remaining:
            select.set("joins", ordered)
    _canonical_aliases(tree)
    _canonical_derived_aliases(tree)
    _order_join_equalities(tree)
    return tree


# gap reason -> the differences that reason covers, set aside on both statements before comparing
_COVERED: dict[str, list[Callable[[exp.Expr], exp.Expr]]] = {
    TDS_FILTER_OPTIONAL_PROPERTY: [_drop_not_null_guards],
    ISOLATED_MILESTONED_JOIN: [_inline_isolated_join_filter],
    LEFT_JOIN_ORDER: [_order_left_joins],
    EXISTS_SUB_SELECT_ORDER: [_order_sub_select_conditions, _order_left_joins],
    TYPED_DATE_LITERAL: [_untype_date_literals],
    DATETIME_MILESTONING_DATE: [_truncate_datetime_date],
    CONCAT_FUNCTION: [_concat_function_to_pipes],
    FLOAT_AVERAGE: [_drop_float_cast_in_average],
    FLOAT_DIVISION: [_drop_float_cast_in_division],
}


def checkable(reason: str) -> bool:
    return reason in _COVERED


def gap_is_only(reason: str, actual_sql: str, expected_sql: str) -> bool:
    """Whether the two statements match once the difference named by ``reason`` is set aside."""
    steps = _COVERED[reason]

    def prepared(sql: str) -> exp.Expr:
        return reduce(lambda tree, step: step(tree), steps, normalize(sql))

    return prepared(actual_sql) == prepared(expected_sql)
