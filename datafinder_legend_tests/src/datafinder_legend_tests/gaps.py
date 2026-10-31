"""Check that a known gap is only the difference it claims, so it cannot hide another one.

A known gap makes a failing SQL comparison an expected failure. To be sure the failure is for the stated reason, the
two statements are compared again with just that difference set aside: they must then match.
"""
from __future__ import annotations

from collections.abc import Callable
from functools import reduce

from sqlglot import exp

from datafinder_examples_tests.sql_compare import _connect, _operands, normalize

from datafinder_legend_tests.legend import (
    CONCAT_FUNCTION, DATETIME_MILESTONING_DATE, JOIN_OPERAND_ORDER_AND_DATETIME_MILESTONING_DATE, NOT_EQUAL_NULL, FLOAT_AVERAGE, JOIN_OPERAND_ORDER, JOIN_OPERAND_ORDER_AND_FLOAT_AVERAGE, FLOAT_DIVISION, JOIN_OPERAND_ORDER_AND_FLOAT_DIVISION, TDS_FILTER_OPTIONAL_PROPERTY, TYPED_DATE_LITERAL,
)


def _swap_equality_operands(tree: exp.Expr) -> exp.Expr:
    """a = b is b = a."""
    for eq in tree.find_all(exp.EQ, exp.NEQ):
        if eq.left.sql() > eq.right.sql():
            left, right = eq.left.copy(), eq.right.copy()
            eq.set("this", right)
            eq.set("expression", left)
    return tree



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


# gap reason -> the differences that reason covers, set aside on both statements before comparing
_COVERED: dict[str, list[Callable[[exp.Expr], exp.Expr]]] = {
    JOIN_OPERAND_ORDER: [_swap_equality_operands],
    TDS_FILTER_OPTIONAL_PROPERTY: [_drop_not_null_guards],
    TYPED_DATE_LITERAL: [_untype_date_literals],
    DATETIME_MILESTONING_DATE: [_truncate_datetime_date],
    JOIN_OPERAND_ORDER_AND_DATETIME_MILESTONING_DATE: [_swap_equality_operands, _truncate_datetime_date],
    CONCAT_FUNCTION: [_concat_function_to_pipes],
    FLOAT_AVERAGE: [_drop_float_cast_in_average],
    FLOAT_DIVISION: [_drop_float_cast_in_division],
    JOIN_OPERAND_ORDER_AND_FLOAT_DIVISION: [_swap_equality_operands, _drop_float_cast_in_division],
    JOIN_OPERAND_ORDER_AND_FLOAT_AVERAGE: [_swap_equality_operands, _drop_float_cast_in_average],
}


def checkable(reason: str) -> bool:
    return reason in _COVERED


def gap_is_only(reason: str, actual_sql: str, expected_sql: str) -> bool:
    """Whether the two statements match once the difference named by ``reason`` is set aside."""
    steps = _COVERED[reason]

    def prepared(sql: str) -> exp.Expr:
        return reduce(lambda tree, step: step(tree), steps, normalize(sql))

    return prepared(actual_sql) == prepared(expected_sql)
