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
    CHAINED_ORDER_BY, JOIN_OPERAND_ORDER, JOIN_OPERAND_ORDER_AND_CHAINED_ORDER_BY, OPTIONAL_PROPERTY,
)


def _swap_equality_operands(tree: exp.Expr) -> exp.Expr:
    """a = b is b = a."""
    for eq in tree.find_all(exp.EQ, exp.NEQ):
        if eq.left.sql() > eq.right.sql():
            left, right = eq.left.copy(), eq.right.copy()
            eq.set("this", right)
            eq.set("expression", left)
    return tree


def _drop_order_by(tree: exp.Expr) -> exp.Expr:
    for select in tree.find_all(exp.Select):
        select.set("order", None)
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
    return tree


# gap reason -> the differences that reason covers, set aside on both statements before comparing
_COVERED: dict[str, list[Callable[[exp.Expr], exp.Expr]]] = {
    JOIN_OPERAND_ORDER: [_swap_equality_operands],
    CHAINED_ORDER_BY: [_drop_order_by],
    JOIN_OPERAND_ORDER_AND_CHAINED_ORDER_BY: [_swap_equality_operands, _drop_order_by],
    OPTIONAL_PROPERTY: [_drop_not_null_guards],
}


def checkable(reason: str) -> bool:
    return reason in _COVERED


def gap_is_only(reason: str, actual_sql: str, expected_sql: str) -> bool:
    """Whether the two statements match once the difference named by ``reason`` is set aside."""
    steps = _COVERED[reason]

    def prepared(sql: str) -> exp.Expr:
        return reduce(lambda tree, step: step(tree), steps, normalize(sql))

    return prepared(actual_sql) == prepared(expected_sql)
