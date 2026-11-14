"""The steps written after a query: of the rows of another query, in the order they were written, and nothing of the SELECTs that say them."""
from model.relational import (
    DistinctRows, DroppedRows, NoOperation, QueryExpression, QueryStep, QuerySpecification, SortDirection, SortOperation, SortedRows,
    Table, TakenRows)

_TABLE = Table("accounts", [("ID", "INT"), ("NAME", "VARCHAR")])


def _query() -> QuerySpecification:
    return QuerySpecification(list(_TABLE.columns), NoOperation(), table=_TABLE)


def test_a_step_is_a_query_expression_of_the_rows_of_the_one_before_it():
    query = _query()
    for step in (DistinctRows(query), SortedRows(query, []), DroppedRows(query, 1), TakenRows(query, 1)):
        assert isinstance(step, QueryExpression) and isinstance(step, QueryStep)
        assert step.source is query and step.columns == query.columns


def test_a_step_has_what_it_says_of_the_rows():
    query = _query()
    sorts = [SortOperation(_TABLE.columns[0], SortDirection.DESC)]
    assert SortedRows(query, sorts).sorts == tuple(sorts)
    assert DroppedRows(query, 2).count == 2 and TakenRows(query, 3).count == 3


def test_the_order_the_steps_are_written_in_is_how_they_are_nested():
    query = _query()
    taken_then_sorted = SortedRows(TakenRows(query, 3), [])
    sorted_then_taken = TakenRows(SortedRows(query, []), 3)
    assert isinstance(taken_then_sorted.source, TakenRows) and isinstance(sorted_then_taken.source, SortedRows)
    assert taken_then_sorted.columns == sorted_then_taken.columns == query.columns
