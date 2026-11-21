"""A percentile of a group: ``percentile(0.5)`` is the median."""
import pytest

from datafinder import DoubleAttribute, IntegerAttribute, constant, sql_for_dialect, to_sql
from model.relational import (
    AggregateOperator, DecimalAggregateOperation, DoubleAggregateOperation, IntegerAggregateOperation, NoOperation, Table)

_T = Table("t", [("PRICE", "DOUBLE"), ("QTY", "INT"), ("G", "INT")])
_PRICE = DoubleAttribute("Price", "PRICE", "DOUBLE", _T)
_QTY = IntegerAttribute("Qty", "QTY", "INT", _T)
_G = IntegerAttribute("G", "G", "INT", _T)


def _sql(*columns, group_by=()):
    return to_sql(None, None, list(columns), _T, NoOperation(), group_by=list(group_by))


def test_a_continuous_percentile_is_a_double_of_the_fraction_of_the_group():
    percentile = _QTY.percentile(0.95)
    assert isinstance(percentile, DoubleAggregateOperation)
    assert percentile.operator == AggregateOperator.PERCENTILE_CONT and percentile.percentile == 0.95 and not percentile.from_largest
    assert 'PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CAST(t0.QTY AS DOUBLE PRECISION) ASC)' in _sql(percentile)


def test_a_discrete_percentile_is_of_the_type_of_the_attribute():
    percentile = _QTY.percentile(0.5, continuous=False)
    assert isinstance(percentile, IntegerAggregateOperation) and percentile.operator == AggregateOperator.PERCENTILE_DISC
    assert 'PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY t0.QTY ASC)' in _sql(percentile)
    assert isinstance(_PRICE.percentile(0.5, continuous=False), DoubleAggregateOperation)


def test_a_percentile_can_be_read_from_the_largest_value():
    assert "ORDER BY CAST(t0.QTY AS DOUBLE PRECISION) DESC)" in _sql(_QTY.percentile(0.25, ascending=False))


def test_the_fraction_is_between_0_and_1():
    for bad in (-0.1, 1.5, 95, True, "0.5"):
        with pytest.raises(ValueError, match="a fraction between 0 and 1"):
            _QTY.percentile(bad)  # pyright: ignore[reportArgumentType]
    assert "PERCENTILE_CONT(0.0)" in _sql(_QTY.percentile(0)) and "PERCENTILE_CONT(1.0)" in _sql(_QTY.percentile(1))


def test_an_expression_has_percentiles_too():
    sql = _sql((_PRICE * _QTY).percentile(0.5))
    assert "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(t0.PRICE * t0.QTY AS DOUBLE PRECISION) ASC)" in sql
    assert isinstance(constant(5).percentile(0.5, continuous=False), IntegerAggregateOperation)


def test_a_percentile_is_not_given_a_window():
    with pytest.raises(ValueError, match="not given a window"):
        _QTY.percentile(0.5).over(partition_by=[_G])


def test_a_percentile_is_a_column_of_a_group_and_has_its_alias():
    sql = _sql(_G, _QTY.percentile(0.5).alias("median"), group_by=[_G])
    assert 'AS "median"' in sql and sql.endswith("GROUP BY t0.G")


def test_duckdb_spells_it_as_a_quantile():
    sql = _sql(_QTY.percentile(0.5, continuous=False), _QTY.percentile(0.25, ascending=False))
    duckdb = sql_for_dialect(sql, "duckdb")
    assert "QUANTILE_DISC(t0.QTY, 0.5 ORDER BY t0.QTY ASC)" in duckdb and "QUANTILE_CONT(" in duckdb


def test_a_decimal_attribute_has_a_discrete_percentile_of_its_own_type():
    from datafinder import DecimalAttribute
    amount = DecimalAttribute("Amount", "AMOUNT", "DECIMAL(10, 2)", Table("a", [("AMOUNT", "DECIMAL(10, 2)")]))
    assert isinstance(amount.percentile(0.5, continuous=False), DecimalAggregateOperation)


def test_a_quantile_is_a_percentile_named_as_numpy_names_it():
    def sql(column):
        return _sql(column.alias("x"))  # without the name each gives itself
    assert sql(_QTY.quantile(0.5)) == sql(_QTY.percentile(0.5))
    assert sql(_QTY.quantile(0.9, method="inverted_cdf")) == sql(_QTY.percentile(0.9, continuous=False))
    assert sql((_PRICE * _QTY).quantile(0.5)) == sql((_PRICE * _QTY).percentile(0.5))


def test_a_quantile_is_a_double_unless_it_is_a_value_that_is_there():
    assert isinstance(_QTY.quantile(0.5), DoubleAggregateOperation)
    assert isinstance(_QTY.quantile(0.5, method="inverted_cdf"), IntegerAggregateOperation)


@pytest.mark.parametrize("method", ["lower", "higher", "nearest", "midpoint", "hazen", "median_unbiased"])
def test_a_method_that_sql_cannot_write_is_refused(method):
    with pytest.raises(ValueError, match="cannot be written in SQL"):
        _QTY.quantile(0.5, method=method)


def test_the_fraction_of_a_quantile_is_between_0_and_1():
    with pytest.raises(ValueError, match="a fraction between 0 and 1"):
        _QTY.quantile(50)
