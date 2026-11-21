"""What a percentile or a quantile refuses and the type it gives. What it gives is in the finder specs (``finance_group_by_specs``), which run
against the engines; these are the behaviours that no finder query shows: an error, and the type of a column that has not been read."""
import pytest

from datafinder import DecimalAttribute, DoubleAttribute, IntegerAttribute, constant
from model.relational import (
    AggregateOperator, DecimalAggregateOperation, DoubleAggregateOperation, IntegerAggregateOperation, Table)

_T = Table("t", [("PRICE", "DOUBLE"), ("QTY", "INT"), ("G", "INT")])
_PRICE = DoubleAttribute("Price", "PRICE", "DOUBLE", _T)
_QTY = IntegerAttribute("Qty", "QTY", "INT", _T)
_G = IntegerAttribute("G", "G", "INT", _T)


def test_a_continuous_percentile_is_a_double_of_the_fraction_of_the_group():
    percentile = _QTY.percentile(0.95)
    assert isinstance(percentile, DoubleAggregateOperation)
    assert percentile.operator == AggregateOperator.PERCENTILE_CONT and percentile.percentile == 0.95 and not percentile.from_largest


def test_a_discrete_percentile_is_of_the_type_of_the_attribute():
    percentile = _QTY.percentile(0.5, continuous=False)
    assert isinstance(percentile, IntegerAggregateOperation) and percentile.operator == AggregateOperator.PERCENTILE_DISC
    assert isinstance(_PRICE.percentile(0.5, continuous=False), DoubleAggregateOperation)
    amount = DecimalAttribute("Amount", "AMOUNT", "DECIMAL(10, 2)", Table("a", [("AMOUNT", "DECIMAL(10, 2)")]))
    assert isinstance(amount.percentile(0.5, continuous=False), DecimalAggregateOperation)
    assert isinstance(constant(5).percentile(0.5, continuous=False), IntegerAggregateOperation)


def test_a_percentile_read_from_the_largest_value_says_so():
    assert _QTY.percentile(0.25, ascending=False).from_largest


def test_the_fraction_is_between_0_and_1():
    for bad in (-0.1, 1.5, 95, True, "0.5"):
        with pytest.raises(ValueError, match="a fraction between 0 and 1"):
            _QTY.percentile(bad)  # pyright: ignore[reportArgumentType]
    assert _QTY.percentile(0).percentile == 0.0 and _QTY.percentile(1).percentile == 1.0
    with pytest.raises(ValueError, match="a fraction between 0 and 1"):
        _QTY.quantile(50)


def test_a_percentile_is_not_given_a_window():
    with pytest.raises(ValueError, match="not given a window"):
        _QTY.percentile(0.5).over(partition_by=[_G])


def test_a_quantile_is_a_double_unless_it_is_a_value_that_is_there():
    assert isinstance(_QTY.quantile(0.5), DoubleAggregateOperation)
    assert isinstance(_QTY.quantile(0.5, method="inverted_cdf"), IntegerAggregateOperation)
    assert _QTY.quantile(0.9, method="inverted_cdf").operator == AggregateOperator.PERCENTILE_DISC


@pytest.mark.parametrize("method", ["lower", "higher", "nearest", "midpoint", "hazen", "median_unbiased"])
def test_a_method_that_sql_cannot_write_is_refused(method):
    with pytest.raises(ValueError, match="cannot be written in SQL"):
        _QTY.quantile(0.5, method=method)


def test_the_engine_of_a_dialect_is_imported_when_sql_is_written_for_it(monkeypatch):
    from datafinder import register_dialect, sql_for_dialect, sql_generator
    imported = []

    def import_module(name):  # the engine registers its dialect when it is imported
        imported.append(name)
        register_dialect("fakedb", "duckdb")

    monkeypatch.setitem(sql_generator._DIALECT_MODULES, "fakedb", "fake_engine_module")
    monkeypatch.setattr(sql_generator.importlib, "import_module", import_module)
    try:
        assert sql_for_dialect("SELECT a ~ 'b' FROM t", "fakedb") == "SELECT REGEXP_MATCHES(a, 'b') FROM t"
        sql_for_dialect("SELECT 1", "fakedb")
        assert imported == ["fake_engine_module"]  # once: after that the dialect is registered
    finally:
        sql_generator._DIALECTS.pop("fakedb", None)


def test_an_engine_that_is_not_installed_is_not_needed(monkeypatch):
    from datafinder import sql_for_dialect, sql_generator
    monkeypatch.setitem(sql_generator._DIALECT_MODULES, "snowflake", "no_such_engine_module_anywhere")
    assert sql_for_dialect("SELECT a ~ 'b' FROM t", "snowflake") == "SELECT REGEXP_LIKE(a, 'b') FROM t"  # sqlglot's own dialect
