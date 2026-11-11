"""exists: a condition of a result that holds when another result has a row meeting a condition, as Legend's ``exists`` in a
``filter`` and SQL's ``EXISTS``. ``~exists`` is ``NOT EXISTS``."""
import pytest

from datafinder import DoubleAttribute, StringAttribute, exists
from datafinder.runner import FinderResult
from model.relational import LogicalOperation, NoOperation, NotOperation, RelationExistsOperation, Table


def _results() -> tuple[FinderResult, FinderResult]:
    trades = Table("trades", [("ACCOUNT", "VARCHAR"), ("SYM", "VARCHAR"), ("PRICE", "DOUBLE")])
    accounts = Table("accounts", [("ID", "VARCHAR"), ("NAME", "VARCHAR")])
    left = FinderResult(None, None, [StringAttribute("Account", "ACCOUNT", "VARCHAR", trades).alias("account"),
                                     StringAttribute("Sym", "SYM", "VARCHAR", trades).alias("sym"),
                                     DoubleAttribute("Price", "PRICE", "DOUBLE", trades).alias("price")], trades, NoOperation())
    right = FinderResult(None, None, [StringAttribute("Id", "ID", "VARCHAR", accounts).alias("id"),
                                      StringAttribute("Name", "NAME", "VARCHAR", accounts).alias("name")], accounts, NoOperation())
    return left, right


def test_exists_is_a_correlated_subquery_over_the_other_result():
    trades, accounts = _results()
    sql = trades.filter(lambda t: exists(accounts, lambda a: a.id == t.account)).to_sql()
    # the filtered row is t0, and the subquery reads the other result, named t1, with the condition on the two
    assert sql.endswith(' WHERE EXISTS (SELECT 1 FROM (SELECT t2.ID AS "id",t2.NAME AS "name" FROM accounts AS t2) AS t1 '
                        'WHERE t1.id = t0.account)')


def test_not_exists_is_the_negation():
    trades, accounts = _results()
    sql = trades.filter(lambda t: ~exists(accounts, lambda a: a.id == t.account)).to_sql()
    assert " WHERE NOT (EXISTS (SELECT 1 FROM (SELECT " in sql


def test_the_condition_can_relate_the_two_rows_by_more_than_equality():
    trades, accounts = _results()
    sql = trades.filter(lambda t: exists(accounts, lambda a: (a.id == t.account) & (a.name != t.sym))).to_sql()
    assert "WHERE EXISTS (SELECT 1 FROM" in sql and " AND " in sql.split("WHERE EXISTS", 1)[1]


def test_exists_and_not_exists_are_combined_like_any_conditions():
    trades, accounts = _results()
    sql = trades.filter(lambda t: exists(accounts, lambda a: a.id == t.account)
                        & ~exists(accounts, lambda a: a.name == t.sym)).to_sql()
    assert sql.count("EXISTS (SELECT 1 FROM") == 2 and " AND NOT (EXISTS" in sql


def test_a_result_can_be_tested_against_itself():
    trades, _ = _results()
    sql = trades.filter(lambda t: exists(trades, lambda o: (o.account == t.account) & (o.sym != t.sym))).to_sql()
    assert sql.count("EXISTS (SELECT 1 FROM") == 1
    # the two are different relations, so the subquery's row is not the filtered row
    assert sql.endswith("AS t1 WHERE t1.account = t0.account AND t1.sym <> t0.sym)")


def test_the_model_of_exists_is_a_condition_over_a_relation_that_negates_with_not():
    trades, accounts = _results()
    seen = []
    trades.filter(lambda t: seen.append(exists(accounts, lambda a: a.id == t.account)) or seen[0])
    (found,) = seen
    assert isinstance(found, RelationExistsOperation) and isinstance(~found, NotOperation)
    assert isinstance(found & found, LogicalOperation)


def test_the_condition_is_a_function_that_gives_a_comparison():
    trades, accounts = _results()
    with pytest.raises(TypeError, match="function of a row"):
        exists(accounts, accounts["id"] == trades["account"])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="comparison"):
        exists(accounts, lambda a: a.id)


def test_a_column_the_other_result_does_not_have_says_which_are():
    trades, accounts = _results()
    with pytest.raises(AttributeError, match="id"):
        exists(accounts, lambda a: a.nope == 1)
