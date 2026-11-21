"""The tables of the example data that each set of specs reads, and the CSV that holds the rows of each: what an engine loads before it runs the specs.

The column types are the ones every database spells the same (INT, VARCHAR, DOUBLE, DATE, TIMESTAMP, BOOLEAN); the rows are in the CSV in the order of the columns.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class TableData:
    name: str  # as the mapping reads it, with its schema: ``trading.trades``, or ``main.companies`` where the mapping gives none (``main`` is the default schema)
    columns: str  # the definition of the columns: ``id INT, name VARCHAR``
    csv: str | None = None  # the file in datafinder_examples with its rows (with a header); none for a table with no rows


COMPANIES = [TableData("main.companies", "id INT, name VARCHAR, category VARCHAR", "companies.csv")]

POSITIONS = [TableData("main.positions", "id INT, trade_date DATE, npv DOUBLE", "positions_date.csv")]

FINANCE_GROUP_BY = [
    TableData("ref_data.account_master", "ID INT, ACCT_NAME VARCHAR, BRANCH_ID INT, in_z TIMESTAMP, out_z TIMESTAMP", "finance_accounts_group_by.csv"),
    TableData("ref_data.branch", "ID INT, CITY VARCHAR", "finance_branches.csv"),
    TableData("ref_data.price", "SYM VARCHAR, PRICE DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP"),
    TableData("trading.trades", "sym VARCHAR, price DOUBLE, is_settled BOOLEAN, account_id INT, in_z TIMESTAMP, out_z TIMESTAMP",
              "finance_trades_group_by.csv"),
]
