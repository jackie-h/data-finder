"""The TPC-BiH report's SQL (SQL:2011 temporal clauses) as SQL that DuckDB runs.

Appendix B of the report writes a temporal table as ``partsupp FOR SYSTEM_TIME AS OF TIMESTAMP '...' FOR BUSINESS_TIME AS
OF '...' AS ps1``. DuckDB has no such clauses, so each table reference is rewritten as a sub-select on the table's period
columns, and sqlglot reads the rest of the statement, renames the report's ``*_start`` columns to this repository's
``*_begin`` and writes it for DuckDB.

The meaning of a clause, as SQL:2011 gives it (the period is begin inclusive, end exclusive):

    AS OF t            begin <= t AND end > t
    FROM a TO b        begin < b  AND end > a        (the period [a, b))
    BETWEEN a AND b    begin <= b AND end > a        (the period [a, b])

A table with no ``FOR SYSTEM_TIME`` is read as of the current system time, and one with no ``FOR BUSINESS_TIME`` is read
over all of application time, which is what the report's queries without a clause mean (T.7, K.5app, B.6).
"""
from __future__ import annotations

import re

import sqlglot
from sqlglot import exp

from datafinder_examples_tests.tpch_bih_data import CURRENT_SYS

#: the application time of each temporal table: the prefix of its _begin and _end columns
APPLICATION_TIME = {"part": "availability_time", "partsupp": "validity_time", "customer": "visible_time",
                    "orders": "active_time", "lineitem": "active_time"}
#: the tables that have a system time
SYSTEM_TIME = {"supplier", "part", "partsupp", "customer", "orders", "lineitem"}

_TABLES = "|".join(sorted(APPLICATION_TIME.keys() | SYSTEM_TIME, key=len, reverse=True))
_TIME = r"(?:TIMESTAMP\s+)?'[^']*'"
_CLAUSE = (rf"FOR\s+(?:SYSTEM_TIME|BUSINESS_TIME)\s+"
           rf"(?:AS\s+OF\s+{_TIME}|FROM\s+{_TIME}\s+TO\s+{_TIME}|BETWEEN\s+{_TIME}\s+AND\s+{_TIME})")
_ONE_CLAUSE = re.compile(
    rf"FOR\s+(?P<dimension>SYSTEM_TIME|BUSINESS_TIME)\s+"
    rf"(?:AS\s+OF\s+(?:TIMESTAMP\s+)?(?P<at>'[^']*')"
    rf"|FROM\s+(?:TIMESTAMP\s+)?(?P<from>'[^']*')\s+TO\s+(?:TIMESTAMP\s+)?(?P<to>'[^']*')"
    rf"|BETWEEN\s+(?:TIMESTAMP\s+)?(?P<low>'[^']*')\s+AND\s+(?:TIMESTAMP\s+)?(?P<high>'[^']*'))", re.I)
_KEYWORDS = r"WHERE|ORDER|GROUP|FOR|LIMIT|HAVING|AND|ON|FETCH|UNION"
_TABLE_REFERENCE = re.compile(
    rf"\b(?P<lead>FROM|,)\s+(?P<table>{_TABLES})\b(?P<clauses>(?:\s+{_CLAUSE})*)"
    rf"(?:\s+(?:AS\s+)?(?!(?:{_KEYWORDS})\b)(?P<alias>\w+))?", re.I)


def _period(column: str, clause: re.Match) -> str:
    begin, end = f"{column}_begin", f"{column}_end"
    if clause["at"]:
        return f"{begin} <= {clause['at']} AND {end} > {clause['at']}"
    if clause["from"]:
        return f"{begin} < {clause['to']} AND {end} > {clause['from']}"
    return f"{begin} <= {clause['high']} AND {end} > {clause['low']}"


def _current_system_time() -> str:
    current = f"TIMESTAMP '{CURRENT_SYS:%Y-%m-%d %H:%M:%S}'"
    return f"sys_time_begin <= {current} AND sys_time_end > {current}"


def temporal_clauses_to_subselects(sql: str, schema: str | None = None) -> str:
    """Each table reference with its FOR SYSTEM_TIME / FOR BUSINESS_TIME clauses becomes a sub-select that filters the table."""
    prefix = f"{schema}." if schema else ""

    def rewrite(reference: re.Match) -> str:
        table = reference["table"].lower()
        system = application = None
        for clause in _ONE_CLAUSE.finditer(reference["clauses"]):
            if clause["dimension"].upper() == "SYSTEM_TIME":
                system = _period("sys_time", clause)
            else:
                application = _period(APPLICATION_TIME[table], clause)
        conditions = []
        if table in SYSTEM_TIME:
            conditions.append(system or _current_system_time())
        if application:
            conditions.append(application)
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        alias = f" AS {reference['alias']}" if reference["alias"] else ""
        return f"{reference['lead']} (SELECT * FROM {prefix}{table}{where}){alias}"

    return _TABLE_REFERENCE.sub(rewrite, sql)


def _rename_start_columns(node):
    if isinstance(node, exp.Column) and node.name.lower().endswith("_start"):
        return exp.column(node.name.lower()[:-len("_start")] + "_begin", table=node.table or None)
    return node


def to_duckdb(sql: str, schema: str | None = None) -> str:
    """The report's SQL as a statement DuckDB runs, on tables in ``schema``."""
    statement = sqlglot.parse_one(temporal_clauses_to_subselects(sql, schema), read="duckdb")
    return statement.transform(_rename_start_columns).sql(dialect="duckdb")
