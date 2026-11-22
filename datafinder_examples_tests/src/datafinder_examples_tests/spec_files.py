"""
The specs as files that any language can read, and the Python that holds only the query of each.

A spec is a ``TestExpectation(name, query)`` in a FinderSpec of ``datafinder_examples_tests``, and the rest of it is files, which are the master
of what the spec expects:

    <suite>/schema.sql                       the schemas and tables of the suite (see suite_data)
    <suite>/suite.json                       the CSV of the rows each table holds
    <suite>/<group>/<name>.json              the query (see query_json), the finder and mapping, the file of the result, the SQL files, the skips
    <suite>/<group>/<name>.csv               the expected result, with the type of each column (see typed_csv)
    <suite>/<group>/<name>.<backend>.sql     the full SQL a backend must write for the query

A suite is the specs that run against one set of tables, and a group is the specs of one finder that a Python module writes together.
The query, the finder and the mapping of the JSON come from the Python (``sync`` writes them again, and a test fails when they differ),
so a query is written once. The rest is written by whoever writes the spec: the result is worked out independently of the engine (see AGENTS.md),
and the SQL is a record of what the engine writes, written by running the tests with ``DATAFINDER_UPDATE_SQL=1``.

Run ``uv run datafinder_examples/tools/write_spec_files.py`` to write the JSON of a new or changed query into datafinder_specs/specs.
"""
from __future__ import annotations

import importlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from datafinder_examples_tests import query_json, typed_csv
from datafinder_examples_tests.spec import Checks, FinderSpec, TestExpectation


SCHEMA = "schema.sql"


@dataclass
class Suite:
    name: str
    specs: list[str]  # the FinderSpecs, as module.NAME in datafinder_examples_tests
    data: dict[str, str]  # the table as the mapping reads it, and the CSV of its rows (in the specs folder's data)


_FINANCE = {"ref_data.account_master": "finance_accounts.csv", "ref_data.branch": "finance_branches.csv", "ref_data.desk": "finance_desks.csv",
            "ref_data.price": "finance_prices.csv", "trading.trades": "finance_trades.csv", "trading.contractualposition": "finance_positions.csv"}

SUITES = [
    Suite("bitemporal_dates", ["bitemporal_dates_specs.BITEMPORAL_DATES_QUOTE_FINDER_SPECS"], {"dated.quotes": "bitemporal_dates_quotes.csv"}),
    Suite("bitemporal_infinity", ["bitemporal_infinity_specs.BITEMPORAL_INFINITY_QUOTE_FINDER_SPECS"],
          {"mkt.quotes": "bitemporal_infinity_quotes.csv"}),
    Suite("business_range", ["business_range_specs.BUSINESS_RANGE_RATE_FINDER_SPECS"], {"mkt.rates": "business_range_rates.csv"}),
    Suite("business_range_infinity", ["business_range_infinity_specs.BUSINESS_RANGE_INFINITY_RATE_FINDER_SPECS"],
          {"ranged.rates": "business_range_infinity_rates.csv"}),
    Suite("companies", ["companies_specs.COMPANY_FINDER_SPECS", "companies_constant_specs.COMPANIES_CONSTANT_SPECS"],
          {"companies": "companies.csv"}),
    Suite("companies_missing_category", ["companies_missing_category_specs.COMPANIES_MISSING_CATEGORY_SPECS"],
          {"companies": "companies_missing_category.csv"}),
    Suite("companies_scalar", ["companies_string_scalar_specs.COMPANIES_STRING_SCALAR_SPECS"], {"companies": "companies_scalar.csv"}),
    Suite("diamond", ["diamond_specs.DIAMOND_FINDER_SPECS"], {"records.items": "diamond_items.csv"}),
    Suite("embedded", ["embedded_specs.EMBEDDED_TRADE_FINDER_SPECS"],
          {"ref_data.branch_master": "finance_branches_embedded.csv", "ref_data.account_master": "finance_accounts_embedded.csv",
           "trading.trades": "finance_trades_embedded.csv"}),
    Suite("finance", [f"finance_specs.{name}" for name in (
        "ACCOUNT_FINDER_SPECS", "TRADE_FINDER_SPECS", "TRADE_MULTI_HOP_FINDER_SPECS", "TRADE_EXISTS_FINDER_SPECS", "TRADE_OR_FINDER_SPECS",
        "TRADE_IS_NONE_FINDER_SPECS", "TRADE_VIEW_FINDER_SPECS", "TRADE_NAVIGATION_DATE_FINDER_SPECS", "TRADE_ONE_WAY_ASSOCIATION_FINDER_SPECS",
        "BRANCH_FINDER_SPECS", "BRANCH_EXISTS_FINDER_SPECS", "DESK_EXISTS_FINDER_SPECS", "ACCOUNT_POSITION_EXISTS_FINDER_SPECS",
        "POSITION_DESK_EXISTS_FINDER_SPECS", "TRADE_TO_MANY_EXISTS_FINDER_SPECS", "CONTRACTUAL_POSITION_FINDER_SPECS")], _FINANCE),
    Suite("finance_group_by", ["finance_group_by_specs.TRADE_GROUP_BY_FINDER_SPECS"],
          {"ref_data.account_master": "finance_accounts_group_by.csv", "ref_data.branch": "finance_branches.csv",
           "trading.trades": "finance_trades_group_by.csv"}),
    Suite("finance_numeric", ["finance_numeric_specs.TRADE_NUMERIC_FINDER_SPECS", "finance_numeric_specs.TRADE_ARITHMETIC_FINDER_SPECS"],
          {"ref_data.account_master": "finance_accounts_numeric.csv", "ref_data.branch": "finance_branches.csv",
           "trading.trades": "finance_trades_numeric.csv"}),
    Suite("join_conditions", ["join_conditions_specs.JOIN_CONDITIONS_FIRM_FINDER_SPECS", "join_conditions_specs.JOIN_CONDITIONS_PERSON_FINDER_SPECS",
                              "join_conditions_specs.JOIN_CONDITIONS_EMPLOYEE_FINDER_SPECS"],
          {f"org.{t}": f"join_conditions_{t}.csv" for t in ("firms", "persons", "memberships", "staff")}),
    Suite("joins_through_tables", ["joins_through_tables_specs.JOINS_THROUGH_TABLES_EMPLOYEE_FINDER_SPECS"],
          {f"hr.{t}": f"joins_through_tables_{t}.csv" for t in ("employees", "country_refs", "countries")}),
    Suite("milestoned_union", ["milestoned_union_specs.MILESTONED_UNION_ORDER_FINDER_SPECS"],
          {"OrderTable": "milestoned_union_orders.csv", "ProductTableWithBusinessSnapshotMilestoning": "milestoned_union_products.csv",
           "ProductClassificationTableWithBusinessSnapshotMilestoning": "milestoned_union_classifications.csv"}),
    Suite("null_end_milestoning", ["null_end_milestoning_specs.NULL_END_PRICE_FINDER_SPECS"], {"mkt.prices": "null_end_prices.csv"}),
    Suite("orgchart", ["orgchart_specs.ORGCHART_FINDER_SPECS", "orgchart_specs.ORGCHART_AGGREGATE_FINDER_SPECS",
                       "orgchart_specs.ORGCHART_EXISTS_FINDER_SPECS"], {"hr.employees": "employees.csv"}),
    Suite("orgchart_inheritance", ["orgchart_inheritance_specs.EMPLOYEE_INHERITANCE_FINDER_SPECS", "orgchart_inheritance_specs.PROJECT_FINDER_SPECS"],
          {"hr.employees": "employees_inheritance.csv", "hr.projects": "projects.csv"}),
    Suite("people_denormalised", ["people_denormalised_specs.PERSON_DENORMALISED_FINDER_SPECS"], {"people": "people_denormalised.csv"}),
    Suite("places", ["places_specs.PLACES_PERSON_FINDER_SPECS", "places_specs.PLACES_LOCATION_FINDER_SPECS"],
          {f"geo.{t}": f"places_{t}.csv" for t in ("people", "regions", "locations")}),
    Suite("positions", ["positions_specs.POSITION_DATE_FINDER_SPECS"], {"positions": "positions_date.csv"}),
    Suite("processing_snapshot", ["processing_snapshot_specs.PROCESSING_SNAPSHOT_QUOTE_FINDER_SPECS",
                                  "processing_snapshot_specs.PROCESSING_SNAPSHOT_POSITION_FINDER_SPECS"],
          {"mkt.quotes": "processing_snapshot_quotes.csv", "mkt.positions": "processing_snapshot_positions.csv"}),
    Suite("sql_queries", [f"sql_queries_specs.SQL_QUERIES_{name}_FINDER_SPECS" for name in (
        "FIRM", "POSITION", "FIRM_X_PERSON", "TAG", "SHOUT", "RATE", "QUOTE", "HOLDER", "UNIT")],
          {f"org.{t}": f"sql_queries_{t}.csv" for t in ("people", "firms", "trades", "rates", "quotes", "holders", "units")}),
    Suite("sql_query_sets", ["sql_query_sets_specs.SQL_QUERY_SETS_CUSTOMER_FINDER_SPECS", "sql_query_sets_specs.SQL_QUERY_SETS_APPLICATION_FINDER_SPECS"],
          {**{f"org.{t}": f"sql_query_sets_{t}.csv" for t in ("retail_customers", "corporate_customers", "orders")},
           **{f"org.{t}": f"sql_query_sets_embedded_{t}.csv" for t in ("applications_a", "applications_b")}}),
]


def _group(variable: str) -> str:
    """The name of a group: the name of its FinderSpecs variable in lower case, without the ending that every one has."""
    return re.sub(r"_(FINDER_)?SPECS$", "", variable).lower()


def groups(suite: Suite) -> dict[str, FinderSpec]:
    """The FinderSpecs of a suite by group."""
    found: dict[str, FinderSpec] = {}
    for reference in suite.specs:
        module, variable = reference.split(".")
        found[_group(variable)] = getattr(importlib.import_module(f"datafinder_examples_tests.{module}"), variable)
    return found


def _json(document: dict) -> str:
    """The document with a key on each line, and the query tree, which is deep, on one line."""
    lines = []
    for key, value in document.items():
        compact = key == "query"
        text = json.dumps(value, ensure_ascii=False, separators=(",", ":")) if compact else json.dumps(value, ensure_ascii=False, indent=2)
        lines.append(f"  {json.dumps(key)}: {text.replace(chr(10), chr(10) + '  ')}")
    return "{\n" + ",\n".join(lines) + "\n}\n"


GENERATED = ("name", "finder", "mapping", "query_text", "query")


def sync(suite: Suite) -> dict[str, str]:
    """The JSON files of a suite, by their path under the specs folder: the query, finder and mapping from the Python, and the rest as the file has it."""
    files: dict[str, str] = {f"{suite.name}/suite.json": json.dumps({"data": suite.data}, indent=2) + "\n"}
    root = specs_folder()
    for group, spec in groups(suite).items():
        for expectation in spec.expectations:
            path = f"{suite.name}/{group}/{expectation.name}.json"
            if path in files:
                raise ValueError(f"The name {expectation.name!r} is used twice in the group {group} of the suite {suite.name}")
            existing = json.loads((root / path).read_text(encoding="utf-8")) if (root / path).is_file() else {"result": f"{expectation.name}.csv"}
            query = query_json.record(expectation.query)
            document: dict = {"name": expectation.name, "finder": spec.finder_name, "mapping": spec.mapping_file,
                              "query_text": query_json.render(query), "query": query}
            document.update({key: value for key, value in existing.items() if key not in GENERATED})
            files[path] = _json(document)
    return files


def sync_all() -> dict[str, str]:
    return {path: text for suite in SUITES for path, text in sync(suite).items()}


def specs_folder() -> Path:
    from datafinder_examples import specs_root
    return specs_root() / "specs"


_LOCATIONS: dict[int, tuple[str, str]] = {}
_CHECKS: dict[tuple[str, str, str], Checks] = {}


def _location(expectation: TestExpectation) -> tuple[str, str]:
    """The suite and group of an expectation: the one whose FinderSpec holds it."""
    if not _LOCATIONS:
        for suite in SUITES:
            for group, spec in groups(suite).items():
                for each in spec.expectations:
                    _LOCATIONS[id(each)] = (suite.name, group)
    if id(expectation) not in _LOCATIONS:
        raise KeyError(f"The spec {expectation.name!r} is in no suite of SUITES (spec_files.py), so it has no files: give it expected_columns and expected_result")
    return _LOCATIONS[id(expectation)]


def checks(expectation: TestExpectation) -> Checks:
    """What the files of the specs folder say the expectation must give."""
    suite, group = _location(expectation)
    key = (suite, group, expectation.name)
    if key not in _CHECKS:
        folder = specs_folder() / suite / group
        document = json.loads((folder / f"{expectation.name}.json").read_text(encoding="utf-8"))
        columns, _, rows = typed_csv.read((folder / document["result"]).read_text(encoding="utf-8"))
        _CHECKS[key] = Checks(columns, np.asarray(rows, dtype=object), sql_files={b: folder / f for b, f in document.get("sql", {}).items()},
                              sql_contains=document.get("sql_contains", {}), sql_not_contains=document.get("sql_not_contains", {}),
                              skip=document.get("skip", {}), approximate=document.get("approximate", False))
    return _CHECKS[key]


def files_of(suite: Suite) -> set[str]:
    """The files of a suite that its specs own: the paths under the specs folder of what is in each group's folder."""
    root = specs_folder()
    return {str(p.relative_to(root)) for group in groups(suite) for p in (root / suite.name / group).glob("*") if p.is_file()}


def write_all() -> None:
    root = specs_folder()
    for path, text in sync_all().items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    write_all()
