"""
The specs as files that any language can read: a JSON file for each expectation, and a CSV of the result it expects.

``export`` writes them from the Python specs, which stay the place where a spec is written: the expected result is worked out there,
independently of the engine. ``load`` reads them back as expectations that run through the same checks as the Python ones.

    <suite>/schema.sql          the schemas and tables of the suite, written by hand (see suite_data)
    <suite>/suite.json          the CSV of the rows each table holds
    <suite>/<group>/<name>.json the query (see query_json), the finder and mapping, the file of the result, and the checks
    <suite>/<group>/<name>.csv  the expected result, with the type of each column (see typed_csv)

A suite is the specs that run against one set of tables, and a group is the specs of one finder that a Python module writes together.
Set ``DATAFINDER_SPECS_FROM_FILES=1`` and the specs of every group are read from these files, whichever test runs them (see conftest.py).

Run ``uv run datafinder_examples/tools/write_spec_files.py`` to write them into datafinder_specs/specs.
"""
from __future__ import annotations

import importlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from datafinder_examples_tests import query_json, typed_csv
from datafinder_examples_tests.spec import FinderSpec, TestExpectation


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


def _types(expectation: TestExpectation) -> list[str]:
    if expectation.expected_types is not None:
        return expectation.expected_types
    result = expectation.expected_result
    if result.shape[0] == 0:
        raise ValueError(f"[{expectation.name}] An empty result has no type to work out: declare it in expected_types")
    return [typed_csv.column_type(list(result[:, c])) for c in range(result.shape[1])]


def _json(document: dict) -> str:
    """The document with a key on each line, and the query tree, which is deep, on one line."""
    lines = []
    for key, value in document.items():
        compact = key == "query"
        text = json.dumps(value, ensure_ascii=False, separators=(",", ":")) if compact else json.dumps(value, ensure_ascii=False, indent=2)
        lines.append(f"  {json.dumps(key)}: {text.replace(chr(10), chr(10) + '  ')}")
    return "{\n" + ",\n".join(lines) + "\n}\n"


def export(suite: Suite) -> dict[str, str]:
    """The files of a suite, by their path under the specs folder."""
    files: dict[str, str] = {f"{suite.name}/suite.json": json.dumps({"data": suite.data}, indent=2) + "\n"}
    for group, spec in groups(suite).items():
        for expectation in _python_expectations(spec):
            path = f"{suite.name}/{group}/{expectation.name}"
            if f"{path}.json" in files:
                raise ValueError(f"The name {expectation.name!r} is used twice in the group {group} of the suite {suite.name}")
            query = query_json.record(expectation.query)
            document: dict = {"name": expectation.name, "finder": spec.finder_name, "mapping": spec.mapping_file,
                              "query_text": query_json.render(query), "query": query, "result": f"{expectation.name}.csv"}
            if expectation.approximate:
                document["approximate"] = True
            if expectation.expected_sql:
                document["sql_contains"] = expectation.expected_sql
            if expectation.unexpected_sql:
                document["sql_not_contains"] = expectation.unexpected_sql
            if expectation.skip_backends:
                document["skip"] = expectation.skip_backends
            files[f"{path}.json"] = _json(document)
            files[f"{path}.csv"] = typed_csv.write(expectation.expected_columns, _types(expectation), expectation.expected_result)
    return files


def export_all() -> dict[str, str]:
    return {path: text for suite in SUITES for path, text in export(suite).items()}


def specs_folder() -> Path:
    from datafinder_examples import specs_root
    return specs_root() / "specs"


def load(suite: str, group: str) -> list[TestExpectation]:
    """The expectations of a group, from the files in the specs folder."""
    folder = specs_folder() / suite / group
    expectations = []
    for path in sorted(folder.glob("*.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        columns, _, rows = typed_csv.read((folder / document["result"]).read_text(encoding="utf-8"))
        node = document["query"]
        expectations.append(TestExpectation(
            name=document["name"], query=lambda f, node=node: query_json.replay(node, f), expected_columns=columns,
            expected_result=np.asarray(rows, dtype=object), expected_sql=document.get("sql_contains", {}),
            unexpected_sql=document.get("sql_not_contains", {}), skip_backends=document.get("skip", {}),
            approximate=document.get("approximate", False)))
    if not expectations:
        raise ValueError(f"No specs in {folder}")
    return expectations


_PYTHON: dict[int, list[TestExpectation]] = {}


def _python_expectations(spec: FinderSpec) -> list[TestExpectation]:
    """The expectations a spec was written with, also after ``use_files`` has replaced them with the ones read from the files."""
    return _PYTHON.get(id(spec), spec.expectations)


def use_files() -> None:
    """Make every spec that is written as files run the expectations read from the files, in place of the ones the Python wrote."""
    for suite in SUITES:
        for group, spec in groups(suite).items():
            _PYTHON.setdefault(id(spec), spec.expectations)
            spec.expectations = load(suite.name, group)


def write_all() -> None:
    root = specs_folder()
    for suite in SUITES:
        folder = root / suite.name
        for old in folder.rglob("*") if folder.exists() else []:
            if old.is_file() and old.name != SCHEMA:  # the schema is written by hand
                old.unlink()
    for path, text in export_all().items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    write_all()
