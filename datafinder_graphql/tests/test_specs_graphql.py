"""
Runs the shared specs against the GraphQL backend, in strict mode, using a mock server seeded from the
example CSV files.

Each scenario is a GraphQL mapping, the data the specs were written against, and the specs to run.
Expectations that do not apply to GraphQL carry a `skip_backends["graphql"]` reason in the spec itself.
"""
from __future__ import annotations

import importlib.util
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pytest

from datafinder import QueryRunnerBase
from datafinder_examples import example_path
from datafinder_graphql.generator import generate as graphql_generate
from datafinder_graphql.graphql_engine import GraphQLConnect
from mapping_markdown.graphql_mapping_markdown import load as load_graphql_mapping
from model.graphql_mapping import GraphQLClassMapping, GraphQLEndpoint, GraphQLFilterConvention

from csv_graphql_server import (
    GraphQLTable, NestedVersions, csv_table, endpoint_url, finance_tables, start_graphql_server,
)
from datafinder_examples_tests.companies_specs import COMPANY_FINDER_SPECS
from datafinder_examples_tests.companies_string_scalar_specs import COMPANIES_STRING_SCALAR_SPECS
from datafinder_examples_tests.diamond_specs import DIAMOND_FINDER_SPECS
from datafinder_examples_tests.embedded_specs import EMBEDDED_TRADE_FINDER_SPECS
from datafinder_examples_tests.finance_group_by_specs import TRADE_GROUP_BY_FINDER_SPECS
from datafinder_examples_tests.finance_numeric_specs import TRADE_NUMERIC_FINDER_SPECS
from datafinder_examples_tests.finance_specs import (
    ACCOUNT_FINDER_SPECS,
    CONTRACTUAL_POSITION_FINDER_SPECS,
    TRADE_EXISTS_FINDER_SPECS,
    TRADE_FINDER_SPECS,
    TRADE_OR_FINDER_SPECS,
    TRADE_IS_NONE_FINDER_SPECS,
    TRADE_MULTI_HOP_FINDER_SPECS,
    TRADE_VIEW_FINDER_SPECS,
)
from datafinder_examples_tests.null_end_milestoning_specs import NULL_END_PRICE_FINDER_SPECS
from datafinder_examples_tests.orgchart_inheritance_specs import (
    EMPLOYEE_INHERITANCE_FINDER_SPECS,
    PROJECT_FINDER_SPECS,
)
from datafinder_examples_tests.orgchart_specs import ORGCHART_FINDER_SPECS
from datafinder_examples_tests.positions_specs import POSITION_DATE_FINDER_SPECS
from datafinder_examples_tests.spec import FinderSpec

BACKEND = "graphql"
_FILTER_CONVENTION = GraphQLFilterConvention("where", "order_by", "limit")


# ---------------------------------------------------------------------------
# Data, shaped as GraphQL responses
# ---------------------------------------------------------------------------

def _companies(csv_name: str) -> Callable[[], dict[str, GraphQLTable]]:
    return lambda: {"companies": csv_table(csv_name, {
        "id": ("id", int), "name": "name", "category": "category"})}


def _positions() -> dict[str, GraphQLTable]:
    return {"positions": csv_table("positions_date.csv", {
        "id": ("id", int), "tradeDate": "trade_date", "npv": ("npv", float)})}


def _null_end_prices() -> dict[str, GraphQLTable]:
    table = csv_table("null_end_prices.csv", {"symbol": "sym", "price": ("price", float)},
                      internal={"_in_z": ("in_z", str), "_out_z": ("out_z", str)}, processing_milestoned=True)
    return {"prices": table}


def _diamond() -> dict[str, GraphQLTable]:
    return {"items": csv_table("diamond_items.csv", {
        "id": ("item_id", int), "createdAt": "created_at", "updatedAt": "updated_at",
        "version": ("version", int), "recordName": "record_name"})}


def _orgchart() -> dict[str, GraphQLTable]:
    employees = csv_table("employees.csv", {"id": ("id", int), "name": "name"},
                          internal={"_manager_id": ("manager_id", int)})
    employees.nested = {"manager": NestedVersions("_manager_id", employees.rows)}
    return {"employees": employees}


def _orgchart_inheritance() -> dict[str, GraphQLTable]:
    employees = csv_table("employees_inheritance.csv", {
        "id": ("emp_id", int), "firstName": "first_name", "lastName": "last_name",
        "email": "email", "department": "department"}, internal={"_manager_id": ("manager_id", int)})
    projects = csv_table("projects.csv", {"id": ("project_id", int), "name": "name", "code": "code"},
                         internal={"_assignee_id": ("assignee_id", int)})
    # a to-many relation is a list on each parent
    for employee in employees.rows:
        employee["projects"] = [p for p in projects.rows if p["_assignee_id"] == employee["id"]]
    employees.nested = {"manager": NestedVersions("_manager_id", employees.rows)}
    projects.nested = {"assignee": NestedVersions("_assignee_id", employees.rows)}
    return {"employees": employees, "projects": projects}


@dataclass
class Scenario:
    id: str
    mapping_file: str
    tables: Callable[[], dict[str, GraphQLTable]]
    specs: list[tuple[str, FinderSpec]]


_FINANCE_MAPPING = "finance_graphql_mapping.md"
_NUMERIC = dict(accounts_csv="finance_accounts_numeric.csv", trades_csv="finance_trades_numeric.csv")
_GROUP_BY = dict(accounts_csv="finance_accounts_group_by.csv", trades_csv="finance_trades_group_by.csv")
_EMBEDDED = dict(accounts_csv="finance_accounts_embedded.csv", trades_csv="finance_trades_embedded.csv",
                 branches_csv="finance_branches_embedded.csv")

_SCENARIOS = [
    Scenario("finance", _FINANCE_MAPPING, finance_tables, [
        ("account", ACCOUNT_FINDER_SPECS),
        ("trade", TRADE_FINDER_SPECS),
        ("position", CONTRACTUAL_POSITION_FINDER_SPECS),
        ("multi_hop", TRADE_MULTI_HOP_FINDER_SPECS),
        ("exists", TRADE_EXISTS_FINDER_SPECS),
        ("or", TRADE_OR_FINDER_SPECS),
        ("is_none", TRADE_IS_NONE_FINDER_SPECS),
        ("view", TRADE_VIEW_FINDER_SPECS),
    ]),
    Scenario("finance_group_by", _FINANCE_MAPPING, lambda: finance_tables(**_GROUP_BY),
             [("group_by", TRADE_GROUP_BY_FINDER_SPECS)]),
    Scenario("finance_numeric", _FINANCE_MAPPING, lambda: finance_tables(**_NUMERIC),
             [("numeric", TRADE_NUMERIC_FINDER_SPECS)]),
    Scenario("finance_embedded", _FINANCE_MAPPING, lambda: finance_tables(**_EMBEDDED),
             [("embedded", EMBEDDED_TRADE_FINDER_SPECS)]),
    Scenario("companies", "companies_graphql_mapping.md", _companies("companies.csv"),
             [("companies", COMPANY_FINDER_SPECS)]),
    Scenario("companies_scalar", "companies_graphql_mapping.md", _companies("companies_scalar.csv"),
             [("companies_scalar", COMPANIES_STRING_SCALAR_SPECS)]),
    Scenario("positions", "positions_graphql_mapping.md", _positions,
             [("positions", POSITION_DATE_FINDER_SPECS)]),
    Scenario("null_end", "null_end_milestoning_graphql_mapping.md", _null_end_prices,
             [("null_end", NULL_END_PRICE_FINDER_SPECS)]),
    Scenario("diamond", "diamond_graphql_mapping.md", _diamond,
             [("diamond", DIAMOND_FINDER_SPECS)]),
    Scenario("orgchart", "orgchart_graphql_mapping.md", _orgchart,
             [("orgchart", ORGCHART_FINDER_SPECS)]),
    Scenario("orgchart_inheritance", "orgchart_inheritance_graphql_mapping.md", _orgchart_inheritance,
             [("employee", EMPLOYEE_INHERITANCE_FINDER_SPECS), ("project", PROJECT_FINDER_SPECS)]),
]

_SCENARIOS_BY_ID = {s.id: s for s in _SCENARIOS}

_CASES = [
    pytest.param(scenario.id, spec.finder_name, expectation, id=f"{scenario.id}-{spec_id}-{expectation.name}")
    for scenario in _SCENARIOS
    for spec_id, spec in scenario.specs
    for expectation in spec.expectations
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _load_finder_classes(mapping_file: str, directory: Path, module_prefix: str) -> dict[str, type]:
    """Generate the finders for a mapping and import them under unique module names."""
    mapping = load_graphql_mapping(str(example_path(mapping_file)))
    graphql_generate(mapping, str(directory))
    classes: dict[str, type] = {}
    for cm in mapping.mappings:
        if not isinstance(cm, GraphQLClassMapping):
            continue
        class_name = f"{cm.clazz.name}Finder"
        module_name = f"{module_prefix}_{cm.clazz.name.lower()}_finder"
        module_spec = importlib.util.spec_from_file_location(module_name, directory / f"{cm.clazz.name.lower()}_finder.py")
        assert module_spec is not None and module_spec.loader is not None
        module = importlib.util.module_from_spec(module_spec)
        sys.modules[module_name] = module
        module_spec.loader.exec_module(module)
        classes[class_name] = getattr(module, class_name)
    return classes


@pytest.fixture(scope="module")
def make_finder():
    """Returns a finder wired to a mock server seeded with the scenario's data (one server per scenario)."""
    tmp = Path(tempfile.mkdtemp())
    classes: dict[str, dict[str, type]] = {}
    servers: dict[str, object] = {}

    def make(scenario_id: str, finder_name: str):
        scenario = _SCENARIOS_BY_ID[scenario_id]
        if scenario.mapping_file not in classes:
            directory = tmp / Path(scenario.mapping_file).stem
            directory.mkdir()
            classes[scenario.mapping_file] = _load_finder_classes(
                scenario.mapping_file, directory, f"graphql_{Path(scenario.mapping_file).stem}")
        if scenario_id not in servers:
            servers[scenario_id] = start_graphql_server(scenario.tables())
        endpoint = GraphQLEndpoint(endpoint_url(servers[scenario_id]), _FILTER_CONVENTION)  # type: ignore[arg-type]
        return classes[scenario.mapping_file][finder_name](endpoint)  # type: ignore[call-arg]

    yield make

    for server in servers.values():
        server.shutdown()  # type: ignore[attr-defined]
    for name in [n for n in sys.modules if n.startswith("graphql_") and n.endswith("_finder")]:
        sys.modules.pop(name, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(autouse=True)
def register_runner():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(GraphQLConnect)
    yield
    QueryRunnerBase.clear()


@pytest.mark.parametrize("scenario_id, finder_name, expectation", _CASES)
def test_spec(scenario_id, finder_name, expectation, make_finder):
    expectation.run(make_finder(scenario_id, finder_name), backend=BACKEND)
