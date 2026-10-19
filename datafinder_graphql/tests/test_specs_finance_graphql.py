"""
Runs the finance specs against the GraphQL backend using a mock server seeded from the example CSV files.

Each entry in _SPEC_SETS names the CSV files the spec was written against; expectations that do not
apply to GraphQL carry a `skip_backends["graphql"]` reason in the spec itself.
"""
from __future__ import annotations

import shutil
import sys
import tempfile

import pytest

from datafinder import QueryRunnerBase
from datafinder_examples import example_path
from datafinder_graphql.generator import generate as graphql_generate
from datafinder_graphql.graphql_engine import GraphQLConnect
from mapping_markdown.graphql_mapping_markdown import load as load_graphql_mapping
from model.graphql_mapping import GraphQLEndpoint, GraphQLFilterConvention

from csv_graphql_server import endpoint_url, finance_tables, start_graphql_server
from datafinder_examples_tests.finance_group_by_specs import TRADE_GROUP_BY_FINDER_SPECS
from datafinder_examples_tests.finance_numeric_specs import TRADE_NUMERIC_FINDER_SPECS
from datafinder_examples_tests.finance_specs import (
    ACCOUNT_FINDER_SPECS,
    CONTRACTUAL_POSITION_FINDER_SPECS,
    TRADE_EXISTS_FINDER_SPECS,
    TRADE_FINDER_SPECS,
    TRADE_IS_NONE_FINDER_SPECS,
    TRADE_MULTI_HOP_FINDER_SPECS,
    TRADE_VIEW_FINDER_SPECS,
)

BACKEND = "graphql"
_FILTER_CONVENTION = GraphQLFilterConvention("where", "order_by", "limit")
FIXTURE = str(example_path("finance_graphql_mapping.md"))

_GRAPHQL_FINDER_MODULES = {
    "AccountFinder": "account_finder",
    "BranchFinder": "branch_finder",
    "TradeFinder": "trade_finder",
    "ContractualPositionFinder": "contractualposition_finder",
}

_DEFAULT_DATA = {}
_NUMERIC_DATA = {"accounts_csv": "finance_accounts_numeric.csv", "trades_csv": "finance_trades_numeric.csv"}
_GROUP_BY_DATA = {"accounts_csv": "finance_accounts_group_by.csv", "trades_csv": "finance_trades_group_by.csv"}

# (id, data the spec was written against, spec)
_SPEC_SETS = [
    ("account", _DEFAULT_DATA, ACCOUNT_FINDER_SPECS),
    ("trade", _DEFAULT_DATA, TRADE_FINDER_SPECS),
    ("position", _DEFAULT_DATA, CONTRACTUAL_POSITION_FINDER_SPECS),
    ("multi_hop", _DEFAULT_DATA, TRADE_MULTI_HOP_FINDER_SPECS),
    ("exists", _DEFAULT_DATA, TRADE_EXISTS_FINDER_SPECS),
    ("is_none", _DEFAULT_DATA, TRADE_IS_NONE_FINDER_SPECS),
    ("view", _DEFAULT_DATA, TRADE_VIEW_FINDER_SPECS),
    ("group_by", _GROUP_BY_DATA, TRADE_GROUP_BY_FINDER_SPECS),
    ("numeric", _NUMERIC_DATA, TRADE_NUMERIC_FINDER_SPECS),
]

_CASES = [
    pytest.param(data, spec.finder_name, expectation, id=f"{spec_id}-{expectation.name}")
    for spec_id, data, spec in _SPEC_SETS
    for expectation in spec.expectations
]


@pytest.fixture(scope="module")
def finder_classes():
    tmp = tempfile.mkdtemp()
    for mod in _GRAPHQL_FINDER_MODULES.values():
        sys.modules.pop(mod, None)
    sys.path.insert(0, tmp)

    graphql_generate(load_graphql_mapping(FIXTURE), tmp)

    from account_finder import AccountFinder  # type: ignore[import]
    from trade_finder import TradeFinder  # type: ignore[import]
    from contractualposition_finder import ContractualPositionFinder  # type: ignore[import]

    yield {
        "AccountFinder": AccountFinder,
        "TradeFinder": TradeFinder,
        "ContractualPositionFinder": ContractualPositionFinder,
    }

    if tmp in sys.path:
        sys.path.remove(tmp)
    for mod in _GRAPHQL_FINDER_MODULES.values():
        sys.modules.pop(mod, None)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(scope="module")
def make_finder(finder_classes):
    """Returns a finder wired to a mock server seeded with the given CSV files (one server per dataset)."""
    servers: dict[tuple, object] = {}

    def make(data: dict, finder_name: str):
        key = tuple(sorted(data.items()))
        if key not in servers:
            servers[key] = start_graphql_server(finance_tables(**data))
        return finder_classes[finder_name](GraphQLEndpoint(endpoint_url(servers[key]), _FILTER_CONVENTION))  # type: ignore[call-arg]

    yield make

    for server in servers.values():
        server.shutdown()  # type: ignore[attr-defined]


@pytest.fixture(autouse=True)
def register_runner():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(GraphQLConnect)
    yield
    QueryRunnerBase.clear()


@pytest.mark.parametrize("data, finder_name, expectation", _CASES)
def test_spec(data, finder_name, expectation, make_finder):
    expectation.run(make_finder(data, finder_name), backend=BACKEND)
