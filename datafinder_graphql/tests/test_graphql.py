import datetime

import numpy as np
import pytest
from numpy.testing import assert_array_equal

from datafinder import Attribute, QueryRunnerBase
from datafinder.runner import FinderResult
from datafinder_graphql.graphql_engine import GraphQLConnect
from model.graphql_mapping import (
    GraphQLEndpoint,
    GraphQLQuery,
    GraphQLField,
    GraphQLPropertyMapping,
    GraphQLClassMapping,
    GraphQLBusinessDateMilestone,
    GraphQLProcessingMilestone,
    GraphQLBiTemporalMilestone,
)
from model.m3 import Class, Property, String, Float, Integer, Package

from mock_graphql_server import start_mock_server


@pytest.fixture(scope="module")
def graphql_server():
    server = start_mock_server()
    yield server
    server.shutdown()


@pytest.fixture(autouse=True)
def register_runner():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(GraphQLConnect)
    yield
    QueryRunnerBase.clear()


def _endpoint(server) -> GraphQLEndpoint:
    host, port = server.server_address
    return GraphQLEndpoint(f"http://{host}:{port}/graphql")


class TestGraphQLMapping:

    def test_graphql_mapping_model(self):
        endpoint = GraphQLEndpoint("http://example.com/graphql")
        query = GraphQLQuery("instruments", endpoint)
        field = GraphQLField("sym")

        sym_prop = Property("Symbol", "symbol", String)
        instrument_class = Class("Instrument", [sym_prop], Package("finance"))

        pm = GraphQLPropertyMapping(sym_prop, field)
        cm = GraphQLClassMapping(instrument_class, [pm], query)

        assert cm.query is query
        assert cm.query.endpoint.url == "http://example.com/graphql"
        assert cm.query.name == "instruments"
        assert pm.target.name == "sym"


class TestGraphQLEngine:

    def test_query_all_instruments(self, graphql_server):
        endpoint = _endpoint(graphql_server)
        gql_query = GraphQLQuery("instruments", endpoint)

        sym_attr = Attribute("Symbol", "sym", "STRING", "instruments")
        price_attr = Attribute("Price", "price", "FLOAT", "instruments")

        result = FinderResult(None, None, [sym_attr, price_attr], gql_query, None)  # type: ignore[arg-type]
        data = result.to_numpy()

        assert_array_equal(data, np.array(
            [["AAPL", 150.0], ["MSFT", 300.0], ["GOOG", 2800.0]],
            dtype="object",
        ))

    def test_query_with_business_date_milestone(self, graphql_server):
        endpoint = _endpoint(graphql_server)
        # Milestone wires business_date to the "businessDate" argument
        gql_query = GraphQLQuery("instruments", endpoint,
                                  milestone=GraphQLBusinessDateMilestone("businessDate"))
        sym_attr = Attribute("Symbol", "sym", "STRING", "instruments")

        bd = datetime.date(2024, 1, 10)
        result = FinderResult(bd, None, [sym_attr], gql_query, None)  # type: ignore[arg-type]
        data = result.to_numpy()

        assert data.shape == (3, 1)
        assert list(data[:, 0]) == ["AAPL", "MSFT", "GOOG"]

    def test_query_with_processing_milestone(self, graphql_server):
        endpoint = _endpoint(graphql_server)
        gql_query = GraphQLQuery("instruments", endpoint,
                                  milestone=GraphQLProcessingMilestone("asOf"))
        sym_attr = Attribute("Symbol", "sym", "STRING", "instruments")

        pdt = datetime.datetime(2020, 1, 1, 9, 0, 0)
        result = FinderResult(None, pdt, [sym_attr], gql_query, None)  # type: ignore[arg-type]
        data = result.to_numpy()

        assert data.shape == (3, 1)

    def test_no_milestone_ignores_dates(self, graphql_server):
        endpoint = _endpoint(graphql_server)
        gql_query = GraphQLQuery("instruments", endpoint)  # no milestone
        sym_attr = Attribute("Symbol", "sym", "STRING", "instruments")

        # Dates present but no milestone — no arguments added, still returns all rows
        result = FinderResult(datetime.date(2024, 1, 10),
                               datetime.datetime(2020, 1, 1, 9, 0, 0),
                               [sym_attr], gql_query, None)  # type: ignore[arg-type]
        data = result.to_numpy()
        assert data.shape == (3, 1)

    def test_query_accounts(self, graphql_server):
        endpoint = _endpoint(graphql_server)
        gql_query = GraphQLQuery("accounts", endpoint)

        id_attr = Attribute("Id", "id", "INT", "accounts")
        name_attr = Attribute("Name", "name", "STRING", "accounts")

        result = FinderResult(None, None, [id_attr, name_attr], gql_query, None)  # type: ignore[arg-type]
        data = result.to_numpy()

        assert_array_equal(data, np.array(
            [[1, "Trading Account 1"], [2, "Trading Account 2"]],
            dtype="object",
        ))

    def test_to_pandas(self, graphql_server):
        endpoint = _endpoint(graphql_server)
        gql_query = GraphQLQuery("instruments", endpoint)

        sym_attr = Attribute("Symbol", "sym", "STRING", "instruments")
        price_attr = Attribute("Price", "price", "FLOAT", "instruments")

        result = FinderResult(None, None, [sym_attr, price_attr], gql_query, None)  # type: ignore[arg-type]
        df = result.to_pandas()

        assert list(df.columns) == ["sym", "price"]
        assert len(df) == 3
        assert df["sym"].tolist() == ["AAPL", "MSFT", "GOOG"]
