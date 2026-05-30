import os
import tempfile

from mapping_markdown.graphql_mapping_markdown import load, save, to_markdown
from model.graphql_mapping import (
    GraphQLClassMapping,
    GraphQLProcessingMilestone,
    GraphQLBusinessDateMilestone,
    GraphQLBiTemporalMilestone,
)

FIXTURE = os.path.join(os.path.dirname(__file__), "finance_graphql_mapping.md")
TEST_DIR = os.path.dirname(__file__)


class TestGraphQLMarkdownLoad:

    def setup_method(self):
        self.mapping = load(FIXTURE)
        self.by_class = {cm.clazz.name: cm for cm in self.mapping.mappings}

    def test_mapping_title(self):
        assert self.mapping.name == "Finance GraphQL Mapping"

    def test_all_classes_mapped(self):
        assert set(self.by_class.keys()) == {"Account", "Instrument", "Trade", "ContractualPosition"}

    def test_account_field_mappings(self):
        cm = self.by_class["Account"]
        by_prop = {pm.property.id: pm for pm in cm.property_mappings}
        assert by_prop["id"].target.name == "id"
        assert by_prop["name"].target.name == "name"

    def test_account_query_name(self):
        assert self.by_class["Account"].query.name == "accounts"

    def test_endpoint_url(self):
        assert self.by_class["Account"].query.endpoint.url == "http://localhost:4000/graphql"

    def test_all_queries_share_same_endpoint(self):
        urls = {cm.query.endpoint.url for cm in self.mapping.mappings}
        assert urls == {"http://localhost:4000/graphql"}

    def test_account_has_no_milestone(self):
        assert self.by_class["Account"].query.milestone is None

    def test_instrument_has_processing_milestone(self):
        ms = self.by_class["Instrument"].query.milestone
        assert isinstance(ms, GraphQLProcessingMilestone)
        assert ms.argument_name == "asOf"

    def test_instrument_field_mappings(self):
        cm = self.by_class["Instrument"]
        by_prop = {pm.property.id: pm for pm in cm.property_mappings}
        assert by_prop["symbol"].target.name == "symbol"
        assert by_prop["price"].target.name == "price"

    def test_position_has_business_date_milestone(self):
        ms = self.by_class["ContractualPosition"].query.milestone
        assert isinstance(ms, GraphQLBusinessDateMilestone)
        assert ms.argument_name == "businessDate"

    def test_position_query_name(self):
        assert self.by_class["ContractualPosition"].query.name == "contractualPositions"

    def test_position_field_mappings(self):
        cm = self.by_class["ContractualPosition"]
        by_prop = {pm.property.id: pm for pm in cm.property_mappings}
        assert by_prop["businessDate"].target.name == "businessDate"
        assert by_prop["quantity"].target.name == "quantity"
        assert by_prop["npv"].target.name == "npv"

    def test_trade_has_bitemporal_milestone(self):
        ms = self.by_class["Trade"].query.milestone
        assert isinstance(ms, GraphQLBiTemporalMilestone)
        assert ms.business_date_argument == "businessDate"
        assert ms.processing_argument == "asOf"

    def test_trade_query_name(self):
        assert self.by_class["Trade"].query.name == "trades"

    def test_trade_field_mappings(self):
        cm = self.by_class["Trade"]
        by_prop = {pm.property.id: pm for pm in cm.property_mappings}
        assert by_prop["symbol"].target.name == "symbol"
        assert by_prop["price"].target.name == "price"
        assert by_prop["isSettled"].target.name == "isSettled"

    def test_all_mappings_are_graphql_class_mappings(self):
        for cm in self.mapping.mappings:
            assert isinstance(cm, GraphQLClassMapping)


class TestGraphQLMarkdownRoundTrip:

    def test_to_markdown_contains_endpoint(self):
        mapping = load(FIXTURE)
        md = to_markdown("Finance GraphQL Mapping", mapping, ["finance.md", "finance_trade.md"])
        assert "## Endpoint: http://localhost:4000/graphql" in md

    def test_to_markdown_contains_query_headings(self):
        mapping = load(FIXTURE)
        md = to_markdown("Finance GraphQL Mapping", mapping, ["finance.md", "finance_trade.md"])
        assert "### Query: accounts → Account" in md
        assert "### Query: instruments → Instrument (milestone: processing, asOf)" in md
        assert "### Query: contractualPositions → ContractualPosition (milestone: business_date, businessDate)" in md
        assert "### Query: trades → Trade (milestone: bitemporal, businessDate, asOf)" in md

    def test_to_markdown_contains_model_references(self):
        mapping = load(FIXTURE)
        md = to_markdown("Finance GraphQL Mapping", mapping, ["finance.md", "finance_trade.md"])
        assert "## Model: finance.md" in md
        assert "## Model: finance_trade.md" in md

    def test_round_trip_preserves_classes(self):
        mapping = load(FIXTURE)
        md = to_markdown("Finance GraphQL Mapping", mapping, ["finance.md", "finance_trade.md"])
        tmp = os.path.join(TEST_DIR, "_tmp_graphql_rt.md")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(md)
            reloaded = load(tmp)
            assert {cm.clazz.name for cm in reloaded.mappings} == {cm.clazz.name for cm in mapping.mappings}
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def test_round_trip_preserves_milestones(self):
        mapping = load(FIXTURE)
        md = to_markdown("Finance GraphQL Mapping", mapping, ["finance.md", "finance_trade.md"])
        tmp = os.path.join(TEST_DIR, "_tmp_graphql_ms.md")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(md)
            reloaded = load(tmp)
            by_class = {cm.clazz.name: cm for cm in reloaded.mappings}
            assert isinstance(by_class["Instrument"].query.milestone, GraphQLProcessingMilestone)
            assert isinstance(by_class["ContractualPosition"].query.milestone, GraphQLBusinessDateMilestone)
            assert isinstance(by_class["Trade"].query.milestone, GraphQLBiTemporalMilestone)
            assert by_class["Account"].query.milestone is None
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def test_save_and_load(self):
        mapping = load(FIXTURE)
        tmp = os.path.join(TEST_DIR, "_tmp_graphql_save.md")
        try:
            save(tmp, "Finance GraphQL Mapping", mapping, ["finance.md", "finance_trade.md"])
            reloaded = load(tmp)
            assert {cm.clazz.name for cm in reloaded.mappings} == {"Account", "Instrument", "Trade", "ContractualPosition"}
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
