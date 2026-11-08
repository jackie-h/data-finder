import os
import tempfile

import pytest
from mapping_markdown.graphql_mapping_markdown import load, loads, save, to_markdown
from model.graphql_mapping import (
    GraphQLClassMapping,
    GraphQLAssociationMapping,
    GraphQLProcessingMilestone,
    GraphQLBusinessDateMilestone,
    GraphQLBiTemporalMilestone,
)

FIXTURE = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "datafinder_examples", "src", "datafinder_examples", "finance_graphql_mapping.md"))
TEST_DIR = os.path.dirname(FIXTURE)


class TestGraphQLMarkdownLoad:

    def setup_method(self):
        self.mapping = load(FIXTURE)
        self.by_class = {cm.clazz.name: cm for cm in self.mapping.mappings if isinstance(cm, GraphQLClassMapping)}

    def test_mapping_title(self):
        assert self.mapping.name == "Finance GraphQL Mapping"

    def test_all_classes_mapped(self):
        assert set(self.by_class.keys()) == {"Account", "Branch", "Desk", "Instrument", "Trade", "ContractualPosition"}

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
        urls = {cm.query.endpoint.url for cm in self.mapping.mappings if isinstance(cm, GraphQLClassMapping)}
        assert urls == {"http://localhost:4000/graphql"}

    def test_account_has_processing_milestone(self):
        ms = self.by_class["Account"].query.milestone
        assert isinstance(ms, GraphQLProcessingMilestone)
        assert ms.argument_name == "asOf"

    def test_branch_has_no_milestone(self):
        assert self.by_class["Branch"].query.milestone is None

    def test_instrument_has_processing_milestone(self):
        ms = self.by_class["Instrument"].query.milestone
        assert isinstance(ms, GraphQLProcessingMilestone)
        assert ms.argument_name == "asOf"

    def test_instrument_field_mappings(self):
        cm = self.by_class["Instrument"]
        by_prop = {pm.property.id: pm for pm in cm.property_mappings}
        assert by_prop["symbol"].target.name == "symbol"
        assert by_prop["price"].target.name == "price"

    def test_position_has_bitemporal_milestone(self):
        ms = self.by_class["ContractualPosition"].query.milestone
        assert isinstance(ms, GraphQLBiTemporalMilestone)
        assert ms.business_date_argument == "businessDate"
        assert ms.processing_argument == "asOf"

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
        assert "### Query: accounts → Account (milestone: processing, asOf)" in md
        assert "### Query: instruments → Instrument (milestone: processing, asOf)" in md
        assert "### Query: contractualPositions → ContractualPosition (milestone: bitemporal, businessDate, asOf)" in md
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
            by_class = {cm.clazz.name: cm for cm in reloaded.mappings if isinstance(cm, GraphQLClassMapping)}
            assert isinstance(by_class["Instrument"].query.milestone, GraphQLProcessingMilestone)
            assert isinstance(by_class["ContractualPosition"].query.milestone, GraphQLBiTemporalMilestone)
            assert isinstance(by_class["Trade"].query.milestone, GraphQLBiTemporalMilestone)
            assert isinstance(by_class["Account"].query.milestone, GraphQLProcessingMilestone)
            assert by_class["Branch"].query.milestone is None
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def test_save_and_load(self):
        mapping = load(FIXTURE)
        tmp = os.path.join(TEST_DIR, "_tmp_graphql_save.md")
        try:
            save(tmp, "Finance GraphQL Mapping", mapping, ["finance.md", "finance_trade.md"])
            reloaded = load(tmp)
            assert {cm.clazz.name for cm in reloaded.mappings} == {"Account", "Branch", "Desk", "Instrument", "Trade", "ContractualPosition"}
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)


class TestGraphQLAssociationMapping:

    def setup_method(self):
        self.mapping = load(FIXTURE)
        self.by_class = {cm.clazz.name: cm for cm in self.mapping.mappings}

    def test_trade_has_account_association(self):
        cm = self.by_class["Trade"]
        assoc_mappings = [pm for pm in cm.property_mappings if isinstance(pm, GraphQLAssociationMapping)]
        assert [pm.association_name for pm in assoc_mappings] == ["TradeAccount", "TradeInstrument"]

    def test_trade_account_association_graphql_field(self):
        cm = self.by_class["Trade"]
        assoc_mappings = [pm for pm in cm.property_mappings if isinstance(pm, GraphQLAssociationMapping)]
        assert assoc_mappings[0].target.name == "account"

    def test_trade_account_association_nav_property(self):
        cm = self.by_class["Trade"]
        assoc_mappings = [pm for pm in cm.property_mappings if isinstance(pm, GraphQLAssociationMapping)]
        assert assoc_mappings[0].property.id == "account"

    def test_trade_account_association_target_class(self):
        cm = self.by_class["Trade"]
        assoc_mappings = [pm for pm in cm.property_mappings if isinstance(pm, GraphQLAssociationMapping)]
        prop_type = assoc_mappings[0].property.type
        assert prop_type is not None
        assert prop_type.name == "Account"

    @staticmethod
    def _target_name(pm) -> str:
        assert pm.property.type is not None
        return pm.property.type.name

    def _associations(self, class_name):
        cm = self.by_class[class_name]
        return {pm.association_name + ":" + pm.target.name: pm
                for pm in cm.property_mappings if isinstance(pm, GraphQLAssociationMapping)}

    def test_account_has_branch_desk_and_positions_associations(self):
        assocs = self._associations("Account")
        assert set(assocs) == {"AccountBranch:branch", "AccountDesk:desk", "PositionAccount:positions"}
        assert self._target_name(assocs["AccountBranch:branch"]) == "Branch"
        assert self._target_name(assocs["AccountDesk:desk"]) == "Desk"
        assert self._target_name(assocs["PositionAccount:positions"]) == "ContractualPosition"

    def test_the_accounts_of_a_branch_and_of_a_desk_are_to_many(self):
        branch = self._associations("Branch")["AccountBranch:accounts"]
        desk = self._associations("Desk")["AccountDesk:accounts"]
        for pm in (branch, desk):
            assert self._target_name(pm) == "Account"
            assert pm.property.multiplicity.is_many()

    def test_a_position_has_an_account(self):
        assocs = self._associations("ContractualPosition")
        assert set(assocs) == {"PositionAccount:account"}
        assert self._target_name(assocs["PositionAccount:account"]) == "Account"

    def test_a_trade_has_an_instrument(self):
        assocs = self._associations("Trade")
        assert set(assocs) == {"TradeAccount:account", "TradeInstrument:instrument"}
        assert self._target_name(assocs["TradeInstrument:instrument"]) == "Instrument"
        assert not assocs["TradeInstrument:instrument"].property.multiplicity.is_many()

    def test_a_class_with_no_association_has_no_association_mappings(self):
        assert self._associations("Instrument") == {}

    def test_round_trip_preserves_association(self):
        mapping = load(FIXTURE)
        md = to_markdown("Finance GraphQL Mapping", mapping, ["finance.md", "finance_trade.md"])
        tmp = os.path.join(TEST_DIR, "_tmp_graphql_assoc_rt.md")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(md)
            reloaded = load(tmp)
            by_class = {cm.clazz.name: cm for cm in reloaded.mappings}
            assoc_mappings = [pm for pm in by_class["Trade"].property_mappings if isinstance(pm, GraphQLAssociationMapping)]
            assert [pm.association_name for pm in assoc_mappings] == ["TradeAccount", "TradeInstrument"]
            assert assoc_mappings[0].target.name == "account"
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def test_to_markdown_contains_association_section(self):
        mapping = load(FIXTURE)
        md = to_markdown("Finance GraphQL Mapping", mapping, ["finance.md", "finance_trade.md"])
        assert "#### Association: TradeAccount" in md
        assert "account" in md

    def test_to_markdown_association_not_in_field_table(self):
        mapping = load(FIXTURE)
        md = to_markdown("Finance GraphQL Mapping", mapping, ["finance.md", "finance_trade.md"])
        # The account field should appear in the association section, not in the Field/Property ID table
        trade_section = md[md.index("### Query: trades"):]
        field_table_end = trade_section.index("#### Association:")
        field_table_section = trade_section[:field_table_end]
        assert "account" not in field_table_section


class TestGraphQLFieldMappingHeaderValidation:

    def _make_pkg(self):
        from model.m3 import Package, Class, Property, Integer, ONE_TO_ONE
        pkg = Package("test")
        Class("Account", [Property("Id", "id", Integer, ONE_TO_ONE)], pkg)
        return pkg

    def _mapping_with_headers(self, header_row: str, separator_row: str) -> str:
        return f"""\
# Test GraphQL Mapping

## Endpoint: http://localhost/graphql

### Query: accounts → Account

{header_row}
{separator_row}
| id | id |
"""

    def test_property_column_raises(self):
        content = self._mapping_with_headers(
            "| Field | Property |",
            "|-------|----------|",
        )
        with pytest.raises(ValueError, match="unexpected headers.*'Property'.*expected.*'Property ID'"):
            loads(content, packages=[self._make_pkg()])

    def test_wrong_order_raises(self):
        content = self._mapping_with_headers(
            "| Property ID | Field |",
            "|-------------|-------|",
        )
        with pytest.raises(ValueError, match="unexpected headers"):
            loads(content, packages=[self._make_pkg()])

    def test_missing_property_id_raises(self):
        content = self._mapping_with_headers(
            "| Field |",
            "|-------|",
        )
        with pytest.raises(ValueError, match="unexpected headers"):
            loads(content, packages=[self._make_pkg()])
