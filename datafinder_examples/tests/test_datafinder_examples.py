"""Verify that all bundled example files are accessible and loadable."""
import csv

from datafinder_examples import example_path
from mapping_markdown.markdown_mapping import load as load_relational
from mapping_markdown.graphql_mapping_markdown import load as load_graphql
from model.graphql_mapping import (
    GraphQLClassMapping,
    GraphQLProcessingMilestone,
    GraphQLBusinessDateMilestone,
    GraphQLBiTemporalMilestone,
)


class TestExampleFilesExist:

    def test_finance_model(self):
        p = example_path("finance.md")
        assert p.exists()
        assert "Account" in p.read_text()
        assert "Instrument" in p.read_text()

    def test_finance_trade_model(self):
        p = example_path("finance_trade.md")
        assert p.exists()
        assert "Trade" in p.read_text()
        assert "ContractualPosition" in p.read_text()

    def test_finance_mapping(self):
        p = example_path("finance_mapping.md")
        assert p.exists()
        assert "DataStore" in p.read_text()

    def test_finance_graphql_mapping(self):
        p = example_path("finance_graphql_mapping.md")
        assert p.exists()
        assert "Endpoint" in p.read_text()

    def test_trades_csv(self):
        p = example_path("trades.csv")
        assert p.exists()
        rows = list(csv.DictReader(p.open()))
        assert len(rows) > 0
        assert "sym" in rows[0]
        assert "price" in rows[0]

    def test_accounts_csv(self):
        p = example_path("accounts.csv")
        assert p.exists()
        rows = list(csv.DictReader(p.open()))
        assert len(rows) > 0
        assert "ACCT_NAME" in rows[0]

    def test_prices_csv(self):
        p = example_path("prices.csv")
        assert p.exists()
        rows = list(csv.DictReader(p.open()))
        assert len(rows) > 0
        assert "SYM" in rows[0]
        assert "PRICE" in rows[0]

    def test_contractualpositions_csv(self):
        p = example_path("contractualpositions.csv")
        assert p.exists()
        rows = list(csv.DictReader(p.open()))
        assert len(rows) > 0
        assert "INSTRUMENT" in rows[0]
        assert "QUANTITY" in rows[0]


class TestExampleMappingsLoadable:

    def test_relational_mapping_loads(self):
        mapping = load_relational(str(example_path("finance_mapping.md")))
        class_names = {cm.clazz.name for cm in mapping.mappings}
        assert {"Account", "Instrument", "Trade", "ContractualPosition"}.issubset(class_names)

    def test_graphql_mapping_loads(self):
        mapping = load_graphql(str(example_path("finance_graphql_mapping.md")))
        by_class = {cm.clazz.name: cm for cm in mapping.mappings}
        assert set(by_class.keys()) == {"Account", "Instrument", "Trade", "ContractualPosition"}

    def test_graphql_mapping_milestones(self):
        mapping = load_graphql(str(example_path("finance_graphql_mapping.md")))
        by_class = {cm.clazz.name: cm for cm in mapping.mappings if isinstance(cm, GraphQLClassMapping)}
        assert isinstance(by_class["Instrument"].query.milestone, GraphQLProcessingMilestone)
        assert isinstance(by_class["ContractualPosition"].query.milestone, GraphQLBiTemporalMilestone)
        assert isinstance(by_class["Trade"].query.milestone, GraphQLBiTemporalMilestone)
        assert by_class["Account"].query.milestone is None

    def test_graphql_mapping_endpoint(self):
        mapping = load_graphql(str(example_path("finance_graphql_mapping.md")))
        urls = {cm.query.endpoint.url for cm in mapping.mappings if isinstance(cm, GraphQLClassMapping)}
        assert urls == {"http://localhost:4000/graphql"}
