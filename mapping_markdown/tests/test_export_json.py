import json
import os

import pytest

from mapping_markdown import export_json

EXAMPLES = os.path.join(os.path.dirname(__file__), "..", "..", "datafinder_examples", "src", "datafinder_examples")


@pytest.fixture(scope="module")
def finance():
    return export_json.export_mapping(os.path.join(EXAMPLES, "finance_mapping.md"), "finance_mapping.md", EXAMPLES)


def _model(name):
    return export_json.export_model(os.path.join(EXAMPLES, name), name)


def test_model_classes_properties_and_docs():
    package = _model("finance.md")["packages"][0]
    assert package["name"] == "finance.reference_data"
    account = next(c for c in package["classes"] if c["name"] == "Account")
    assert account["description"] == "Trading account used to buy and sell securities"
    # own properties only: the association-derived navigation property is not a property of the class
    assert [p["id"] for p in account["properties"]] == ["id", "name"]
    assert account["properties"][0] == {
        "name": "Id", "id": "id", "type": "Integer", "multiplicity": "1", "key": True, "description": "",
    }
    branch = next(c for c in package["classes"] if c["name"] == "Branch")
    assert next(p for p in branch["properties"] if p["id"] == "city")["multiplicity"] == "0..1"


def test_model_association_ends_are_properties_on_their_own_class():
    association = _model("finance.md")["packages"][0]["associations"][0]
    assert association["name"] == "AccountBranch"
    # Account.branch (0..1) returns a Branch; Branch.accounts (*) returns Accounts
    assert (association["source"], association["sourcePropertyId"], association["sourceMultiplicity"]) == ("Account", "branch", "0..1")
    assert (association["target"], association["targetPropertyId"], association["targetMultiplicity"]) == ("Branch", "accounts", "*")
    assert (association["sourcePropertyName"], association["targetPropertyName"]) == ("Branch", "Accounts")


def test_one_way_association_has_no_target_property():
    package = _model("finance_trade.md")["packages"][0]
    one_way = next(a for a in package["associations"] if a["name"] == "TradeInstrument")
    assert (one_way["targetPropertyName"], one_way["targetPropertyId"]) == (None, None)
    assert one_way["targetMultiplicity"] == "*"                              # still declared
    assert one_way["sourcePropertyId"] == "instrument"


def test_inheritance_is_exported():
    classes = {c["name"]: c for c in _model("orgchart_inheritance.md")["packages"][0]["classes"]}
    assert classes["Employee"]["superclasses"] == ["Person", "Contactable"]
    assert classes["Person"]["superclasses"] == []


def test_mapping_tables_columns_and_classes(finance):
    assert finance["title"] == "Finance Mapping"
    assert finance["models"] == ["finance.md", "finance_trade.md"]
    assert finance["dataStore"] == {"name": "finance_db", "type": "Database"}
    tables = {(s["name"], t["name"]): t for s in finance["schemas"] for t in s["tables"]}
    assert set(tables) == {("ref_data", "account_master"), ("ref_data", "branch"), ("ref_data", "desk"), ("ref_data", "price"),
                           ("trading", "trades"), ("trading", "contractualposition")}
    account = tables[("ref_data", "account_master")]
    assert account["class"] == "Account"
    columns = {c["name"]: c for c in account["columns"]}
    assert columns["ID"] == {"name": "ID", "type": "INT", "primaryKey": True, "nullable": False, "property": "id"}
    assert columns["ACCT_NAME"]["property"] == "name"
    assert columns["in_z"]["property"] == "validFrom"                       # milestoning column
    assert columns["BRANCH_ID"]["property"] is None                         # a foreign key: mapped through the association


def test_mapping_associations_carry_columns_and_multiplicities(finance):
    by_name = {a["name"]: a for a in finance["associations"]}
    assert by_name["AccountBranch"]["rows"] == [{
        "sourceSchema": "ref_data", "sourceTable": "account_master", "sourceColumn": "BRANCH_ID",
        "targetSchema": "ref_data", "targetTable": "branch", "targetColumn": "ID",
        "sourceIsQuery": False, "targetIsQuery": False,
    }]
    assert (by_name["AccountBranch"]["sourceMultiplicity"], by_name["AccountBranch"]["targetMultiplicity"]) == ("0..1", "*")
    assert by_name["TradeInstrument"]["targetMultiplicity"] == "*"             # one-way: the join is still mapped
    trade_account = by_name["TradeAccount"]["rows"][0]                      # a foreign key across schemas
    assert (trade_account["sourceSchema"], trade_account["targetSchema"]) == ("trading", "ref_data")


@pytest.fixture(scope="module")
def sql_queries():
    return export_json.export_mapping(os.path.join(EXAMPLES, "sql_queries_mapping.md"), "sql_queries_mapping.md", EXAMPLES)


def test_a_query_a_class_is_mapped_to_is_exported_with_its_sql_and_columns(sql_queries):
    queries = {q["name"]: q for q in sql_queries["queries"]}
    assert set(queries) == {"FIRM_VIEW", "POSITION_VIEW", "FIRM_X_PEOPLE", "TAG_VIEW", "SHOUT_VIEW", "RATE_QUERY", "QUOTE_QUERY"}
    firm = queries["FIRM_VIEW"]
    assert firm["class"] == "Firm"
    assert firm["sql"] == "SELECT DISTINCT firm_id, firm_name\nFROM people"
    assert firm["reads"] == [{"schema": "org", "table": "people"}]
    assert firm["milestoning"] is None
    assert firm["columns"] == [
        {"name": "firm_id", "type": "INT", "primaryKey": True, "nullable": False, "property": "id"},
        {"name": "firm_name", "type": "VARCHAR", "primaryKey": False, "nullable": True, "property": "legalName"},
    ]


def test_a_query_that_reads_a_query_names_it_and_a_milestoned_one_names_its_scheme(sql_queries):
    queries = {q["name"]: q for q in sql_queries["queries"]}
    assert queries["SHOUT_VIEW"]["reads"] == [{"query": "FIRM_VIEW"}]
    assert queries["RATE_QUERY"]["milestoning"] == "business_date_range"
    assert queries["QUOTE_QUERY"]["milestoning"] == "bitemporal"
    properties = {c["name"]: c["property"] for c in queries["QUOTE_QUERY"]["columns"]}
    assert properties["in_z"] == "validFrom" and properties["thru_z"] == "businessThru"


def test_a_query_is_in_no_schema_and_a_class_mapped_to_one_is_not_lost(sql_queries):
    tables = {t["name"]: t for s in sql_queries["schemas"] for t in s["tables"]}
    assert "FIRM_VIEW" not in tables and tables["people"]["class"] == "Person"  # the table is still mapped to its own class
    assert tables["rates"]["class"] is None  # read by a query only


def test_a_join_that_starts_from_a_query_has_no_schema_and_says_so(sql_queries):
    by_name = {a["name"]: a for a in sql_queries["associations"]}
    assert by_name["FirmPerson"]["rows"] == [{
        "sourceSchema": None, "sourceTable": "FIRM_VIEW", "sourceColumn": "firm_id",
        "targetSchema": "org", "targetTable": "people", "targetColumn": "firm_id",
        "sourceIsQuery": True, "targetIsQuery": False,
    }]
    assert by_name["HolderRate"]["rows"][0]["targetIsQuery"] is True        # a join that ends at a query
    assert by_name["HolderRate"]["rows"][0]["targetTable"] == "RATE_QUERY"


def test_dir_can_name_an_installed_package(tmp_path):
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"dir": "python:datafinder_examples", "models": ["diamond_model.md"]}))
    assert export_json.export(str(config))["models"][0]["file"] == "diamond_model.md"
    assert os.path.samefile(export_json.resolve_dir(str(config), "python:datafinder_examples"), EXAMPLES)


def test_export_from_config_includes_models_the_mappings_reference(tmp_path):
    config = tmp_path / "config.json"
    config.write_text(json.dumps({
        "dir": os.path.relpath(EXAMPLES, tmp_path), "models": ["positions_model.md"],
        "mappings": ["finance_mapping.md", {"path": "positions_mapping.md"}],
    }))
    data = export_json.export(str(config))
    assert [m["file"] for m in data["models"]] == ["positions_model.md", "finance.md", "finance_trade.md"]
    assert [m["file"] for m in data["mappings"]] == ["finance_mapping.md", "positions_mapping.md"]


def test_cli_writes_json(tmp_path):
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"dir": os.path.relpath(EXAMPLES, tmp_path), "models": ["diamond_model.md"]}))
    out = tmp_path / "nested" / "data.json"
    export_json.main(["--config", str(config), "--out", str(out)])
    assert json.loads(out.read_text())["models"][0]["file"] == "diamond_model.md"
