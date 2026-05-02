import os
import tempfile

import pytest

from mapping_markdown.refresh import refresh_mapping, refresh_mapping_content
from model.relational import Repository, Schema, Table, Column


def _repo(schema_tables: dict[str, list[tuple[str, list[tuple[str, str, bool]]]]]) -> Repository:
    """
    Build a Repository from a dict:
      {schema_name: [(table_name, [(col_name, col_type, is_pk), ...]), ...]}
    """
    repo = Repository("db", "duckdb://test.db")
    for schema_name, tables in schema_tables.items():
        schema = Schema(schema_name, repo)
        for table_name, cols in tables:
            columns = [Column(name, typ, primary_key=pk) for name, typ, pk in cols]
            Table(table_name, columns, schema)
    return repo


SIMPLE_MAPPING = """\
# Test Mapping

## Repository: db

### Schema: s

#### Table: my_table → MyClass

| Column | Type    | Key | Property |
|--------|---------|-----|----------|
| id     | INT     | PK  | id       |
| name   | VARCHAR |     | name     |
"""


class TestNoChanges:
    def test_existing_columns_preserved(self):
        repo = _repo({"s": [("my_table", [("id", "INT", True), ("name", "VARCHAR", False)])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        assert "id" in result
        assert "name" in result

    def test_property_mappings_retained(self):
        repo = _repo({"s": [("my_table", [("id", "INT", True), ("name", "VARCHAR", False)])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        lines = [l.strip() for l in result.splitlines() if "id" in l and "|" in l]
        assert any("id" in l and "id" in l.split("|")[-2] for l in lines)

    def test_table_class_heading_preserved(self):
        repo = _repo({"s": [("my_table", [("id", "INT", True), ("name", "VARCHAR", False)])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        assert "#### Table: my_table → MyClass" in result

    def test_preamble_preserved(self):
        repo = _repo({"s": [("my_table", [("id", "INT", True), ("name", "VARCHAR", False)])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        assert "# Test Mapping" in result
        assert "## Repository: db" in result


class TestAddColumn:
    def test_new_column_appears_in_output(self):
        repo = _repo({"s": [("my_table", [
            ("id", "INT", True), ("name", "VARCHAR", False), ("email", "VARCHAR", False),
        ])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        assert "email" in result

    def test_new_column_has_empty_property(self):
        repo = _repo({"s": [("my_table", [
            ("id", "INT", True), ("name", "VARCHAR", False), ("email", "VARCHAR", False),
        ])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        for line in result.splitlines():
            if "email" in line and "|" in line:
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                assert cells[-1] == "", f"Expected empty property for email, got: {cells}"
                break
        else:
            pytest.fail("email column not found in output")

    def test_new_column_type_set(self):
        repo = _repo({"s": [("my_table", [
            ("id", "INT", True), ("name", "VARCHAR", False), ("created_at", "TIMESTAMP", False),
        ])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        assert "TIMESTAMP" in result

    def test_existing_columns_still_present(self):
        repo = _repo({"s": [("my_table", [
            ("id", "INT", True), ("name", "VARCHAR", False), ("extra", "TEXT", False),
        ])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        assert "id" in result
        assert "name" in result

    def test_new_pk_column_gets_pk_key(self):
        repo = _repo({"s": [("my_table", [
            ("id", "INT", True), ("name", "VARCHAR", False), ("code", "VARCHAR", True),
        ])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        for line in result.splitlines():
            if "code" in line and "|" in line:
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                assert "PK" in cells
                break


class TestRemoveColumn:
    def test_deleted_column_absent(self):
        repo = _repo({"s": [("my_table", [("id", "INT", True)])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        col_lines = [l for l in result.splitlines() if "|" in l and "name" in l and "---" not in l and "Column" not in l]
        assert not col_lines, "Deleted column 'name' should not appear in output"

    def test_remaining_column_preserved(self):
        repo = _repo({"s": [("my_table", [("id", "INT", True)])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        assert "id" in result

    def test_property_of_remaining_column_preserved(self):
        repo = _repo({"s": [("my_table", [("id", "INT", True)])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        for line in result.splitlines():
            if "id" in line and "|" in line and "---" not in line and "Column" not in line:
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                assert "id" in cells  # property 'id' kept
                break


class TestAddAndRemoveColumns:
    def test_combined_add_and_remove(self):
        repo = _repo({"s": [("my_table", [
            ("id", "INT", True), ("email", "VARCHAR", False),  # 'name' removed, 'email' added
        ])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        assert "email" in result
        col_lines = [l for l in result.splitlines() if "|" in l and "name" in l and "---" not in l and "Column" not in l]
        assert not col_lines, "'name' should be removed"


class TestTypeUpdate:
    def test_type_updated_from_new_schema(self):
        repo = _repo({"s": [("my_table", [
            ("id", "BIGINT", True), ("name", "TEXT", False),
        ])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        assert "BIGINT" in result
        assert "TEXT" in result

    def test_property_preserved_when_type_changes(self):
        repo = _repo({"s": [("my_table", [
            ("id", "BIGINT", True), ("name", "TEXT", False),
        ])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        for line in result.splitlines():
            if "name" in line and "|" in line and "---" not in line and "Column" not in line:
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                assert "name" in cells  # property still mapped
                break


class TestNewTableInExistingSchema:
    def test_new_table_appended(self):
        repo = _repo({"s": [
            ("my_table", [("id", "INT", True), ("name", "VARCHAR", False)]),
            ("new_table", [("code", "VARCHAR", True), ("value", "DOUBLE", False)]),
        ]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        assert "new_table" in result

    def test_new_table_heading_uses_placeholder(self):
        repo = _repo({"s": [
            ("my_table", [("id", "INT", True), ("name", "VARCHAR", False)]),
            ("new_table", [("code", "VARCHAR", True)]),
        ]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        assert "#### Table: new_table → ?" in result

    def test_new_table_columns_present(self):
        repo = _repo({"s": [
            ("my_table", [("id", "INT", True), ("name", "VARCHAR", False)]),
            ("new_table", [("code", "VARCHAR", True), ("value", "DOUBLE", False)]),
        ]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        assert "code" in result
        assert "value" in result

    def test_new_table_in_same_schema_not_duplicated_as_new_schema(self):
        repo = _repo({"s": [
            ("my_table", [("id", "INT", True), ("name", "VARCHAR", False)]),
            ("new_table", [("code", "VARCHAR", True)]),
        ]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        # Schema heading should appear exactly once
        assert result.count("### Schema: s") == 1


class TestNewSchema:
    def test_new_schema_appended(self):
        repo = _repo({
            "s": [("my_table", [("id", "INT", True), ("name", "VARCHAR", False)])],
            "analytics": [("metrics", [("metric_name", "VARCHAR", False), ("value", "DOUBLE", False)])],
        })
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        assert "### Schema: analytics" in result
        assert "metrics" in result

    def test_new_schema_table_has_placeholder_class(self):
        repo = _repo({
            "s": [("my_table", [("id", "INT", True), ("name", "VARCHAR", False)])],
            "analytics": [("metrics", [("metric_name", "VARCHAR", False)])],
        })
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        assert "#### Table: metrics → ?" in result


class TestTableNotInNewRepo:
    def test_table_absent_from_repo_is_kept(self):
        # new repo has no 'my_table' — the mapping table should be preserved
        repo = _repo({"s": [("other_table", [("x", "INT", False)])]})
        result = refresh_mapping_content(SIMPLE_MAPPING, repo)
        assert "my_table" in result


class TestAssociationsPreserved:
    MAPPING_WITH_ASSOC = """\
# Finance Mapping

## Repository: db

### Schema: trading

#### Table: trades → Trade

| Column     | Type | Key | Property   |
|------------|------|-----|------------|
| account_id | INT  | FK  | account    |

#### Association: TradeAccount

| Source Column | Target Table   | Target Column |
|---------------|----------------|---------------|
| account_id    | account_master | ID            |
"""

    def test_association_section_preserved(self):
        repo = _repo({"trading": [
            ("trades", [("account_id", "INT", False), ("sym", "VARCHAR", False)]),
        ]})
        result = refresh_mapping_content(self.MAPPING_WITH_ASSOC, repo)
        assert "#### Association: TradeAccount" in result
        assert "account_master" in result

    def test_fk_key_preserved_for_existing_column(self):
        repo = _repo({"trading": [
            ("trades", [("account_id", "INT", False)]),
        ]})
        result = refresh_mapping_content(self.MAPPING_WITH_ASSOC, repo)
        for line in result.splitlines():
            if "account_id" in line and "|" in line and "---" not in line and "Column" not in line:
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                assert "FK" in cells
                break


class TestMilestoningPreserved:
    MAPPING_WITH_MILESTONING = """\
# Finance Mapping

## Repository: db

| Scheme          | processing_start | processing_end | business_date | business_date_from | business_date_to |
|-----------------|------------------|----------------|---------------|--------------------|------------------|
| processing_only | in_z             | out_z          |               |                    |                  |

### Schema: ref_data

#### Table: price → Instrument (milestoning: processing_only)

| Column | Type      | Key | Property   |
|--------|-----------|-----|------------|
| SYM    | VARCHAR   | PK  | symbol     |
| in_z   | TIMESTAMP |     | valid_from |
| out_z  | TIMESTAMP |     | valid_to   |
"""

    def test_milestoning_heading_preserved(self):
        repo = _repo({"ref_data": [
            ("price", [("SYM", "VARCHAR", True), ("PRICE", "DOUBLE", False),
                       ("in_z", "TIMESTAMP", False), ("out_z", "TIMESTAMP", False)]),
        ]})
        result = refresh_mapping_content(self.MAPPING_WITH_MILESTONING, repo)
        assert "(milestoning: processing_only)" in result

    def test_milestoning_scheme_table_preserved(self):
        repo = _repo({"ref_data": [
            ("price", [("SYM", "VARCHAR", True), ("in_z", "TIMESTAMP", False), ("out_z", "TIMESTAMP", False)]),
        ]})
        result = refresh_mapping_content(self.MAPPING_WITH_MILESTONING, repo)
        assert "processing_only" in result
        assert "in_z" in result


class TestMultipleSchemas:
    MULTI_SCHEMA_MAPPING = """\
# Multi Mapping

## Repository: db

### Schema: schema_a

#### Table: table_a → ClassA

| Column | Type    | Key | Property |
|--------|---------|-----|----------|
| id     | INT     | PK  | id       |
| col_a  | VARCHAR |     | col_a    |

### Schema: schema_b

#### Table: table_b → ClassB

| Column | Type   | Key | Property |
|--------|--------|-----|----------|
| id     | INT    | PK  | id       |
| col_b  | DOUBLE |     | col_b    |
"""

    def test_both_schemas_present(self):
        repo = _repo({
            "schema_a": [("table_a", [("id", "INT", True), ("col_a", "VARCHAR", False)])],
            "schema_b": [("table_b", [("id", "INT", True), ("col_b", "DOUBLE", False)])],
        })
        result = refresh_mapping_content(self.MULTI_SCHEMA_MAPPING, repo)
        assert "### Schema: schema_a" in result
        assert "### Schema: schema_b" in result

    def test_add_column_only_to_correct_schema(self):
        repo = _repo({
            "schema_a": [("table_a", [("id", "INT", True), ("col_a", "VARCHAR", False), ("new_a", "TEXT", False)])],
            "schema_b": [("table_b", [("id", "INT", True), ("col_b", "DOUBLE", False)])],
        })
        result = refresh_mapping_content(self.MULTI_SCHEMA_MAPPING, repo)
        assert "new_a" in result
        # new_a should only appear within schema_a context, not in schema_b
        schema_b_start = result.index("### Schema: schema_b")
        assert "new_a" not in result[schema_b_start:]


class TestFileIO:
    def test_refresh_mapping_writes_file(self):
        mapping_content = SIMPLE_MAPPING
        repo = _repo({"s": [("my_table", [("id", "INT", True), ("name", "VARCHAR", False), ("extra", "TEXT", False)])]})
        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False, encoding="utf-8") as inp:
            inp.write(mapping_content)
            inp_path = inp.name
        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False, encoding="utf-8") as out:
            out_path = out.name
        try:
            result = refresh_mapping(inp_path, repo, output_path=out_path)
            with open(out_path, encoding="utf-8") as f:
                written = f.read()
            assert written == result
            assert "extra" in written
        finally:
            os.unlink(inp_path)
            os.unlink(out_path)

    def test_refresh_mapping_returns_string(self):
        mapping_content = SIMPLE_MAPPING
        repo = _repo({"s": [("my_table", [("id", "INT", True), ("name", "VARCHAR", False)])]})
        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False, encoding="utf-8") as f:
            f.write(mapping_content)
            path = f.name
        try:
            result = refresh_mapping(path, repo)
            assert isinstance(result, str)
            assert "my_table" in result
        finally:
            os.unlink(path)
