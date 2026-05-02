import os
import pytest

from mapping_markdown.validate import validate, validate_file
from model.relational import Table, Column


FIXTURE = os.path.join(os.path.dirname(__file__), "finance_mapping.md")
SPLIT_FIXTURE = os.path.join(os.path.dirname(__file__), "finance_mapping_split.md")


class TestTableDuplicateColumn:

    def test_unique_columns_accepted(self):
        Table("t", [Column("a", "INT"), Column("b", "VARCHAR")])

    def test_duplicate_column_raises(self):
        with pytest.raises(ValueError, match="Duplicate column name 'a' in table 't'"):
            Table("t", [Column("a", "INT"), Column("a", "VARCHAR")])

    def test_error_names_the_table(self):
        with pytest.raises(ValueError, match="my_table"):
            Table("my_table", [Column("x", "INT"), Column("x", "INT")])


class TestValidateMarkdown:

    def test_valid_mapping_has_no_errors(self):
        result = validate_file(FIXTURE)
        assert result.valid

    def test_split_mapping_has_no_errors(self):
        result = validate_file(SPLIT_FIXTURE)
        assert result.valid

    def test_duplicate_column_detected(self):
        content = """\
# Test

## Repository: db

### Schema: s

#### Table: my_table → MyClass

| Column | Type |
|--------|------|
| id     | INT  |
| id     | INT  |
"""
        result = validate(content)
        assert not result.valid
        assert any("id" in e.message and "my_table" in e.message for e in result.errors)

    def test_missing_include_file_detected(self):
        content = """\
# Test

## Repository: db

## Schema: nonexistent_schema.md
"""
        result = validate(content, base_dir="/tmp")
        assert not result.valid
        assert any("nonexistent_schema.md" in e.message for e in result.errors)

    def test_valid_result_str(self):
        result = validate_file(FIXTURE)
        assert str(result) == "OK"

    def test_invalid_result_str_contains_message(self):
        content = """\
# Test

## Repository: db

### Schema: s

#### Table: t → C

| Column | Type |
|--------|------|
| x      | INT  |
| x      | INT  |
"""
        result = validate(content)
        assert "x" in str(result)
