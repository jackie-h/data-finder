"""Embedded properties with no table to fall back on: a person row that also holds the firm and addresses."""
import importlib
import shutil
import sys
import tempfile

import pytest

from datafinder_generator.generator import generate
from mapping_markdown.markdown_mapping import load

_MODEL = """\
# Denormalised Model

## Sub-Domain: denorm

### Class: Person

| Name   | Description |
|--------|-------------|
| Person |             |

| Property  | Id        | Type   | Key | Description |
|-----------|-----------|--------|-----|-------------|
| PersonId  | personId  | Integer | Y  |             |
| FirstName | firstName | String |     |             |

### Class: Firm

| Name | Description |
|------|-------------|
| Firm |             |

| Property  | Id        | Type   | Key | Description |
|-----------|-----------|--------|-----|-------------|
| LegalName | legalName | String |     |             |

### Class: Address

| Name    | Description |
|---------|-------------|
| Address |             |

| Property | Id   | Type   | Key | Description |
|----------|------|--------|-----|-------------|
| Name     | name | String |     |             |

### Association: PersonFirm

| Name       | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonFirm | Person | Employees            | employees          | *                   | Firm   | Firm                 | firm               | 1                   |             |

### Association: PersonAddress

| Name          | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|---------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| PersonAddress | Person | Residents            | residents          | *                   | Address | Address              | address            | 1                   |             |

### Association: FirmAddress

| Name        | Source | Source Property Name | Source Property ID | Source Multiplicity | Target  | Target Property Name | Target Property ID | Target Multiplicity | Description |
|-------------|--------|----------------------|--------------------|---------------------|---------|----------------------|--------------------|---------------------|-------------|
| FirmAddress | Firm   | Firms                | firms              | *                   | Address | Address              | address            | 1                   |             |
"""

_MAPPING = """\
# Denormalised Mapping

## Model: denorm_model.md

## DataStore: denorm_db (Database)

### Schema: main

#### Table: PERSON_FIRM_DENORM → Person

| Column              | Type    | Key | Nullable | Property ID  |
|---------------------|---------|-----|----------|--------------|
| PERSON_ID           | INT     | PK  | N        | personId     |
| PERSON_FIRSTNAME    | VARCHAR |     | Y        | firstName    |
| PERSON_ADDRESS_NAME | VARCHAR |     | Y        | address.name |
| FIRM_LEGALNAME      | VARCHAR |     | Y        | firm.legalName |
| FIRM_ADDRESS_NAME   | VARCHAR |     | Y        | firm.address.name |
"""


@pytest.fixture(scope="module")
def person_finder():
    tmp = tempfile.mkdtemp()
    try:
        with open(f"{tmp}/denorm_model.md", "w") as f:
            f.write(_MODEL)
        with open(f"{tmp}/denorm_mapping.md", "w") as f:
            f.write(_MAPPING)
        generate(load(f"{tmp}/denorm_mapping.md"), tmp)
        sys.path.insert(0, tmp)
        try:
            yield importlib.import_module("person_finder").PersonFinder()
        finally:
            sys.path.remove(tmp)
            for name in [n for n in sys.modules if n.endswith("_finder") or n.endswith("_finder_base")]:
                del sys.modules[name]
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _sql(finder, columns, where=None):
    return finder.find_all(None, None, columns, where) if where is not None else finder.find_all(None, None, columns)


def test_embedded_properties_are_columns_of_the_row(person_finder):
    f = person_finder
    sql = _sql(f, [f.first_name(), f.address().name(), f.firm().legal_name(), f.firm().address().name()]).to_sql()
    assert "JOIN" not in sql.upper()
    for column in ("PERSON_FIRSTNAME", "PERSON_ADDRESS_NAME", "FIRM_LEGALNAME", "FIRM_ADDRESS_NAME"):
        assert column in sql
    assert sql.upper().count("FROM") == 1


def test_embedded_property_can_be_filtered(person_finder):
    f = person_finder
    sql = _sql(f, [f.first_name()], f.firm().legal_name().eq("Firm A")).to_sql()
    assert "JOIN" not in sql.upper() and "FIRM_LEGALNAME" in sql and "Firm A" in sql


def test_nested_embedded_attribute_has_a_display_name(person_finder):
    assert person_finder.firm().address().name().display_name() == "Firm Address Name"
