# Legend Union With Extra Information Model

Source: legend-engine core_relational `tests/testModel/simpleTestModel.pure` (Person with its extraInformation, and Firm with its employees),
as `tests/mapping/union/testUnion.pure` uses them.

## Sub-Domain: extra

### Class: Person

| Name   | Description |
|--------|-------------|
| Person |             |

| Property         | Id               | Type    | Multiplicity | Key | Description |
|------------------|------------------|---------|--------------|-----|-------------|
| LastName         | lastName         | String  | 1            |     |             |
| ExtraInformation | extraInformation | String  | 0..1         |     |             |
| OtherNames       | otherNames       | String  | *            |     |             |

### Class: Firm

| Name | Description |
|------|-------------|
| Firm |             |

| Property  | Id        | Type   | Multiplicity | Key | Description |
|-----------|-----------|--------|--------------|-----|-------------|
| LegalName | legalName | String | 1            |     |             |

### Association: PersonFirm

| Name       | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonFirm | Person | Firm                 | firm               | 0..1                | Firm   | Employees            | employees          | *                   |             |
