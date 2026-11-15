# Legend Filter Mapping Tree Model

Source: legend-engine core_relational `tests/mapping/filter/testFilterMappingTree.pure` (Org, with its parent and children).

## Sub-Domain: org

### Class: Org

| Name | Description |
|------|-------------|
| Org  |             |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Name     | name | String  | 1            |     |             |

### Association: OrgOrg

| Name   | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|--------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| OrgOrg | Org    | Parent               | parent             | 0..1                | Org    | Children             | children           | *                   |             |
