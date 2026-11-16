# Legend Union For SQL Query Merging Model

Source: legend-engine core_relational `tests/mapping/union/testUnionForSQLQueryMerging.pure` (A, B, C and D, each with a key, and the associations AB, BC and CD).

## Sub-Domain: merging

### Class: A

| Name | Description |
|------|-------------|
| A    |             |

| Property | Id | Type    | Multiplicity | Key | Description |
|----------|----|---------|--------------|-----|-------------|
| Pk       | pk | Integer | 1            | Y   |             |

### Class: B

| Name | Description |
|------|-------------|
| B    |             |

| Property | Id | Type    | Multiplicity | Key | Description |
|----------|----|---------|--------------|-----|-------------|
| Pk       | pk | Integer | 1            | Y   |             |

### Class: C

| Name | Description |
|------|-------------|
| C    |             |

| Property | Id | Type    | Multiplicity | Key | Description |
|----------|----|---------|--------------|-----|-------------|
| Pk       | pk | Integer | 1            | Y   |             |

### Class: D

| Name | Description |
|------|-------------|
| D    |             |

| Property | Id | Type    | Multiplicity | Key | Description |
|----------|----|---------|--------------|-----|-------------|
| Pk       | pk | Integer | 1            | Y   |             |

### Association: AB

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| AB   | A      | B                    | b                  | 1                   | B      |                      |                    | *                   |             |

### Association: BC

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| BC   | B      | C                    | c                  | 1                   | C      |                      |                    | *                   |             |

### Association: CD

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| CD   | C      | D                    | d                  | 1                   | D      |                      |                    | *                   |             |
