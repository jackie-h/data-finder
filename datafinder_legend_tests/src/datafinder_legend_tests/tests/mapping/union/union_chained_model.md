# Legend Union With Chained Joins Model

Source: legend-engine core_relational `tests/mapping/union/testUnionWithMultipleChainedJoins.pure` (X, Y and Z, each with a pk, and the
associations XY and YZ). The classes are business temporal in Legend; here their queries carry the business date range.

## Sub-Domain: chained

### Class: X

| Name | Description |
|------|-------------|
| X    |             |

| Property | Id | Type    | Multiplicity | Key | Description |
|----------|----|---------|--------------|-----|-------------|
| Pk       | pk | Integer | 1            | Y   |             |

### Class: Y

| Name | Description |
|------|-------------|
| Y    |             |

| Property | Id | Type    | Multiplicity | Key | Description |
|----------|----|---------|--------------|-----|-------------|
| Pk       | pk | Integer | 1            | Y   |             |

### Class: Z

| Name | Description |
|------|-------------|
| Z    |             |

| Property | Id | Type    | Multiplicity | Key | Description |
|----------|----|---------|--------------|-----|-------------|
| Pk       | pk | Integer | 1            | Y   |             |

### Association: XY

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| XY   | X      | Y                    | y                  | *                   | Y      | X                    | x                  | *                   |             |

### Association: YZ

| Name | Source | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|------|--------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| YZ   | Y      | Z                    | z                  | *                   | Z      | Y                    | y                  | *                   |             |
