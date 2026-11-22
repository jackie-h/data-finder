# Places Model

A person has locations, and a location is in a region.

## Sub-Domain: travel.places

### Class: Person

| Name   | Description |
|--------|-------------|
| Person | A person    |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Name     | name | String  | 1            |     |             |

### Class: Location

| Name     | Description                      |
|----------|----------------------------------|
| Location | A place a person has been, and how often |

| Property | Id     | Type    | Multiplicity | Key | Description |
|----------|--------|---------|--------------|-----|-------------|
| Id       | id     | Integer | 1            | Y   |             |
| Place    | place  | String  | 1            |     |             |
| Visits   | visits | Integer | 1            |     |             |

### Class: Region

| Name   | Description |
|--------|-------------|
| Region | A region    |

| Property | Id   | Type    | Multiplicity | Key | Description |
|----------|------|---------|--------------|-----|-------------|
| Id       | id   | Integer | 1            | Y   |             |
| Name     | name | String  | 1            |     |             |

### Association: PersonLocation

| Name           | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|----------------|----------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| PersonLocation | Location | Person               | person             | 0..1                | Person | Locations            | locations          | *                   |             |

### Association: LocationRegion

| Name           | Source   | Source Property Name | Source Property ID | Source Multiplicity | Target | Target Property Name | Target Property ID | Target Multiplicity | Description |
|----------------|----------|----------------------|--------------------|---------------------|--------|----------------------|--------------------|---------------------|-------------|
| LocationRegion | Location | Region               | region             | 0..1                | Region | Locations            | locations          | *                   |             |
