# Diamond Inheritance Model

## Sub-Domain: diamond

### Class: Auditable

| Name      | Description                    |
|-----------|--------------------------------|
| Auditable | Base class with audit columns  |

| Property   | Id         | Type    | Multiplicity | Key | Description |
|------------|------------|---------|--------------|-----|-------------|
| Id         | id         | Integer | 1            | Y   |             |
| Created At | createdAt  | String  | 1            |     |             |

### Class: Trackable extends Auditable

| Name      | Description                   |
|-----------|-------------------------------|
| Trackable | Tracks last-updated timestamp |

| Property   | Id         | Type   | Multiplicity | Key | Description |
|------------|------------|--------|--------------|-----|-------------|
| Updated At | updatedAt  | String | 1            |     |             |

### Class: Versioned extends Auditable

| Name      | Description              |
|-----------|--------------------------|
| Versioned | Carries a version number |

| Property | Id      | Type    | Multiplicity | Key | Description |
|----------|---------|---------|--------------|-----|-------------|
| Version  | version | Integer | 1            |     |             |

### Class: Record extends Trackable, Versioned

| Name   | Description                                      |
|--------|--------------------------------------------------|
| Record | Diamond: inherits Auditable via both Trackable and Versioned |

| Property    | Id          | Type   | Multiplicity | Key | Description |
|-------------|-------------|--------|--------------|-----|-------------|
| Record Name | recordName  | String | 1            |     |             |
