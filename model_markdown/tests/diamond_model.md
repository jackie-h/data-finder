# Diamond Inheritance Model

## Sub-Domain: diamond

### Class: Auditable

| Name      | Description                    |
|-----------|--------------------------------|
| Auditable | Base class with audit columns  |

| Property   | Id         | Type    | Key | Description |
|------------|------------|---------|-----|-------------|
| Id         | id         | Integer | Y   |             |
| Created At | created_at | String  |     |             |

### Class: Trackable extends Auditable

| Name      | Description                   |
|-----------|-------------------------------|
| Trackable | Tracks last-updated timestamp |

| Property   | Id         | Type   | Key | Description |
|------------|------------|--------|-----|-------------|
| Updated At | updated_at | String |     |             |

### Class: Versioned extends Auditable

| Name      | Description              |
|-----------|--------------------------|
| Versioned | Carries a version number |

| Property | Id      | Type    | Key | Description |
|----------|---------|---------|-----|-------------|
| Version  | version | Integer |     |             |

### Class: Record extends Trackable, Versioned

| Name   | Description                                      |
|--------|--------------------------------------------------|
| Record | Diamond: inherits Auditable via both Trackable and Versioned |

| Property    | Id          | Type   | Key | Description |
|-------------|-------------|--------|-----|-------------|
| Record Name | record_name | String |     |             |
