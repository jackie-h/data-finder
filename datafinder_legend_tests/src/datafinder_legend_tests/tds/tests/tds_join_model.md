# Legend TDS Join Model

Source: legend-engine core_relational `tds/tests/testTDSJoin.pure` (testJoinTDS_Person, testJoinTDS_Firm and testJoinTDS_Address).

## Sub-Domain: tdsJoin

### Class: JoinPerson

| Name       | Description |
|------------|-------------|
| JoinPerson |             |

| Property   | Id         | Type    | Multiplicity | Key | Description |
|------------|------------|---------|--------------|-----|-------------|
| PersonID   | personID   | Integer | 1            | Y   |             |
| FirstName  | firstName  | String  | 1            |     |             |
| LastName   | lastName   | String  | 1            |     |             |
| Age        | age        | Integer | 0..1         |     |             |
| EmployerID | employerID | Integer | 0..1         |     |             |
| ManagerID  | managerID  | Integer | 0..1         |     |             |

### Class: JoinFirm

| Name     | Description |
|----------|-------------|
| JoinFirm |             |

| Property    | Id          | Type    | Multiplicity | Key | Description |
|-------------|-------------|---------|--------------|-----|-------------|
| LegalName   | legalName   | String  | 1            |     |             |
| FirmID      | firmID      | Integer | 1            | Y   |             |
| FirmAddrID  | firmAddrID  | Integer | 1            |     |             |
