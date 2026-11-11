-- Test data from legend-engine core_relational advancedRelationalSetUp.pure::createTablesAndFillDb
-- Tables: PersonTable, FirmTable, firmPersonBridgeTable
Drop table if exists PersonTable;
Create Table PersonTable(id INT, firstName VARCHAR(200), lastName VARCHAR(200), addressId INT, firmId INT, flag INT,AGE INT);
insert into PersonTable (id, firstName, lastName, addressId, firmId, flag,age) values (1, 'Peter', 'Smith',1,1,-1,30);
insert into PersonTable (id, firstName, lastName, addressId, firmId, flag,age) values (2, 'John', 'Johnson',2,1,-1,35);
insert into PersonTable (id, firstName, lastName, addressId, firmId, flag,age) values (3, 'John', 'Hill',3,1,-1,34);
insert into PersonTable (id, firstName, lastName, addressId, firmId, flag,age) values (4, 'Anthony', 'Allen',4,1,-1,34);
insert into PersonTable (id, firstName, lastName, addressId, firmId, flag,age) values (5, 'Fabrice', 'Roberts',5,2,-1,23);
insert into PersonTable (id, firstName, lastName, addressId, firmId, flag,age) values (6, 'Oliver', 'Hill',6,3,-1,22);
insert into PersonTable (id, firstName, lastName, addressId, firmId, flag,age) values (7, 'David', 'Harris',7,4,-1,36);
Drop table if exists FirmTable;
Create Table FirmTable(id INT, legalName VARCHAR(200), addressId INT, flag INT);
insert into FirmTable (id, legalName, addressId, flag) values (1, 'Firm X', 8, null);
insert into FirmTable (id, legalName, addressId, flag) values (2, 'Firm A', 9, null);
insert into FirmTable (id, legalName, addressId, flag) values (3, 'Firm B', 10, null);
insert into FirmTable (id, legalName, addressId, flag) values (4, 'Firm C', 11, null);
Drop table if exists firmPersonBridgeTable;
Create Table firmPersonBridgeTable(FIRM_ID INT, PERSON_ID INT);
insert into firmPersonBridgeTable (FIRM_ID, PERSON_ID) values (1, 1);
insert into firmPersonBridgeTable (FIRM_ID, PERSON_ID) values (1, 2);
insert into firmPersonBridgeTable (FIRM_ID, PERSON_ID) values (1, 3);
insert into firmPersonBridgeTable (FIRM_ID, PERSON_ID) values (1, 4);
insert into firmPersonBridgeTable (FIRM_ID, PERSON_ID) values (2, 5);
insert into firmPersonBridgeTable (FIRM_ID, PERSON_ID) values (3, 6);
