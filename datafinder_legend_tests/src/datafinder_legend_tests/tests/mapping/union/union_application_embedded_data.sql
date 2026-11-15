-- Test data from legend-engine core_relational tests/mapping/union/testUnion.pure::createTablesAndFillDb (ApplicationsSet1 and ApplicationsSet2)
Drop table if exists ApplicationsSet1;
Create Table ApplicationsSet1 (date DATE, personID INT, firstName_s1 VARCHAR(200), lastName_s1 VARCHAR(200), nickName_s1 VARCHAR(200), person_address_s1 VARCHAR(200), firmID INT, firm_legalName_s1 VARCHAR(200), firm_nickName_s1 VARCHAR(200), firm_address_s1 VARCHAR(200));
insert into ApplicationsSet1 (date, personID, firstName_s1, lastName_s1, nickName_s1, firmID, firm_legalName_s1, firm_nickName_s1) values ('2016-01-01', 1, 'firstName_s1', 'lastName_s1', 'nickName_s1', 1, 'firm_legalName_s1', 'firm_nickName_s1');
Drop table if exists ApplicationsSet2;
Create Table ApplicationsSet2 (date DATE, firstName VARCHAR(200), lastName VARCHAR(200), nickName VARCHAR(200), person_address_s2 VARCHAR(200), firm_legalName VARCHAR(200), firm_nickName VARCHAR(200), firm_address_s2 VARCHAR(200));
insert into ApplicationsSet2 (date, firstName, lastName, nickName, firm_legalName, firm_nickName) values ('2016-01-01', 'firstName', 'lastName', 'nickName', 'firm_legalName', 'firm_nickName');
