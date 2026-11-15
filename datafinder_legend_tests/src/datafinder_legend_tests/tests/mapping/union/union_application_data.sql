-- Test data from legend-engine core_relational tests/mapping/union/testUnion.pure::createTablesAndFillDb (ApplicationsSet1, with its person and firm)
Drop table if exists ApplicationsSet1;
Create Table ApplicationsSet1 (date DATE, personID INT, firstName_s1 VARCHAR(200), lastName_s1 VARCHAR(200), nickName_s1 VARCHAR(200), firmID INT, firm_legalName_s1 VARCHAR(200), firm_nickName_s1 VARCHAR(200));
insert into ApplicationsSet1 (date, personID, firstName_s1, lastName_s1, nickName_s1, firmID, firm_legalName_s1, firm_nickName_s1) values ('2016-01-01', 1, 'firstName_s1', 'lastName_s1', 'nickName_s1', 1, 'firm_legalName_s1', 'firm_nickName_s1');
Drop table if exists PersonSet1;
Create Table PersonSet1 (ID INT, NICKNAME VARCHAR(200));
insert into PersonSet1 (ID, NICKNAME) values (1, 'Scotty');
Drop table if exists FirmSet1;
Create Table FirmSet1 (ID INT, NICKNAME VARCHAR(200));
insert into FirmSet1 (ID, NICKNAME) values (1, 'FX');
