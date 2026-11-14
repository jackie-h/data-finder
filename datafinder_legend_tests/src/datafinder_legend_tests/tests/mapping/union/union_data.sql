-- Test data from legend-engine core_relational tests/mapping/union/testUnion.pure::createTablesAndFillDb (PersonSet1 and PersonSet2)
Drop table if exists PersonSet1;
Create Table PersonSet1 (ID INT, firstName_s1 VARCHAR(200),lastName_s1 VARCHAR(200), FirmID INT, ADDRESSID INT, NICKNAME VARCHAR(200));
insert into PersonSet1 (ID, firstName_s1, lastName_s1, FirmID, ADDRESSID) values (1, '', 'Scott', 1, 1);
insert into PersonSet1 (ID, firstName_s1, lastName_s1, FirmID, ADDRESSID) values (2, '', 'Anand', 1, 1);
insert into PersonSet1 (ID, firstName_s1, lastName_s1, FirmID, ADDRESSID) values (3, '', 'Roberts', 2, 2);
Drop table if exists PersonSet2;
Create Table PersonSet2 (ID INT, firstName_s2 VARCHAR(200),lastName_s2 VARCHAR(200), FirmID INT, ADDRESSID INT);
insert into PersonSet2 (ID, firstName_s2, lastName_s2, FirmID, ADDRESSID) values (1, '', 'Taylor', 1, 1);
insert into PersonSet2 (ID, firstName_s2, lastName_s2, FirmID, ADDRESSID) values (2, '', 'Wright', 1, 1);
