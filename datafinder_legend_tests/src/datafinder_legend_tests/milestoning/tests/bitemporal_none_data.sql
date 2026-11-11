-- Test data from legend-engine core_relational businessDateMilestoningSetUp.pure::initDatabase
-- Tables: OrderTable, ProductTableNoMilestoning, ProductClassificationTableNoMilestoning
drop table if exists OrderTable;
create table OrderTable (orderDate DATE, id Integer, prodFk Integer);
insert into OrderTable values('2015-8-26', 1,1);
insert into OrderTable values('2015-8-26', 2,2);
drop table if exists ProductTableNoMilestoning;
create table ProductTableNoMilestoning (id Integer, name VARCHAR(200),type VARCHAR(200),exchange VARCHAR(200), referenceSystemName VARCHAR(200),  classificationSystemId Integer);
insert into ProductTableNoMilestoning values(1,'ProductName','STOCK','ISE', 'SYS1', 1);
insert into ProductTableNoMilestoning values(2,'ProductName3','CDS','ISE', 'SYS1', 1);
drop table if exists ProductClassificationTableNoMilestoning;
create table ProductClassificationTableNoMilestoning (type VARCHAR(200),type_description VARCHAR(400),system VARCHAR(200), exchange VARCHAR(200));
insert into ProductClassificationTableNoMilestoning values('STOCK','STOCK DESC-V1','SYS1', 'LNSE');
insert into ProductClassificationTableNoMilestoning values('STOCK','STOCK DESC-V2','SYS1', 'LNSE');
insert into ProductClassificationTableNoMilestoning values('STOCK','STOCK DESC-V3','SYS1', 'LNSE');
insert into ProductClassificationTableNoMilestoning values('STOCK','STOCK DESC-V4','SYS1', 'LNSE');
insert into ProductClassificationTableNoMilestoning values('OPTION','OPTION DESC-V1','SYS1', 'ISE');
