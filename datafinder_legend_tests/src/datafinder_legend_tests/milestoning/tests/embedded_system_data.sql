-- Test data from legend-engine core_relational businessDateMilestoningSetUp.pure::initDatabase
-- Tables: ProductTable, ProductClassificationSystemTable, SystemTable
drop table if exists ProductTable;
create table ProductTable (id Integer, name VARCHAR(200),type VARCHAR(200),exchange VARCHAR(200), referenceSystemName VARCHAR(200),  classificationSystemId Integer, from_z DATE, thru_z DATE);
insert into ProductTable values(1,'ProductName','STOCK','ISE', 'SYS1', 1, '2015-8-15', '2015-8-26');
insert into ProductTable values(2,'ProductName1','STOCK','LNSE', 'SYS1', 1, '2015-8-26', '2015-10-16');
insert into ProductTable values(2,'ProductName2','STOCK','LNSE', 'SYS1', 1, '2015-10-16', '9999-12-31');
insert into ProductTable values(3,'ProductName3','OPTION','ISE', 'SYS1', 1, '2015-8-26', '9999-12-31');
drop table if exists ProductClassificationSystemTable;
create table ProductClassificationSystemTable (id Integer, name VARCHAR(200), from_z DATE, thru_z DATE);
insert into ProductClassificationSystemTable values(1,'SYS1','2015-10-15','9999-12-31');
drop table if exists SystemTable;
create table SystemTable (name VARCHAR(200));
insert into SystemTable values('SYS1');
