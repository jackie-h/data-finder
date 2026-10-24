-- Test data from legend-engine core_relational businessDateMilestoningSetUp.pure::initDatabase
-- Tables: BiTemporalProductTable, BiTemporalProductClassificationTable
drop table if exists BiTemporalProductTable;
create table BiTemporalProductTable (id Integer, type VARCHAR(200), in_z DATE, out_z DATE, from_z DATE, thru_z DATE);
insert into BiTemporalProductTable values(1, 'STOCK', '2017-6-9', '9999-12-31', '2017-6-9', '2017-6-26');
drop table if exists BiTemporalProductClassificationTable;
create table BiTemporalProductClassificationTable (type VARCHAR(200),type_description VARCHAR(400),system VARCHAR(200), exchange VARCHAR(200), in_z DATE, out_z DATE, from_z DATE, thru_z DATE);
insert into BiTemporalProductClassificationTable values('STOCK','STOCK DESC-V1','SYS1', 'LNSE', '2017-6-11', '9999-12-31', '2017-6-10', '9999-12-31');
