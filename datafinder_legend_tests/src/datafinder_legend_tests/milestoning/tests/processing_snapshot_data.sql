-- Test data from legend-engine core_relational businessDateMilestoningSetUp.pure::initDatabase
-- Tables: OrderTable, ProductTableWithProcessingSnapshotMilestoning, ProductClassificationTableWithProcessingSnapshotMilestoning
drop table if exists OrderTable;
create table OrderTable (orderDate TIMESTAMP, id Integer, prodFk Integer);
insert into OrderTable values('2015-8-26 00:00:00', 1, 1);
insert into OrderTable values('2015-8-26 00:00:00', 2, 2);
drop table if exists ProductTableWithProcessingSnapshotMilestoning;
create table ProductTableWithProcessingSnapshotMilestoning (id Integer, name VARCHAR(200), type VARCHAR(200), snapshotDate DATE);
insert into ProductTableWithProcessingSnapshotMilestoning values(1, 'ProductName', 'STOCK', '2015-8-15');
insert into ProductTableWithProcessingSnapshotMilestoning values(2, 'ProductName1', 'STOCK', '2015-8-26');
insert into ProductTableWithProcessingSnapshotMilestoning values(2, 'ProductName2', 'STOCK', '2015-10-16');
insert into ProductTableWithProcessingSnapshotMilestoning values(3, 'ProductName3', 'OPTION', '2015-8-26');
drop table if exists ProductClassificationTableWithProcessingSnapshotMilestoning;
create table ProductClassificationTableWithProcessingSnapshotMilestoning (type VARCHAR(200), type_description VARCHAR(400), snapshotDate DATE);
insert into ProductClassificationTableWithProcessingSnapshotMilestoning values('STOCK', 'STOCK DESC-V1', '2015-8-15');
insert into ProductClassificationTableWithProcessingSnapshotMilestoning values('STOCK', 'STOCK DESC-V2', '2015-8-26');
insert into ProductClassificationTableWithProcessingSnapshotMilestoning values('STOCK', 'STOCK DESC-V3', '2015-9-15');
insert into ProductClassificationTableWithProcessingSnapshotMilestoning values('STOCK', 'STOCK DESC-V4', '2015-10-17');
insert into ProductClassificationTableWithProcessingSnapshotMilestoning values('OPTION', 'OPTION DESC-V1', '2015-1-15');
