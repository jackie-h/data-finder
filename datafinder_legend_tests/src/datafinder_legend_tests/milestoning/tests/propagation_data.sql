-- Test data from legend-engine core_relational milestoning/tests/businessDateMilestoningSetUp.pure::initDatabase (ProductTable, NewActivityInfoTable, CancelActivitiesTable)
drop table if exists ProductTable;
create table ProductTable (id Integer, from_z DATE, thru_z DATE);
insert into ProductTable values(1, '2015-8-15', '2015-8-26');
insert into ProductTable values(2, '2015-8-26', '2015-10-16');
insert into ProductTable values(2, '2015-10-16', '9999-12-31');
insert into ProductTable values(3, '2015-8-26', '9999-12-31');
drop table if exists NewActivityInfoTable;
create table NewActivityInfoTable (productId Integer, created_by VARCHAR(200), from_z DATE, thru_z DATE);
insert into NewActivityInfoTable values (1, 'Scottd', '2015-1-15', '2015-8-15');
insert into NewActivityInfoTable values (2, 'smith', '2015-1-15', '2015-8-15');
drop table if exists CancelActivitiesTable;
create table CancelActivitiesTable (productId Integer, from_z DATE, thru_z DATE);
insert into CancelActivitiesTable values (2, '2015-10-15', '9999-12-31');
