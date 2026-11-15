-- Test data from legend-engine core_relational tests/mapping/filter/testFilterMappingTree.pure::createTablesAndFillDb
Drop table if exists orgTable;
Create Table orgTable(id INT, filterVal INT, parentId INT, name VARCHAR(200));
insert into orgTable (id, filterVal, parentId, name) values (1, 4,-1, 'ROOT');
insert into orgTable (id, filterVal, parentId, name) values (2, 4, 1, 'Firm X');
insert into orgTable (id, filterVal, parentId, name) values (3, 4, 1, 'Firm A');
insert into orgTable (id, filterVal, parentId, name) values (4, 4, 2, 'Securities');
insert into orgTable (id, filterVal, parentId, name) values (5, 4, 2, 'Banking');
insert into orgTable (id, filterVal, parentId, name) values (6, 4, 2, 'Federation');
insert into orgTable (id, filterVal, parentId, name) values (7, 6, 2, 'ShouldNotBeDisplayed');
Drop table if exists otherTable;
Create Table otherTable(orgTableId INT, filterVal INT);
insert into otherTable (orgTableId, filterVal) values (1, 4);
insert into otherTable (orgTableId, filterVal) values (2, 4);
insert into otherTable (orgTableId, filterVal) values (3, 4);
insert into otherTable (orgTableId, filterVal) values (4, 4);
insert into otherTable (orgTableId, filterVal) values (5, 4);
insert into otherTable (orgTableId, filterVal) values (6, 4);
insert into otherTable (orgTableId, filterVal) values (7, 6);
