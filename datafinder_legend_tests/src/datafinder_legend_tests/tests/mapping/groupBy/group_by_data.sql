-- Test data from legend-engine core_relational tests/mapping/groupBy/testGroupBy.pure::createTablesAndFillDb
Drop table if exists TRADE;
Create Table TRADE (TRADE_ID INT PRIMARY KEY, ACC_NUM INT, GSN VARCHAR(20), PRODUCT_ID INT, QTY INT);
insert into TRADE (TRADE_ID, ACC_NUM, GSN, PRODUCT_ID, QTY) values (1, 7900002, 'YU2EF5', 1, 3);
insert into TRADE (TRADE_ID, ACC_NUM, GSN, PRODUCT_ID, QTY) values (2, 7900002, 'YU2EF5', 1, 5);
insert into TRADE (TRADE_ID, ACC_NUM, GSN, PRODUCT_ID, QTY) values (3, 7900003, 'EA4GNY', 2, 100);
insert into TRADE (TRADE_ID, ACC_NUM, GSN, PRODUCT_ID, QTY) values (4, 7900003, 'EA4GNY', 2, 200);
Drop table if exists PRODUCT;
Create Table PRODUCT (ID INT PRIMARY KEY, DESCRIPTION VARCHAR(20));
insert into PRODUCT (ID, DESCRIPTION) values (1, 'Firm D');
insert into PRODUCT (ID, DESCRIPTION) values (2, 'Gold');
Drop table if exists PRODUCT_EXPLODED;
Create Table PRODUCT_EXPLODED (TRADEABLE_ID INT PRIMARY KEY, PRODUCT_ID INT, DESCRIPTION VARCHAR(20), TRADEABLE VARCHAR(20), ACTIVE INT);
insert into PRODUCT_EXPLODED (TRADEABLE_ID, PRODUCT_ID, DESCRIPTION, ACTIVE) values (11, 1, 'Firm D', 1);
insert into PRODUCT_EXPLODED (TRADEABLE_ID, PRODUCT_ID, DESCRIPTION, ACTIVE) values (22, 2, 'Gold', 0);
insert into PRODUCT_EXPLODED (TRADEABLE_ID, PRODUCT_ID, DESCRIPTION, ACTIVE) values (33, 2, 'Gold', 0);
