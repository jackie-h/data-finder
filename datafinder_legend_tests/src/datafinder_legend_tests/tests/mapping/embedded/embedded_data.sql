-- Test data from legend-engine core_relational testInlineEmbeddedMapping.pure::createTablesAndFillDb
Drop table if exists PRODUCT_DENORM;
Create Table PRODUCT_DENORM(PRODUCT_ID INT PRIMARY KEY, PRODUCT_NAME VARCHAR(200), MARKET_NAME VARCHAR(200), BOND_DETAILS VARCHAR(200));
insert into PRODUCT_DENORM (PRODUCT_ID, PRODUCT_NAME, MARKET_NAME, BOND_DETAILS) values (1, 'Product 1', 'LSE', 'Bond 1');
insert into PRODUCT_DENORM (PRODUCT_ID, PRODUCT_NAME, MARKET_NAME, BOND_DETAILS) values (2, 'Product 2', 'LSE', 'Bond 2');
insert into PRODUCT_DENORM (PRODUCT_ID, PRODUCT_NAME, MARKET_NAME, BOND_DETAILS) values (5, 'Product 3', 'MILAN', 'SuperBond 3 super');
Drop table if exists BOND_DETAIL;
Create Table BOND_DETAIL(BOND_ID INT PRIMARY KEY, NOT_SO_GOOD_DETAIL VARCHAR(200), TYPE VARCHAR(10), HOLDERID INT);
insert into BOND_DETAIL (BOND_ID, NOT_SO_GOOD_DETAIL, TYPE, HOLDERID) values (1, 'P 1', '15 years', 3);
insert into BOND_DETAIL (BOND_ID, NOT_SO_GOOD_DETAIL, TYPE, HOLDERID) values (2, 'P 2', '15 years', 4);
insert into BOND_DETAIL (BOND_ID, NOT_SO_GOOD_DETAIL, TYPE, HOLDERID) values (5, 'P 3', '5 years', 5);
Drop table if exists Party;
Create Table Party(id INT PRIMARY KEY, name VARCHAR(200));
insert into Party (id, name) values (3, 'Party 1');
insert into Party (id, name) values (4, 'Party 2');
insert into Party (id, name) values (5, 'Party 3');
