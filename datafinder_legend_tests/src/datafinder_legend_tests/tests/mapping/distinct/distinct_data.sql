-- Test data from legend-engine core_relational tests/mapping/distinct/testDistinct.pure::createTablesAndFillDb
Drop table if exists ACCOUNT_INFO;
Create Table ACCOUNT_INFO(id INT PRIMARY KEY, ACC_NUM INT, ACCT_NAME VARCHAR(20), IF_CODE INT, IF_NAME VARCHAR(200), IF_TYPE VARCHAR(20), DESKHEAD_ID INT, DESKHEAD_NAME VARCHAR(20));
insert into ACCOUNT_INFO (id, ACC_NUM, ACCT_NAME, IF_CODE, IF_NAME, IF_TYPE, DESKHEAD_ID, DESKHEAD_NAME) values (1, 7900001, 'Acct 1', 1001, 'IF 1', 'Type 1', 11, 'Jack');
insert into ACCOUNT_INFO (id, ACC_NUM, ACCT_NAME, IF_CODE, IF_NAME, IF_TYPE, DESKHEAD_ID, DESKHEAD_NAME) values (2, 7900002, 'Acct 2',1002, 'IF 2', 'Type 1', 11, 'Jack');
insert into ACCOUNT_INFO (id, ACC_NUM, ACCT_NAME, IF_CODE, IF_NAME, IF_TYPE, DESKHEAD_ID, DESKHEAD_NAME) values (3, 7900003, 'Acct 3',1002, 'IF 2', 'Type 1', 11, 'Jack');
insert into ACCOUNT_INFO (id, ACC_NUM, ACCT_NAME, IF_CODE, IF_NAME, IF_TYPE, DESKHEAD_ID, DESKHEAD_NAME) values (4, 7900004, 'Acct 4',1022, 'IF 2', 'Type 1', 11, 'Jack');
insert into ACCOUNT_INFO (id, ACC_NUM, ACCT_NAME, IF_CODE, IF_NAME, IF_TYPE, DESKHEAD_ID, DESKHEAD_NAME) values (5, 7900005, 'Acct 5',1002, 'IF 2', 'Type 2', 11, 'Jack');
