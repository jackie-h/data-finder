-- The schemas and tables that the specs of this suite read. Run it on an empty database, then load the rows of each table from
-- its CSV (suite.json names the CSV of each table; a table with no CSV has no rows).
-- Types are the ones every engine spells the same (INT, VARCHAR, DOUBLE, DATE, TIMESTAMP, BOOLEAN), and VECTOR(n), a vector of n
-- numbers that its CSV writes as [0.5,0.25]: an engine makes it its own array of floats.

CREATE SCHEMA IF NOT EXISTS trading;
CREATE SCHEMA IF NOT EXISTS ref_data;
CREATE TABLE ref_data.branch_master (ID INT, CITY VARCHAR);
CREATE TABLE ref_data.account_master (ID INT, ACCT_NAME VARCHAR, BRANCH_ID INT);
CREATE TABLE trading.trades (sym VARCHAR, price DOUBLE, is_settled BOOLEAN, account_id INT, in_z TIMESTAMP, out_z TIMESTAMP, acct_name VARCHAR, branch_city VARCHAR);
