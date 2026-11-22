-- The schemas and tables that the specs of this suite read. It first drops what an earlier run made, so it can run on any database.
-- Then load the rows of each table from its CSV (suite.json names the CSV of each table; a table with no CSV has no rows).
-- Types are the ones every engine spells the same (INT, VARCHAR, DOUBLE, DATE, TIMESTAMP, BOOLEAN), and VECTOR(n), a vector of n
-- numbers that its CSV writes as [0.5,0.25]: an engine makes it its own array of floats.

DROP SCHEMA IF EXISTS records CASCADE;

CREATE SCHEMA IF NOT EXISTS records;
CREATE TABLE records.items ( item_id INT, created_at VARCHAR, updated_at VARCHAR, version INT, record_name VARCHAR );
