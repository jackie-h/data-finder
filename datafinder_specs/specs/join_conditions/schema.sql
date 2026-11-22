-- The schemas and tables that the specs of this suite read. It first drops what an earlier run made, so it can run on any database.
-- Then load the rows of each table from its CSV (suite.json names the CSV of each table; a table with no CSV has no rows).
-- Types are the ones every engine spells the same (INT, VARCHAR, DOUBLE, DATE, TIMESTAMP, BOOLEAN), and VECTOR(n), a vector of n
-- numbers that its CSV writes as [0.5,0.25]: an engine makes it its own array of floats.

DROP SCHEMA IF EXISTS org CASCADE;

CREATE SCHEMA IF NOT EXISTS org;
CREATE TABLE org.firms (id INT, legal_name VARCHAR, flag INT);
CREATE TABLE org.persons (id INT, first_name VARCHAR, flag INT, firm_id INT);
CREATE TABLE org.memberships (person_id INT, firm_id INT, active INT);
CREATE TABLE org.staff (id INT, name VARCHAR, manager_id INT, active INT);
