-- The schemas and tables that the specs of this suite read. Run it on an empty database, then load the rows of each table from
-- its CSV (suite.json names the CSV of each table; a table with no CSV has no rows).
-- Types are the ones every engine spells the same (INT, VARCHAR, DOUBLE, DATE, TIMESTAMP, BOOLEAN), and VECTOR(n), a vector of n
-- numbers that its CSV writes as [0.5,0.25]: an engine makes it its own array of floats.

CREATE SCHEMA IF NOT EXISTS org;
CREATE TABLE org.people (person_id INT, first_name VARCHAR, age INT, firm_id INT, firm_name VARCHAR);
CREATE TABLE org.firms (id INT, name VARCHAR);
CREATE TABLE org.trades (acc_num INT, gsn VARCHAR, qty INT);
CREATE TABLE org.rates (code VARCHAR, amount DOUBLE, from_z DATE, thru_z DATE);
CREATE TABLE org.quotes (code VARCHAR, amount DOUBLE, in_z TIMESTAMP, out_z TIMESTAMP, from_z DATE, thru_z DATE);
CREATE TABLE org.holders (id INT, rate_code VARCHAR);
CREATE TABLE org.units (unit_id INT, name VARCHAR, parent_id INT, active INT);
