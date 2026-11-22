-- The schemas and tables that the specs of this suite read. It first drops what an earlier run made, so it can run on any database.
-- Then load the rows of each table from its CSV (suite.json names the CSV of each table; a table with no CSV has no rows).
-- Types are the ones every engine spells the same (INT, VARCHAR, DOUBLE, DATE, TIMESTAMP, BOOLEAN), and VECTOR(n), a vector of n
-- numbers that its CSV writes as [0.5,0.25]: an engine makes it its own array of floats.

DROP SCHEMA IF EXISTS geo CASCADE;

CREATE SCHEMA IF NOT EXISTS geo;
CREATE TABLE geo.people (id INT, name VARCHAR);
CREATE TABLE geo.regions (id INT, name VARCHAR);
CREATE TABLE geo.locations (id INT, place VARCHAR, visits INT, person_id INT, region_id INT);
