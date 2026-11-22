# data-finder-specs

Language neutral specs for [data-finder](https://github.com/jackie-h/data-finder). This package holds data
files only, with no Python.

- `specs/models/`, `specs/mappings/`: the example models and mappings (Markdown); a mapping names its model by a relative path.
- `specs/data/`: the rows the specs run against (CSV).
- `specs/<suite>/`: a suite is the specs that run against one set of tables. `schema.sql` drops what an earlier run made and creates its schemas and tables
  (types INT, VARCHAR, DOUBLE, DATE, TIMESTAMP, BOOLEAN, and VECTOR(n), which an engine makes its own array of floats),
  and `suite.json` names, for each table as the mapping reads it, the CSV in `specs/data` that holds its rows.
- `specs/<suite>/<group>/`: the finder specs, each a JSON file with the query (a tree of calls of the finder API, and
  the same query as Python in `query_text`), the finder and mapping it reads, and the checks, and a CSV of the expected
  result: a row of column names, a row of types (`Integer`, `Double`, `String[0..1]`, `Date`, ...), then the rows. A
  missing value is an empty cell and the empty string is `""`. Where the SQL matters, the JSON names a `<name>.<backend>.sql`
  file with the full SQL that backend must write, and `sql_not_contains` fragments it must not contain. `skip` gives, per
  backend, why a spec does not apply, and `approximate` compares doubles to a relative 1e-9.

The version is the same as data-finder's.

To use it without Python, download the sdist of the version you want from PyPI and unpack it.
