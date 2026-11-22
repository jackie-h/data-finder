# data-finder-specs

Language neutral specs for [data-finder](https://github.com/jackie-h/data-finder). This package holds data
files only, with no Python.

- `specs/models/`, `specs/mappings/`: the example models and mappings (Markdown); a mapping names its model by a relative path.
- `specs/data/`: the rows the specs run against (CSV).
- `specs/`: the finder specs, each a JSON file with the query, a CSV of the expected result and, where the SQL
  matters, the expected SQL.

The version is the same as data-finder's.

To use it without Python, download the sdist of the version you want from PyPI and unpack it.
