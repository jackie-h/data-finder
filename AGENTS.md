# Agent Instructions

## Environment

- This project uses `uv` and a `.venv`. Run Python scripts with `uv run` rather than invoking `python` directly.
- Run the full test suite with `uv run --extra test pytest`. The root project's test dependencies are in the `test` optional dependency group.
- Run `uv run pyright` and the full test suite before every push. CI runs both, and a pyright error fails the build even when the tests pass. Fix the type; do not add an ignore comment to silence it.
- Add packages with `uv add`, not `pip install`.

## Dependencies

- Keep library dependencies to a minimum.
- Add a dependency to the sub-project that needs it, using the appropriate optional dependency group.
- Add a dependency to the root project only when all sub-projects need it.

## Models and Tests

- Prefer reusable finder specs in `datafinder_examples_tests` over backend-specific unit tests when behavior can be expressed as a finder query and expected result.
- Put shared example models, mappings, and fixture data in `datafinder_specs/specs/{models,mappings,data}` (a data-only project, published as `data-finder-specs` and versioned with data-finder; `datafinder_examples.example_path` reads it); have backend test suites run the shared specs against their engine.
- Update the shared model and example/test data when a feature needs new schema or representative rows. Use individual unit tests for behavior that cannot be covered by a finder spec.

### Writing correctness tests

- Always write correctness tests as spec tests, not unit tests. A spec is a `TestExpectation(name, query)` in `datafinder_examples_tests` (a finder query) with its expected columns and rows in a CSV, and, where the SQL itself matters, the full SQL in a `.sql` file (or `sql_not_contains` fragments in its JSON); every engine's suite runs it through the shared example mappings and data. Add the spec to the file for the mapping it reads, next to the specs of the same kind (aggregates in `finance_group_by_specs`, date functions in `positions_specs` or `companies_constant_specs`).
- Compute the expected result independently of the code under test: from Python (`datetime`, `math`, `decimal`) or numpy (`np.quantile`), in a scratch script or by hand, and write it in the CSV; never copy what the engine returned. A test that only checks the generated SQL text does not show that the query is right.
- Check the cases that break a function, not only the one that works: boundaries (the end of a year, a leap day, a negative direction), a missing value, and each variant of an option.
- Reuse and extend the existing models, mappings, data and specs as much as possible; do not make a new model for one feature. When a feature needs a column, a row or a mapping, add it to the existing one, update the existing specs that its data changes, and then write the new spec. Adding data is fine: a spec that compares against the whole table (`<`, `>`, counts) is updated for the new rows, not worked around. A new model or dataset is for behaviour that no existing one can hold. A `constant(value)` gives a one-row spec with no data of its own, and suits a function of a value, but it is not a reason to avoid extending the data.
- Use a unit test only for what no finder query can show: an error that is raised, the type an expression has, or the SQL written for an engine that has no live runner (Databricks). Say in its docstring where the behaviour itself is specced.

### Spec files

- A spec is written as files in `datafinder_specs/specs/<suite>/<group>/`, which are the master of what it expects, and as a `TestExpectation(name, query)` in `datafinder_examples_tests`, which holds only the query. The files are `<name>.json` (the query recorded from the Python, the finder and mapping, the file of the result, the SQL files, `skip` reasons per backend, `approximate`, `sql_not_contains`), `<name>.csv` (the expected result, with the type of each column in the names of the model: `Integer`, `Double`, `String[0..1]`, `Date`, ...; a missing value is an empty cell, the empty string is `""`) and `<name>.<backend>.sql` (the full SQL that backend must write). A suite is the specs that run against one set of tables (`schema.sql` drops what an earlier run made and creates its schemas and tables, written by hand, and `suite.json` names the CSV of the rows of each table; an engine's tests load a suite with `suite_data`: `load_duckdb`, or `tables` for Spark (`datafinder_spark/tests/test_spark_specs.py` runs every suite on Spark; a spec Spark cannot run has a `"spark"` reason in the `skip` of its JSON), and write no DDL or DROP of their own); the suites and their groups are listed in `SUITES` in `spec_files.py`, and a new spec module goes there.
- To add a spec: write the `TestExpectation(name, query)`, run `uv run datafinder_examples/tools/write_spec_files.py` (it writes the JSON, and keeps what else the JSON has), write the CSV by hand, from the values you worked out, and add `"sql": {"duckdb": "<name>.duckdb.sql"}` to the JSON for a spec whose SQL matters. Run the tests with `DATAFINDER_UPDATE_SQL=1` to write the `.sql` files from the engine, and read them: they are a record of the SQL, so a result is checked by the CSV and never by the SQL. A test fails when a query no longer matches its JSON, or a spec has no file or a file no spec.
- A query is recorded by running its lambda against a stand-in for the finder (`query_json.py`), so it must be built from calls of the finder API and not branch on a value of the query.

## Git Workflow

- Create a branch for each feature or bug fix before changing files.
- Use a short, descriptive branch name.
