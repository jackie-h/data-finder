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

- Always write correctness tests as spec tests, not unit tests. A spec is a `TestExpectation` in `datafinder_examples_tests` (a finder query, the columns and rows it must give, and, where the SQL itself matters, `expected_sql` or `unexpected_sql` fragments), which every engine's suite runs through the shared example mappings and data. Add the spec to the file for the mapping it reads, next to the specs of the same kind (aggregates in `finance_group_by_specs`, date functions in `positions_specs` or `companies_constant_specs`).
- Compute the expected result independently of the code under test: from Python (`datetime`, `math`, `decimal`) or numpy (`np.quantile`), never by copying what the engine returned. A test that only checks the generated SQL text does not show that the query is right.
- Check the cases that break a function, not only the one that works: boundaries (the end of a year, a leap day, a negative direction), a missing value, and each variant of an option.
- Reuse and extend the existing models, mappings, data and specs as much as possible; do not make a new model for one feature. When a feature needs a column, a row or a mapping, add it to the existing one, update the existing specs that its data changes, and then write the new spec. Adding data is fine: a spec that compares against the whole table (`<`, `>`, counts) is updated for the new rows, not worked around. A new model or dataset is for behaviour that no existing one can hold. A `constant(value)` gives a one-row spec with no data of its own, and suits a function of a value, but it is not a reason to avoid extending the data.
- Use a unit test only for what no finder query can show: an error that is raised, the type an expression has, or the SQL written for an engine that has no live runner (Databricks). Say in its docstring where the behaviour itself is specced.

## Git Workflow

- Create a branch for each feature or bug fix before changing files.
- Use a short, descriptive branch name.
