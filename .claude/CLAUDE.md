# Claude Instructions

Follow the shared project instructions in [`../AGENTS.md`](../AGENTS.md).

Add Claude-specific guidance here only when it does not belong in the shared agent instructions.

## Tests

- Always write correctness tests as spec tests, not unit tests. A spec is a `TestExpectation` in `datafinder_examples_tests` (a finder query, the columns and rows it must give, and, where the SQL itself matters, `expected_sql` or `unexpected_sql` fragments), which every engine's suite runs through the shared example mappings and data. Add the spec to the file for the mapping it reads, next to the specs of the same kind (aggregates in `finance_group_by_specs`, date functions in `positions_specs` or `companies_constant_specs`).
- Compute the expected result independently of the code under test: from Python (`datetime`, `math`, `decimal`) or numpy (`np.quantile`), never by copying what the engine returned. A test that only checks the generated SQL text does not show that the query is right.
- Check the cases that break a function, not only the one that works: boundaries (the end of a year, a leap day, a negative direction), a missing value, and each variant of an option.
- Reuse and extend the existing models, mappings, data and specs as much as possible; do not make a new model for one feature. When a feature needs a column, a row or a mapping, add it to the existing one, update the existing specs that its data changes, and then write the new spec. Adding data is fine: a spec that compares against the whole table (`<`, `>`, counts) is updated for the new rows, not worked around. A new model or dataset is for behaviour that no existing one can hold. A `constant(value)` gives a one-row spec with no data of its own, and suits a function of a value, but it is not a reason to avoid extending the data.
- Use a unit test only for what no finder query can show: an error that is raised, the type an expression has, or the SQL written for an engine that has no live runner (Databricks). Say in its docstring where the behaviour itself is specced.
