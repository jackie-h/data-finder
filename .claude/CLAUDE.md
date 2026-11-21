# Claude Instructions

Follow the shared project instructions in [`../AGENTS.md`](../AGENTS.md).

Add Claude-specific guidance here only when it does not belong in the shared agent instructions.

## Tests

- Always write correctness tests as spec tests, not unit tests. A spec is a `TestExpectation` in `datafinder_examples_tests` (a finder query, the columns and rows it must give, and, where the SQL itself matters, `expected_sql` or `unexpected_sql` fragments), which every engine's suite runs through the shared example mappings and data. Add the spec to the file for the mapping it reads, next to the specs of the same kind (aggregates in `finance_group_by_specs`, date functions in `positions_specs` or `companies_constant_specs`).
- Compute the expected result independently of the code under test: from Python (`datetime`, `math`, `decimal`) or numpy (`np.quantile`), never by copying what the engine returned. A test that only checks the generated SQL text does not show that the query is right.
- Check the cases that break a function, not only the one that works: boundaries (the end of a year, a leap day, a negative direction), a missing value, and each variant of an option.
- A constant (`constant(value)`) gives a one-row spec with no data of its own. Do not add rows to a shared CSV for one spec: other specs compare against the whole table (`<`, `>`, counts), and they break.
- Use a unit test only for what no finder query can show: an error that is raised, the type an expression has, or the SQL written for an engine that has no live runner (Databricks). Say in its docstring where the behaviour itself is specced.
