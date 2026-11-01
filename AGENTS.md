# Agent Instructions

## Environment

- This project uses `uv` and a `.venv`. Run Python scripts with `uv run` rather than invoking `python` directly.
- Run the full test suite with `uv run --extra test pytest`. The root project's test dependencies are in the `test` optional dependency group.
- Add packages with `uv add`, not `pip install`.

## Dependencies

- Keep library dependencies to a minimum.
- Add a dependency to the sub-project that needs it, using the appropriate optional dependency group.
- Add a dependency to the root project only when all sub-projects need it.

## Models and Tests

- Prefer reusable finder specs in `datafinder_examples_tests` over backend-specific unit tests when behavior can be expressed as a finder query and expected result.
- Put shared example models, mappings, and fixture data in `datafinder_examples`; have backend test suites run the shared specs against their engine.
- Update the shared model and example/test data when a feature needs new schema or representative rows. Use individual unit tests for behavior that cannot be covered by a finder spec.

## Git Workflow

- Create a branch for each feature or bug fix before changing files.
- Use a short, descriptive branch name.
