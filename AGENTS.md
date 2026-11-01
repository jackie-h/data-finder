# Agent Instructions

## Environment

- This project uses `uv` and a `.venv`. Run Python scripts with `uv run` rather than invoking `python` directly.
- Run the full test suite with `uv run --extra test pytest`. The root project's test dependencies are in the `test` optional dependency group.
- Add packages with `uv add`, not `pip install`.

## Dependencies

- Keep library dependencies to a minimum.
- Add a dependency to the sub-project that needs it, using the appropriate optional dependency group.
- Add a dependency to the root project only when all sub-projects need it.

## Git Workflow

- Create a branch for each feature or bug fix before changing files.
- Use a short, descriptive branch name.
