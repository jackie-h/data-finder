# Claude Instructions

## Environment

This project uses `uv` with a `.venv`. Always:
- At the start of each session, activate the venv: `source .venv/bin/activate`
- Run all Python scripts with `uv run file.py` (not `python file.py`)
- Use `uv add <package>` to add dependencies (not pip install)
- Use `uv add --optional <group> <package>` for optional dependencies
- Use `uv run pytest` to run tests

## Dependencies

- Keep library dependencies to a minimum
- Only add a dependency to the sub-project that requires it, using `uv add --optional <group> <package>`
- Never add a dependency to the root project unless it is needed by all sub-projects

## Git Workflow

- Create a new branch for every new feature or bug fix before making any changes
- Branch naming: `<short-description>`
