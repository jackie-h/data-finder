# Claude Instructions

## Environment

This project uses `uv` with a `.venv`. Always:
- At the start of each session, activate the venv: `source .venv/bin/activate`
- Run all Python scripts with `uv run file.py` (not `python file.py`)
- Use `uv add <package>` to add dependencies (not pip install)
- Use `uv add --optional <group> <package>` for optional dependencies
- Use `uv run pytest` to run tests

## Git Workflow

- Create a new branch for every new feature or bug fix before making any changes
- Branch naming: `<short-description>`
