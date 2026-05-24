"""
Bundled examples for data-finder: domain model, relational and GraphQL
mapping definitions, and sample CSV data for the finance domain.

Access files via importlib.resources:

    import importlib.resources
    ref = importlib.resources.files("datafinder_examples").joinpath("finance_mapping.md")
    with ref.open() as f:
        content = f.read()

Or obtain the filesystem path for libraries that require one:

    from datafinder_examples import example_path
    path = example_path("finance_mapping.md")

"""
import importlib.resources
from pathlib import Path


def example_path(filename: str) -> Path:
    """Return a Path to a bundled example file."""
    ref = importlib.resources.files(__name__).joinpath(filename)
    return Path(str(ref))
