"""
Access to the example models, mappings and sample CSV data, which live in the data-finder-specs
project (``datafinder_specs/specs/{models,mappings,data}``) so that they ship with no Python.

    from datafinder_examples import example_path
    path = example_path("finance_mapping.md")

In a source checkout the files are read from ``datafinder_specs`` next to this package; otherwise from the
installed ``data-finder-specs`` distribution.
"""
from importlib import metadata
from pathlib import Path

_CHECKOUT = Path(__file__).resolve().parents[3] / "datafinder_specs"
_DISTRIBUTION = "data-finder-specs"


def specs_root() -> Path:
    """The directory that holds the project's files: ``specs`` is inside it."""
    if _CHECKOUT.is_dir():
        return _CHECKOUT
    return Path(str(metadata.distribution(_DISTRIBUTION).locate_file("")))


def example_path(filename: str) -> Path:
    """Return a Path to an example file: a CSV is in ``data``, a mapping in ``mappings``, any other markdown a model in ``models``."""
    if filename.endswith(".csv"):
        folder = "data"
    elif "mapping" in filename:
        folder = "mappings"
    else:
        folder = "models"
    return specs_root() / "specs" / folder / filename
