"""The data-finder-specs project holds data only and is released with the same version as data-finder."""
import tomllib
from pathlib import Path

from datafinder_examples import specs_root

_ROOT = Path(__file__).resolve().parents[2]


def _version(pyproject: Path) -> str:
    return tomllib.loads(pyproject.read_text())["project"]["version"]


def test_the_specs_version_is_the_data_finder_version():
    assert _version(_ROOT / "datafinder_specs" / "pyproject.toml") == _version(_ROOT / "pyproject.toml")


def test_the_specs_project_has_no_python():
    python_files = [p for p in specs_root().rglob("*.py") if "__pycache__" not in p.parts]
    assert python_files == []
