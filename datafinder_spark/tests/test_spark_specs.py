"""Runs every spec suite of data-finder-specs against Spark, as the engines of DuckDB do: each suite's tables are loaded from its schema.sql and CSVs,
and the finders of the mapping of each group are generated. A spec that does not apply to Spark says so in the ``skip`` of its JSON."""
import pytest

from datafinder_examples_tests import spec_files
from spark_harness import finders_of


def _scenarios():
    """(suite, mapping, [(group, FinderSpec)]) for each set of tables and mapping the specs read."""
    found: dict[tuple[str, str], list] = {}
    for suite in spec_files.SUITES:
        for group, spec in spec_files.groups(suite).items():
            found.setdefault((suite.name, spec.mapping_file), []).append((group, spec))
    return [(suite, mapping, specs) for (suite, mapping), specs in found.items()]


def _register(suite: str, mapping: str, specs: list) -> None:
    from datafinder_examples_tests.suite_data import tables
    name = f"{suite}_{mapping.removesuffix('.md')}"

    @pytest.fixture(scope="module", name=f"{name}_finders")
    def finders(spark):
        with finders_of(spark, mapping, tables(suite), sorted({spec.finder_name for _, spec in specs})) as found:
            yield found

    cases = [pytest.param(spec, expectation, id=f"{group}-{expectation.name}") for group, spec in specs for expectation in spec.expectations]

    @pytest.mark.parametrize("spec,expectation", cases)
    def test(spec, expectation, request):
        expectation.run(request.getfixturevalue(f"{name}_finders")[spec.finder_name], backend="spark")

    test.__name__ = test.__qualname__ = f"test_{name}"
    globals()[f"{name}_finders"] = finders
    globals()[test.__name__] = test


for _suite, _mapping, _specs in _scenarios():
    _register(_suite, _mapping, _specs)
