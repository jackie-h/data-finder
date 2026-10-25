"""Helpers for porting Legend tests into specs."""
from __future__ import annotations

from datafinder_examples_tests.spec import TestExpectation

BACKEND = "duckdb"

# Differences from Legend that the strict comparison reports. The SQL means the same; the text does not match.
JOIN_OPERAND_ORDER = (
    "join conditions are written with the primary key column first; Legend writes them in the order the join was defined, "
    "here foreign key first"
)
SUB_AGGREGATION_SHAPE = (
    "Legend aggregates a to-many association per root object in a derived table joined back to the root; here the "
    "aggregate is grouped by the root explicitly, which returns the same rows"
)
RESTRICT_AFTER_GROUP_BY = (
    "Legend restricts the columns of a group by in a derived table around it; here the group by selects only the "
    "columns that are kept, which returns the same rows"
)
# Known-gap keys are backend names. This one marks a gap in the rows a query returns rather than in its SQL.
ROWS = f"{BACKEND}/rows"

TYPED_DATE_LITERAL = (
    "a date constant in the select list is written `DATE '2016-08-10'` so it comes back as a date; Legend writes the "
    "bare string literal, which comes back as text"
)

EMBEDDED_WITH_JOIN = (
    "an embedded property is read from the embedded column; the finder reads it from the joined table when the query "
    "joins that table anyway, which gives other values when the two copies differ"
)
FLOAT_AVERAGE = (
    "Legend writes avg(1.0 * x) so an integer column averages as a float; the column is already a float, so the finder "
    "writes avg(x)"
)
FLOAT_DIVISION = (
    "Legend writes x / y as (1.0 * x) / y so integer columns divide as floats; the column is already a float, so the "
    "finder writes x / y"
)
JOIN_OPERAND_ORDER_AND_FLOAT_DIVISION = f"{JOIN_OPERAND_ORDER}; and {FLOAT_DIVISION}"
JOIN_OPERAND_ORDER_AND_FLOAT_AVERAGE = f"{JOIN_OPERAND_ORDER}; and {FLOAT_AVERAGE}"
CONCAT_FUNCTION = (
    "Legend writes `+` on text as concat(a, b), which treats a NULL part as empty in DuckDB; the finder writes a || b, "
    "which is NULL when any part is"
)

OPTIONAL_PROPERTY = (
    "Legend adds `col IS NOT NULL` before a comparison on an optional property; the model has no multiplicity, and "
    "the extra predicate does not change the result"
)


def _unsupported_query(finder):
    raise NotImplementedError


def ported(name: str, source: str, legend_sql: str, query, **kwargs) -> TestExpectation:
    """A SQL-only expectation: ``query`` must produce SQL equivalent to what Legend asserts."""
    return TestExpectation(name=name, query=query, source=source,
                           equivalent_sql={BACKEND: legend_sql}, **kwargs)


def unsupported(name: str, source: str, legend_sql: str, reason: str) -> TestExpectation:
    """A Legend test that cannot be expressed with the finder API yet. Kept so the gap stays visible."""
    return TestExpectation(name=name, query=_unsupported_query, source=source,
                           equivalent_sql={BACKEND: legend_sql}, skip_backends={BACKEND: f"unsupported: {reason}"})


def discover_specs(mapping_file: str) -> list:
    """Every FinderSpec in the ported spec modules (``*_specs``) that runs against ``mapping_file``."""
    import importlib
    import pkgutil

    import datafinder_legend_tests
    from datafinder_examples_tests.spec import FinderSpec

    specs = []
    for module_info in pkgutil.walk_packages(datafinder_legend_tests.__path__, "datafinder_legend_tests."):
        if module_info.name.endswith("_specs"):
            module = importlib.import_module(module_info.name)
            specs.extend(v for v in vars(module).values() if isinstance(v, FinderSpec) and v.mapping_file == mapping_file)
    return specs
