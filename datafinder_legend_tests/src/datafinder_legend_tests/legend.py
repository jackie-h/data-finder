"""Helpers for porting Legend tests into specs."""
from __future__ import annotations

from datafinder_examples_tests.spec import TestExpectation

BACKEND = "duckdb"

# Differences from Legend that the strict comparison reports. The SQL means the same; the text does not match.
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
NOT_EQUAL_NULL = (
    "Legend writes `!=` as `col <> x or col is null`, so a missing value counts as different; the finder writes just "
    "`col <> x`, which is not true for a missing value"
)
FLOAT_DIVISION = (
    "Legend writes x / y as (1.0 * x) / y so integer columns divide as floats; the column is already a float, so the "
    "finder writes x / y"
)
DATETIME_MILESTONING_DATE = (
    "a DateTime given as a milestoning date is written by Legend as `cast(truncate('2015-08-26 02:00:00') as date)`; the "
    "finder works out the date itself and writes `'2015-08-26'`, which is the same date"
)
PROCESSING_SNAPSHOT_DATETIME = (
    "a DateTime given to a processing snapshot is written by Legend as `cast(truncate('2015-08-26 02:00:00') as date)`; the "
    "finder keeps the whole datetime and writes `'2015-08-26 02:00:00'`"
)
SECOND_JOIN_FOR_PROJECTED_MILESTONED_PROPERTY = (
    "when a milestoned navigation is used in both the filter and the projection, Legend joins its table a second time "
    "through a derived table to read the projected column; the finder joins it once and reads both from it, which "
    "returns the same rows"
)
ISOLATED_MILESTONED_JOIN = (
    "Legend reads a milestoned table that is joined from another milestoned table through a derived table holding its "
    "milestoning filter; the finder puts that filter in the join's ON clause, which keeps the same rows because the "
    "join is a left outer join"
)
LEFT_JOIN_ORDER = (
    "Legend writes the left joins of a query in the order its filter and projection first reach them; the finder writes "
    "them in the order of the projection and then the filter. Left joins that do not depend on one another return the "
    "same rows in either order"
)
NOT_EQUAL_NULL_AND_ISOLATED_MILESTONED_JOIN = NOT_EQUAL_NULL + "; and " + ISOLATED_MILESTONED_JOIN
AGGREGATE_IN_DERIVED_TABLE = (
    "when a query projects an aggregate of a to-many navigation beside plain columns, Legend aggregates in a derived "
    "table grouped by the root's keys and joins it back to the root; the finder groups the one query by the root's keys, "
    "which returns one row per root row with the same aggregate"
)
EXISTS_SUB_SELECT_ORDER = (
    "for a to-many step that is only asked about, Legend writes the sub-select's milestoning before its property filter and "
    "joins the sub-select before the join that reads values through the same step; the finder writes the property filter "
    "first and the sub-select after the other join. And-ed conditions and independent left joins give the same rows in "
    "either order"
)
CORRELATED_EXISTS = (
    "when a query only filters on a to-many step, Legend writes `not (not exists (select 1 ... where child.fk = root.id))`; the "
    "finder joins a DISTINCT sub-select of the child's join column and tests it is not null, which returns the same rows"
)
EXISTS_ON_TO_ONE_STEP = (
    "Legend reads exists on a step that is declared to-one through a DISTINCT sub-select as it does for a to-many step; the "
    "finder keeps a to-one step as a join, which cannot repeat a row and so returns the same rows"
)
EXISTS_COLUMN_SUB_SELECT = (
    "when exists is a projected column, Legend's DISTINCT sub-select joins the root table again inside the derived table "
    "to reach the child; the finder's sub-select reads only the child's join column, which gives the same flag"
)
CONCAT_FUNCTION = (
    "Legend writes `+` on text as concat(a, b), which treats a NULL part as empty in DuckDB; the finder writes a || b, "
    "which is NULL when any part is"
)

TDS_FILTER_OPTIONAL_PROPERTY = (
    "Legend filters a projected column after the project (a TDS filter), which it does not guard; the finder filters the "
    "class, so it writes `col IS NOT NULL` before a comparison on an optional property, which changes no row")


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
