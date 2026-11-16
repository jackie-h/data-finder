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

DISTINCT_QUERY_JOIN_INSIDE = (
    "a property that a distinct mapping takes through a join is read from the join inside the distinct query, which holds it; "
    "Legend joins to that table again outside the sub select, which gives the same value for each row the distinct keeps"
)
DISTINCT_QUERY_FILTER_ON_JOIN = (
    "a filter on the joined side of a distinct mapping is a condition on the joined sub select, which a left outer join keeps "
    "to the rows that match it; Legend writes it as an exists over a second copy of the sub select, which keeps the same rows"
)

QUERY_EXPRESSION_IN_DERIVED_TABLE = (
    "a property that is an expression (a CASE) on a table that is joined to is a column of a query here, so the expression is "
    "computed in a derived table before the join; Legend computes it in the outer select, after the left outer join, so for a row "
    "with no match Legend's CASE gives its else value ('Unknown') where the query's column is NULL"
)

FILTER_REPEATED_IN_CLASS_FILTER = (
    "Legend writes a filter on the class both outside and inside the derived table that holds the class mapping's ~filter; the "
    "finder writes it once, outside, which keeps the same rows"
)

CHAINED_RESULT_SUBSELECT = (
    "a step on a result that is already a query (a filter on a concatenation) reads it as a derived table of its own; Legend adds the "
    "step to the query's outer select, so the finder has one more level of subselect, which keeps the same rows"
)

LIMIT_OF_LIMIT = (
    "a limit on rows that were already limited or offset is the one limit of the rows kept, in one select; Legend nests the first "
    "limit in a subselect and limits that, which returns the same rows"
)

SPECIAL_UNION_FILTERS = (
    "Legend selects only the columns the query reads from each set and writes a filter or a join on the union into each of its "
    "selects; the finder's union selects each set's key columns and its set id, and the filter and the join are over the union, "
    "which keeps the same rows"
)

GROUP_BY_QUERY_SUBSELECT = (
    "a class mapping ~groupBy is a query here, which is read as a derived table; Legend writes the group by into the select that reads "
    "the table, and a filter on an aggregate over a derived table of it, so the finder's SQL has a level of select of its own, which "
    "keeps the same rows"
)

GROUP_BY_JOIN_COLUMN = (
    "Legend groups TRADE by its columns and reads the description of the PRODUCT it joins in the same select, which a database that "
    "checks the group by refuses (DuckDB does); the finder joins the product to the query that groups, so it returns the rows Legend means"
)

AGGREGATE_COLUMN_QUALIFIED = (
    "Legend writes the column an aggregate reads from a derived table unqualified, listagg(\"lastName\", '*'); the finder qualifies it with "
    "the alias of the derived table, which is the same column"
)

JOIN_KEPT_AFTER_RESTRICT = (
    "Legend cuts the join to a column that a restrict drops out of the query; the finder keeps the join in the derived table the restrict "
    "reads, and counts a column where Legend counts rows, which are the same where the column is not null"
)

DATE_ARITHMETIC_SHAPE = (
    "Legend writes a date moved or cut to a period with dateadd and day arithmetic, and today as now(); the finder writes INTERVAL, DATE_TRUNC "
    "and CURRENT_DATE, which are the same dates"
)

DAY_NAME_FUNCTION = (
    "Legend writes the name of the day as formatdatetime(x, 'EEEE'); the finder writes DAYNAME(x), which is the same name"
)

DATE_PART_TRUNCATE = (
    "Legend writes the date of a datetime as cast(truncate(x) as date); the finder writes cast(date_trunc('day', x) as date), which is the same date"
)

HOUR_OF_DATETIME_LITERAL = (
    "the finder writes the hour of a datetime constant as EXTRACT(HOUR FROM TIMESTAMP '...'); Legend writes hour('...') on a bare "
    "string, which DuckDB cannot run, so there are no rows of Legend's to compare"
)

CLASS_FILTER_AS_QUERY = (
    "a class mapping ~filter is a query here, which is read as a derived table; Legend adds its condition to the WHERE of the query "
    "that reads the class, so the finder has one more level of subselect, which keeps the same rows"
)

DISTINCT_COUNT_QUALIFIED = (
    "Legend writes count(distinct(\"lastName\")) with the column unqualified; the finder qualifies it with the alias of the derived table "
    "it reads, which is the same column"
)

UNION_WITH_KEY_COLUMNS = (
    "Legend selects the key of each set implementation into the union, padded with null for the other set (pk_0_0, pk_0_1) so the rows of "
    "the two sets stay apart; the finder's union selects the columns each set gives and the id of its set in _set, which keeps them apart"
)

UNION_SETS_BY_NUMBER = (
    "Legend selects each set's key and milestoning columns, padded with null for the other sets, and joins each pair of sets through "
    "its own join and link tables; the finder gives each row the id of its set in _set and joins the unions on the pairs of sets that the "
    "association names, so its SQL has no padding and no link tables. The rows are the same"
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
DECIMAL_LITERAL_CAST = (
    "Legend casts a decimal literal, cast(1.0 as Decimal(32,16)); the finder writes the literal"
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
ISOLATED_JOIN_DATE_ADDED = (
    ISOLATED_MILESTONED_JOIN + "; and a date that is the milestoning date plus a day Legend writes as dateadd(DAY, 1, date), where the finder writes the date it is"
)

MAP_NAVIGATION = (
    "Legend reads the class a navigation reaches (map) as a join with the milestoning of the class in the WHERE, and names its aliases after the "
    "dates; the finder reads the navigation as a left outer join that has the milestoning in its ON, and the rows of the class that are there "
    "are those that have one"
)

LEFT_JOIN_ORDER = (
    "Legend writes the left joins of a query in the order its filter and projection first reach them; the finder writes "
    "them in the order of the projection and then the filter. Left joins that do not depend on one another return the "
    "same rows in either order"
)
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


QUALIFIED_PROPERTIES = (
    "qualified (derived) properties are deliberately not supported: a class has the properties it maps, and what is computed from them is "
    "written where it is used"
)


def not_supported(name: str, source: str, legend_sql: str, reason: str) -> TestExpectation:
    """A Legend test of something the finder deliberately does not do, and so will never pass: kept so that it is counted apart from the
    tests that are only not done yet."""
    return TestExpectation(name=name, query=_unsupported_query, source=source,
                           equivalent_sql={BACKEND: legend_sql}, skip_backends={BACKEND: f"unsupported: {reason}"})


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
