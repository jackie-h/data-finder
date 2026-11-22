"""
Test specifications for classes mapped to a query written as SQL (sql_queries_mapping.md): the SQL of a Query section is read as
PostgreSQL, and a query over the class is a query over that SQL, written as a derived table. Nothing is created in the database.

    org.people — person_id, first_name, age, firm_id, firm_name; org.firms — id, name; org.trades — acc_num, gsn, qty
"""

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

SQL_QUERIES_MAPPING = "sql_queries_mapping.md"
_NO_QUERIES = {"graphql": "The GraphQL finder does not read a mapping's SQL queries"}

SQL_QUERIES_FIRM_FINDER_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_distinct_query_gives_each_firm_once",
            query=lambda f: f.find_all([f.id_(), f.legal_name()]).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="a_join_from_a_query_back_to_the_table_it_reads",
            query=lambda f: f.find_all([f.legal_name(), f.employees().first_name()])
                             .order_by(f.legal_name().ascending(), f.employees().first_name().ascending()),
        ),
    ],
)

SQL_QUERIES_POSITION_FINDER_SPECS = FinderSpec(
    finder_name="PositionFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_group_by_query_gives_the_sum_of_each_group",
            query=lambda f: f.find_all([f.gsn(), f.quantity()]).order_by(f.gsn().ascending()),
        ),
        TestExpectation(
            name="a_filter_on_a_column_the_query_computes",
            query=lambda f: f.find_all([f.gsn()], f.quantity() > 5),
        ),
    ],
)

SQL_QUERIES_FIRM_X_PERSON_FINDER_SPECS = FinderSpec(
    finder_name="FirmXPersonFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_query_restricted_by_an_inner_join_and_a_filter",
            # only the people of the firm called Firm X
            query=lambda f: f.find_all([f.id_(), f.first_name()]).order_by(f.id_().ascending()),
        ),
    ],
)

SQL_QUERIES_TAG_FINDER_SPECS = FinderSpec(
    finder_name="TagFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_property_that_is_an_expression_of_the_querys_columns",
            query=lambda f: f.find_all([f.id_(), f.text()]).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="a_filter_on_the_expression",
            query=lambda f: f.find_all([f.id_()], f.text().eq("Mr Ann")),
        ),
    ],
)

SQL_QUERIES_SHOUT_FINDER_SPECS = FinderSpec(
    finder_name="ShoutFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_query_that_reads_another_query",
            # the query of the firms is a derived table inside the query of their names
            query=lambda f: f.find_all([f.id_(), f.text()]).order_by(f.id_().ascending()),
        ),
    ],
)

SQL_QUERIES_RATE_FINDER_SPECS = FinderSpec(
    finder_name="RateFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_query_with_the_columns_of_a_business_date_range_is_given_the_business_date",
            # CCC has no positive amount, so the query leaves it out; AAA's later rate has an open end (NULL thru_z)
            # the milestoning is written over the columns of the query, outside it
            query=lambda f: f.find_all("2021-06-01", None, [f.code(), f.amount()]).order_by(f.code().ascending()),
        ),
        TestExpectation(
            name="an_earlier_business_date_gets_the_earlier_rate_and_a_later_one_the_end_of_the_range",
            query=lambda f: f.find_all("2020-07-01", None, [f.code(), f.amount()]).order_by(f.code().ascending()),
        ),
        TestExpectation(
            name="a_date_after_every_range_ended_finds_only_the_open_one",
            query=lambda f: f.find_all("2023-01-01", None, [f.code(), f.amount()]),
        ),
    ],
)

SQL_QUERIES_QUOTE_FINDER_SPECS = FinderSpec(
    finder_name="QuoteFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_query_with_a_processing_and_a_business_range_is_given_both_dates",
            # CCC has no amount, which the query drops
            # each axis ends at its own infinity: a datetime for processing and a date for business
            query=lambda f: f.find_all("2021-06-01", "2021-06-01 00:00:00", [f.code(), f.amount()])
                             .order_by(f.code().ascending()),
        ),
        TestExpectation(
            name="an_earlier_processing_time_gets_the_version_known_then",
            query=lambda f: f.find_all("2021-06-01", "2020-06-01 00:00:00", [f.code(), f.amount()])
                             .order_by(f.code().ascending()),
        ),
    ],
)

SQL_QUERIES_HOLDER_FINDER_SPECS = FinderSpec(
    finder_name="HolderFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_join_to_a_milestoned_query_is_given_the_date_of_the_step",
            # CCC's rate has no positive amount, so the query has no row for it: the left join gives NULL
            query=lambda f: f.find_all([f.id_(), f.rate(business_date="2021-06-01").amount()]).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="the_date_of_the_step_picks_the_version",
            query=lambda f: f.find_all([f.id_(), f.rate(business_date="2020-07-01").amount()]).order_by(f.id_().ascending()),
        ),
    ],
)

SQL_QUERIES_UNIT_FINDER_SPECS = FinderSpec(
    finder_name="UnitFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_query_is_joined_to_a_second_copy_of_itself_through_its_parent",
            # the parent of Under Hidden is a unit that the query does not give, so it has none
            query=lambda f: f.find_all([f.name(), f.parent().name()]).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="the_parent_of_a_parent",
            query=lambda f: f.find_all([f.name(), f.parent().parent().name()], f.name().eq("EMEA Sales")),
        ),
        TestExpectation(
            name="a_filter_on_the_parent",
            query=lambda f: f.find_all([f.name()], f.parent().name().eq("Root")).order_by(f.name().ascending()),
        ),
        TestExpectation(
            name="the_children_of_a_unit",
            query=lambda f: f.find_all([f.name(), f.children().name()], f.name().eq("Sales")),
        ),
    ],
)
