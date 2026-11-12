"""
Test specifications for classes mapped to a query written as SQL (sql_queries_mapping.md): the SQL of a Query section is read as
PostgreSQL, and a query over the class is a query over that SQL, written as a derived table. Nothing is created in the database.

    org.people — person_id, first_name, age, firm_id, firm_name; org.firms — id, name; org.trades — acc_num, gsn, qty
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

SQL_QUERIES_MAPPING = "sql_queries_mapping.md"
_NO_QUERIES = {"graphql": "The GraphQL finder does not read a mapping's SQL queries"}

SQL_QUERIES_FIRM_FINDER_SPECS = FinderSpec(
    finder_name="FirmFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_distinct_query_gives_each_firm_once",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.id_(), f.legal_name()]).order_by(f.id_().ascending()),
            expected_columns=["Id", "LegalName"],
            expected_result=np.array([[1, "Firm X"], [2, "Beta"], [3, "Gamma"]], dtype=object),
            expected_sql={"duckdb": "FROM (SELECT DISTINCT "},
        ),
        TestExpectation(
            name="a_join_from_a_query_back_to_the_table_it_reads",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.legal_name(), f.employees().first_name()])
                             .order_by(f.legal_name().ascending(), f.employees().first_name().ascending()),
            expected_columns=["LegalName", "Employees FirstName"],
            expected_result=np.array([
                ["Beta", "John"], ["Firm X", "Mary"], ["Firm X", "Peter"], ["Gamma", "Ann"],
            ], dtype=object),
        ),
    ],
)

SQL_QUERIES_POSITION_FINDER_SPECS = FinderSpec(
    finder_name="PositionFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_group_by_query_gives_the_sum_of_each_group",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.gsn(), f.quantity()]).order_by(f.gsn().ascending()),
            expected_columns=["Gsn", "Quantity"],
            expected_result=np.array([["G1", 12], ["G2", 3]], dtype=object),
            expected_sql={"duckdb": "SUM(t1.qty) AS \"quantity\" FROM org.trades AS t1 GROUP BY t1.gsn"},
        ),
        TestExpectation(
            name="a_filter_on_a_column_the_query_computes",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.gsn()], f.quantity() > 5),
            expected_columns=["Gsn"],
            expected_result=np.array([["G1"]], dtype=object),
        ),
    ],
)

SQL_QUERIES_FIRM_X_PERSON_FINDER_SPECS = FinderSpec(
    finder_name="FirmXPersonFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_query_restricted_by_an_inner_join_and_a_filter",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.id_(), f.first_name()]).order_by(f.id_().ascending()),
            expected_columns=["Id", "FirstName"],
            # only the people of the firm called Firm X
            expected_result=np.array([[1, "Peter"], [2, "Mary"]], dtype=object),
            expected_sql={"duckdb": "INNER JOIN org.firms AS"},
        ),
    ],
)

SQL_QUERIES_TAG_FINDER_SPECS = FinderSpec(
    finder_name="TagFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_property_that_is_an_expression_of_the_querys_columns",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.id_(), f.text()]).order_by(f.id_().ascending()),
            expected_columns=["Id", "Text"],
            expected_result=np.array([[1, "Mr Peter"], [2, "Mr Mary"], [3, "Mr John"], [4, "Mr Ann"]], dtype=object),
            expected_sql={"duckdb": "'Mr ' || t1.first_name AS \"text\""},
        ),
        TestExpectation(
            name="a_filter_on_the_expression",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.id_()], f.text().eq("Mr Ann")),
            expected_columns=["Id"],
            expected_result=np.array([[4]], dtype=object),
        ),
    ],
)

SQL_QUERIES_SHOUT_FINDER_SPECS = FinderSpec(
    finder_name="ShoutFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_query_that_reads_another_query",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.id_(), f.text()]).order_by(f.id_().ascending()),
            expected_columns=["Id", "Text"],
            expected_result=np.array([[1, "FIRM X"], [2, "BETA"], [3, "GAMMA"]], dtype=object),
            # the query of the firms is a derived table inside the query of their names
            expected_sql={"duckdb": "FROM (SELECT DISTINCT "},
        ),
    ],
)

SQL_QUERIES_RATE_FINDER_SPECS = FinderSpec(
    finder_name="RateFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_query_with_the_columns_of_a_business_date_range_is_given_the_business_date",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all("2021-06-01", None, [f.code(), f.amount()]).order_by(f.code().ascending()),
            expected_columns=["Code", "Amount"],
            # CCC has no positive amount, so the query leaves it out; AAA's later rate has an open end (NULL thru_z)
            expected_result=np.array([["AAA", 1.7], ["BBB", 2.0]], dtype=object),
            # the milestoning is written over the columns of the query, outside it
            expected_sql={"duckdb": ") AS t0 WHERE t0.from_z <= '2021-06-01' AND (t0.thru_z > '2021-06-01' OR t0.thru_z IS NULL)"},
        ),
        TestExpectation(
            name="an_earlier_business_date_gets_the_earlier_rate_and_a_later_one_the_end_of_the_range",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all("2020-07-01", None, [f.code(), f.amount()]).order_by(f.code().ascending()),
            expected_columns=["Code", "Amount"],
            expected_result=np.array([["AAA", 1.5], ["BBB", 2.0]], dtype=object),
        ),
        TestExpectation(
            name="a_date_after_every_range_ended_finds_only_the_open_one",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all("2023-01-01", None, [f.code(), f.amount()]),
            expected_columns=["Code", "Amount"],
            expected_result=np.array([["AAA", 1.7]], dtype=object),
        ),
    ],
)

SQL_QUERIES_QUOTE_FINDER_SPECS = FinderSpec(
    finder_name="QuoteFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_query_with_a_processing_and_a_business_range_is_given_both_dates",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all("2021-06-01", "2021-06-01 00:00:00", [f.code(), f.amount()])
                             .order_by(f.code().ascending()),
            expected_columns=["Code", "Amount"],
            # CCC has no amount, which the query drops
            expected_result=np.array([["AAA", 1.1], ["BBB", 2.0]], dtype=object),
            # each axis ends at its own infinity: a datetime for processing and a date for business
            expected_sql={"duckdb": ") AS t0 WHERE t0.in_z <= '2021-06-01 00:00:00' AND t0.out_z > '2021-06-01 00:00:00' AND "
                                    "t0.from_z <= '2021-06-01' AND t0.thru_z > '2021-06-01'"},
        ),
        TestExpectation(
            name="an_earlier_processing_time_gets_the_version_known_then",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all("2021-06-01", "2020-06-01 00:00:00", [f.code(), f.amount()])
                             .order_by(f.code().ascending()),
            expected_columns=["Code", "Amount"],
            expected_result=np.array([["AAA", 1.0], ["BBB", 2.0]], dtype=object),
        ),
    ],
)

SQL_QUERIES_HOLDER_FINDER_SPECS = FinderSpec(
    finder_name="HolderFinder",
    mapping_file=SQL_QUERIES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_join_to_a_milestoned_query_is_given_the_date_of_the_step",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.id_(), f.rate(business_date="2021-06-01").amount()]).order_by(f.id_().ascending()),
            expected_columns=["Id", "Rate Amount"],
            # CCC's rate has no positive amount, so the query has no row for it: the left join gives NULL
            expected_result=np.array([[1, 1.7], [2, 2.0], [3, None]], dtype=object),
            expected_sql={"duckdb": "ON (t1.code = t0.rate_code AND t1.from_z <= '2021-06-01' AND "
                                    "(t1.thru_z > '2021-06-01' OR t1.thru_z IS NULL))"},
        ),
        TestExpectation(
            name="the_date_of_the_step_picks_the_version",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.id_(), f.rate(business_date="2020-07-01").amount()]).order_by(f.id_().ascending()),
            expected_columns=["Id", "Rate Amount"],
            expected_result=np.array([[1, 1.5], [2, 2.0], [3, None]], dtype=object),
        ),
    ],
)
