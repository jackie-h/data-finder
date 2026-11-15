"""
Test specifications for a class mapped to several queries, each a named set (sql_query_sets_mapping.md): the class is the union of its sets, and a retail
customer and a corporate one can have the same id.

    org.retail_customers — id, name; org.corporate_customers — id, name; org.orders — id, customer_id, amount
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

SQL_QUERY_SETS_MAPPING = "sql_query_sets_mapping.md"
_NO_QUERIES = {"graphql": "The GraphQL finder does not read a mapping's SQL queries"}

SQL_QUERY_SETS_CUSTOMER_FINDER_SPECS = FinderSpec(
    finder_name="CustomerFinder",
    mapping_file=SQL_QUERY_SETS_MAPPING,
    expectations=[
        TestExpectation(
            name="the_customers_of_both_sets_are_one_class_and_an_id_in_both_is_two_customers",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.id_(), f.name()]).order_by(f.id_().ascending(), f.name().ascending()),
            expected_columns=["Id", "Name"],
            expected_result=np.array([[1, "Acme"], [1, "Ann"], [2, "Bob"], [3, "Globex"]], dtype=object),
            expected_sql={"duckdb": "UNION ALL"},
        ),
        TestExpectation(
            name="a_filter_on_the_union_reads_the_rows_of_every_set",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.id_(), f.name()], f.id_().eq(1)).order_by(f.name().ascending()),
            expected_columns=["Id", "Name"],
            expected_result=np.array([[1, "Acme"], [1, "Ann"]], dtype=object),
        ),
        TestExpectation(
            name="an_association_that_names_the_retail_set_joins_the_orders_of_a_retail_customer_and_not_of_a_corporate_one_with_the_same_id",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.name(), f.orders().amount()]).order_by(f.name().ascending()),
            expected_columns=["Name", "Orders Amount"],
            expected_result=np.array([["Acme", None], ["Ann", 50], ["Bob", 70], ["Globex", None]], dtype=object),
            expected_sql={"duckdb": "_set"},
        ),
    ],
)


SQL_QUERY_SETS_EMBEDDED_MAPPING = "sql_query_sets_embedded_mapping.md"

SQL_QUERY_SETS_APPLICATION_FINDER_SPECS = FinderSpec(
    finder_name="ApplicationFinder",
    mapping_file=SQL_QUERY_SETS_EMBEDDED_MAPPING,
    expectations=[
        TestExpectation(
            name="an_embedded_object_is_read_from_the_columns_of_the_set_a_row_is_of",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.id_(), f.source(), f.applicant().first_name(), f.applicant().nick_name()])
                             .order_by(f.id_().ascending(), f.source().ascending()),
            expected_columns=["Id", "Source", "Applicant FirstName", "Applicant NickName"],
            expected_result=np.array([[1, "agency1", "Ann", "Annie"], [1, "agency2", "Cy", "Cee"], [2, "agency1", "Bob", None],
                                      [3, "agency2", "Di", "Dee"]], dtype=object),
            expected_sql={"duckdb": "UNION ALL"},
        ),
        TestExpectation(
            name="an_object_embedded_in_an_embedded_object",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.id_(), f.applicant().address().name()]).order_by(f.id_().ascending(), f.applicant().address().name().ascending()),
            expected_columns=["Id", "Applicant Address Name"],
            expected_result=np.array([[1, "1 High St"], [1, "3 Mid Ln"], [2, "2 Low Rd"], [3, None]], dtype=object),
        ),
        TestExpectation(
            name="a_constant_of_a_set_is_a_column_of_its_query",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.id_(), f.firm().legal_name()]).order_by(f.id_().ascending(), f.firm().legal_name().ascending()),
            expected_columns=["Id", "Firm LegalName"],
            expected_result=np.array([[1, "Acme"], [1, "vogue"], [2, "Globex"], [3, "vogue"]], dtype=object),
        ),
        TestExpectation(
            name="a_filter_on_an_embedded_property_of_the_union",
            skip_backends=_NO_QUERIES,
            query=lambda f: f.find_all([f.id_(), f.applicant().first_name()], f.applicant().address().name().eq("1 High St")),
            expected_columns=["Id", "Applicant FirstName"],
            expected_result=np.array([[1, "Ann"]], dtype=object),
        ),
    ],
)
