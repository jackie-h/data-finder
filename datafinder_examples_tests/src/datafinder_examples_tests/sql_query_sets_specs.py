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
