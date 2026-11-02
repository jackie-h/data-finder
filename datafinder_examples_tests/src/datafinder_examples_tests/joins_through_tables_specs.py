"""
Test specifications for joins that pass through other tables (joins_through_tables_mapping.md).

An employee's home address is held in the employee's row, and a country is reached through a table of references:

    hr.employees    — id INT, name VARCHAR, street VARCHAR, home_country INT, citizen_ref INT
    hr.country_refs — ref INT, code VARCHAR (no class)
    hr.countries    — code VARCHAR, name VARCHAR
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

JOINS_THROUGH_TABLES_MAPPING = "joins_through_tables_mapping.md"

JOINS_THROUGH_TABLES_EMPLOYEE_FINDER_SPECS = FinderSpec(
    finder_name="EmployeeFinder",
    mapping_file=JOINS_THROUGH_TABLES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_property_of_an_embedded_address_that_reaches_another_table",
            query=lambda f: f.find_all(None, None, [f.name(), f.home().street(), f.home().country().name()])
                             .order_by(f.name().ascending()),
            expected_columns=["Name", "Home Street", "Home Country Name"],
            expected_result=np.array([
                ["Ada", "1 Analytical Way", "United Kingdom"],
                ["Grace", "2 Compiler Court", "United States"],
                ["Linus", "3 Kernel Road", "Finland"],
            ], dtype=object),
            expected_sql={"duckdb": "LEFT OUTER JOIN hr.country_refs AS t2 ON (t0.home_country = t2.ref) "
                                    "LEFT OUTER JOIN hr.countries AS t1 ON (t1.code = t2.code)"},
        ),
        TestExpectation(
            name="a_filter_on_a_property_of_the_embedded_address_that_reaches_another_table",
            query=lambda f: f.find_all(None, None, [f.name()], f.home().country().name().eq("United States")),
            expected_columns=["Name"],
            expected_result=np.array([["Grace"]], dtype=object),
        ),
        TestExpectation(
            name="a_property_mapped_through_another_table",
            query=lambda f: f.find_all(None, None, [f.name(), f.citizenship().name()])
                             .order_by(f.name().ascending()),
            expected_columns=["Name", "Citizenship Name"],
            expected_result=np.array([
                ["Ada", "United Kingdom"], ["Grace", "United Kingdom"], ["Linus", None],
            ], dtype=object),
            expected_sql={"duckdb": "LEFT OUTER JOIN hr.country_refs AS t2 ON (t0.citizen_ref = t2.ref) "
                                    "LEFT OUTER JOIN hr.countries AS t1 ON (t1.code = t2.code)"},
        ),
        TestExpectation(
            name="a_filter_on_a_property_mapped_through_another_table",
            query=lambda f: f.find_all(None, None, [f.name()], f.citizenship().name().eq("United Kingdom"))
                             .order_by(f.name().ascending()),
            expected_columns=["Name"],
            expected_result=np.array([["Ada"], ["Grace"]], dtype=object),
        ),
        TestExpectation(
            name="the_country_of_the_home_and_of_citizenship_are_read_through_the_same_tables_separately",
            query=lambda f: f.find_all(None, None, [f.name(), f.home().country().name(), f.citizenship().name()])
                             .order_by(f.name().ascending()),
            expected_columns=["Name", "Home Country Name", "Citizenship Name"],
            expected_result=np.array([
                ["Ada", "United Kingdom", "United Kingdom"],
                ["Grace", "United States", "United Kingdom"],
                ["Linus", "Finland", None],
            ], dtype=object),
        ),
    ],
)
