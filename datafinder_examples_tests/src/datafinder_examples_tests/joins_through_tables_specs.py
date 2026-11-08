"""
Test specifications for joins that pass through other tables (joins_through_tables_mapping.md).

An employee's home address is held in the employee's row, and a country is reached through a table of references whose
rows are valid for a range of dates:

    hr.employees    — id INT, name VARCHAR, street VARCHAR, home_country INT, citizen_ref INT
    hr.country_refs — ref INT, code VARCHAR, valid_from DATE, valid_to DATE (no class; reference 3 was Finland until 2010
                      and is Sweden since)
    hr.countries    — code VARCHAR, name VARCHAR

The employees have no milestoning, so a query of them has no dates: a date is given to the property that reaches the
country, and applies to the table the country is read through.
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

JOINS_THROUGH_TABLES_MAPPING = "joins_through_tables_mapping.md"

_NOW = "2024-01-01"
_THEN = "2005-01-01"

JOINS_THROUGH_TABLES_EMPLOYEE_FINDER_SPECS = FinderSpec(
    finder_name="EmployeeFinder",
    mapping_file=JOINS_THROUGH_TABLES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_property_of_an_embedded_address_that_reaches_another_table",
            query=lambda f: f.find_all([f.name(), f.home().street(), f.home().country(business_date=_NOW).name()])
                             .order_by(f.name().ascending()),
            expected_columns=["Name", "Home Street", "Home Country Name"],
            expected_result=np.array([
                ["Ada", "1 Analytical Way", "United Kingdom"],
                ["Grace", "2 Compiler Court", "United States"],
                ["Linus", "3 Kernel Road", "Sweden"],
            ], dtype=object),
            expected_sql={"duckdb": "LEFT OUTER JOIN hr.country_refs AS t2 ON (t0.home_country = t2.ref AND "
                                    "t2.valid_from <= '2024-01-01' AND t2.valid_to > '2024-01-01') "
                                    "LEFT OUTER JOIN hr.countries AS t1 ON (t1.code = t2.code)"},
        ),
        TestExpectation(
            name="a_filter_on_a_property_of_the_embedded_address_that_reaches_another_table",
            query=lambda f: f.find_all([f.name()], f.home().country(business_date=_NOW).name().eq("United States")),
            expected_columns=["Name"],
            expected_result=np.array([["Grace"]], dtype=object),
        ),
        TestExpectation(
            name="a_property_mapped_through_another_table",
            query=lambda f: f.find_all([f.name(), f.citizenship(business_date=_NOW).name()])
                             .order_by(f.name().ascending()),
            expected_columns=["Name", "Citizenship Name"],
            expected_result=np.array([
                ["Ada", "United Kingdom"], ["Grace", "United Kingdom"], ["Linus", "Sweden"],
            ], dtype=object),
            expected_sql={"duckdb": "LEFT OUTER JOIN hr.country_refs AS t2 ON (t0.citizen_ref = t2.ref AND "
                                    "t2.valid_from <= '2024-01-01' AND t2.valid_to > '2024-01-01') "
                                    "LEFT OUTER JOIN hr.countries AS t1 ON (t1.code = t2.code)"},
        ),
        TestExpectation(
            name="a_filter_on_a_property_mapped_through_another_table",
            query=lambda f: f.find_all([f.name()], f.citizenship(business_date=_NOW).name().eq("United Kingdom"))
                             .order_by(f.name().ascending()),
            expected_columns=["Name"],
            expected_result=np.array([["Ada"], ["Grace"]], dtype=object),
        ),
        TestExpectation(
            name="the_country_of_the_home_and_of_citizenship_are_read_through_the_same_tables_separately",
            query=lambda f: f.find_all([f.name(), f.home().country(business_date=_NOW).name(), f.citizenship(business_date=_NOW).name()])
                             .order_by(f.name().ascending()),
            expected_columns=["Name", "Home Country Name", "Citizenship Name"],
            expected_result=np.array([
                ["Ada", "United Kingdom", "United Kingdom"],
                ["Grace", "United States", "United Kingdom"],
                ["Linus", "Sweden", "Sweden"],
            ], dtype=object),
        ),
        TestExpectation(
            name="a_date_given_to_the_property_reaches_the_table_on_the_way",
            query=lambda f: f.find_all([f.name(), f.citizenship(business_date=_THEN).name()])
                             .order_by(f.name().ascending()),
            expected_columns=["Name", "Citizenship Name"],
            expected_result=np.array([
                ["Ada", "United Kingdom"], ["Grace", "United Kingdom"], ["Linus", "Finland"],
            ], dtype=object),
            expected_sql={"duckdb": "t2.valid_from <= '2005-01-01' AND t2.valid_to > '2005-01-01'"},
        ),
        TestExpectation(
            name="a_date_given_to_a_property_of_the_embedded_address_reaches_the_table_on_the_way",
            query=lambda f: f.find_all([f.name(), f.home().country(business_date=_THEN).name()])
                             .order_by(f.name().ascending()),
            expected_columns=["Name", "Home Country Name"],
            expected_result=np.array([
                ["Ada", "United Kingdom"], ["Grace", "United States"], ["Linus", "Finland"],
            ], dtype=object),
            expected_sql={"duckdb": "t2.valid_from <= '2005-01-01' AND t2.valid_to > '2005-01-01'"},
        ),
        TestExpectation(
            name="without_a_date_every_version_of_the_table_on_the_way_is_read",
            query=lambda f: f.find_all([f.name(), f.citizenship().name()], f.name().eq("Linus"))
                             .order_by(f.citizenship().name().ascending()),
            expected_columns=["Name", "Citizenship Name"],
            expected_result=np.array([["Linus", "Finland"], ["Linus", "Sweden"]], dtype=object),
            unexpected_sql={"duckdb": "valid_from"},
        ),
        TestExpectation(
            name="a_date_before_any_row_of_the_table_on_the_way_finds_no_country",
            query=lambda f: f.find_all([f.name(), f.citizenship(business_date="1990-01-01").name()])
                             .order_by(f.name().ascending()),
            expected_columns=["Name", "Citizenship Name"],
            expected_result=np.array([["Ada", None], ["Grace", None], ["Linus", None]], dtype=object),
        ),
    ],
)
