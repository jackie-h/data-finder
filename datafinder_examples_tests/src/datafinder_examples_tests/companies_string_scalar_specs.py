"""
Test specifications for string scalar functions using companies_mapping.md.

Uses a different dataset from companies_specs.py — names with whitespace variations
to exercise trimming, case conversion, and other string transforms.

    companies — id INT, name VARCHAR, category VARCHAR
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

COMPANIES_MAPPING = "companies_mapping.md"

COMPANIES_SCALAR = [
    (1, "  Acme Corp  ", "Technology"),
    (2, "beta corp", "Manufacturing"),
    (3, "Hello World", "Finance"),
]

COMPANIES_STRING_SCALAR_SPECS = FinderSpec(
    finder_name="CompanyFinder",
    mapping_file=COMPANIES_MAPPING,
    expectations=[
        TestExpectation(
            name="upper",
            query=lambda f: f.find_all(None, None, [f.name().upper()], f.id_().eq(2)),
            expected_columns=["Upper Name"],
            expected_result=np.array([["BETA CORP"]], dtype=object),
        ),
        TestExpectation(
            name="lower",
            query=lambda f: f.find_all(None, None, [f.name().lower()], f.id_().eq(3)),
            expected_columns=["Lower Name"],
            expected_result=np.array([["hello world"]], dtype=object),
        ),
        TestExpectation(
            name="strip",
            query=lambda f: f.find_all(None, None, [f.name().strip()], f.id_().eq(1)),
            expected_columns=["Strip Name"],
            expected_result=np.array([["Acme Corp"]], dtype=object),
        ),
        TestExpectation(
            name="lstrip",
            query=lambda f: f.find_all(None, None, [f.name().lstrip()], f.id_().eq(1)),
            expected_columns=["Lstrip Name"],
            expected_result=np.array([["Acme Corp  "]], dtype=object),
        ),
        TestExpectation(
            name="rstrip",
            query=lambda f: f.find_all(None, None, [f.name().rstrip()], f.id_().eq(1)),
            expected_columns=["Rstrip Name"],
            expected_result=np.array([["  Acme Corp"]], dtype=object),
        ),
        TestExpectation(
            name="length",
            query=lambda f: f.find_all(None, None, [f.name().length()], f.id_().eq(2)),
            expected_columns=["Length Name"],
            expected_result=np.array([[len("beta corp")]], dtype=object),
        ),
        TestExpectation(
            name="reverse",
            query=lambda f: f.find_all(None, None, [f.name().reverse()], f.id_().eq(2)),
            expected_columns=["Reverse Name"],
            expected_result=np.array([["beta corp"[::-1]]], dtype=object),
        ),
        TestExpectation(
            name="left_four",
            query=lambda f: f.find_all(None, None, [f.name().left(4)], f.id_().eq(2)),
            expected_columns=["Left Name"],
            expected_result=np.array([["beta"]], dtype=object),
        ),
        TestExpectation(
            name="right_four",
            query=lambda f: f.find_all(None, None, [f.name().right(4)], f.id_().eq(2)),
            expected_columns=["Right Name"],
            expected_result=np.array([["corp"]], dtype=object),
        ),
        TestExpectation(
            name="repeat_twice",
            query=lambda f: f.find_all(None, None, [f.category().repeat(2)], f.id_().eq(1)),
            expected_columns=["Repeat Category"],
            expected_result=np.array([["TechnologyTechnology"]], dtype=object),
        ),
        TestExpectation(
            name="replace",
            query=lambda f: f.find_all(None, None, [f.name().replace("corp", "inc")], f.id_().eq(2)),
            expected_columns=["Replace Name"],
            expected_result=np.array([["beta inc"]], dtype=object),
        ),
        TestExpectation(
            name="substring_from_six",
            query=lambda f: f.find_all(None, None, [f.name().substring(6)], f.id_().eq(3)),
            expected_columns=["Substring Name"],
            expected_result=np.array([["World"]], dtype=object),
        ),
        TestExpectation(
            name="substring_zero_five",
            query=lambda f: f.find_all(None, None, [f.name().substring(0, 5)], f.id_().eq(3)),
            expected_columns=["Substring Name"],
            expected_result=np.array([["Hello"]], dtype=object),
        ),
        TestExpectation(
            name="slice_stop_only",
            query=lambda f: f.find_all(None, None, [f.name()[:4]], f.id_().eq(2)),
            expected_columns=["Left Name"],
            expected_result=np.array([["beta"]], dtype=object),
        ),
        TestExpectation(
            name="slice_negative_start",
            query=lambda f: f.find_all(None, None, [f.name()[-4:]], f.id_().eq(2)),
            expected_columns=["Right Name"],
            expected_result=np.array([["corp"]], dtype=object),
        ),
        TestExpectation(
            name="slice_reverse",
            query=lambda f: f.find_all(None, None, [f.name()[::-1]], f.id_().eq(2)),
            expected_columns=["Reverse Name"],
            expected_result=np.array([["beta corp"[::-1]]], dtype=object),
        ),
    ],
)
