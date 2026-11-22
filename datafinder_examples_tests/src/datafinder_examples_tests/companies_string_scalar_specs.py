"""
Test specifications for string scalar functions using companies_mapping.md.

Uses a different dataset from companies_specs.py — names with whitespace variations
to exercise trimming, case conversion, and other string transforms.

    companies — id INT, name VARCHAR, category VARCHAR
"""

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

COMPANIES_MAPPING = "companies_mapping.md"

COMPANIES_STRING_SCALAR_SPECS = FinderSpec(
    finder_name="CompanyFinder",
    mapping_file=COMPANIES_MAPPING,
    expectations=[
        TestExpectation(
            name="upper",
            query=lambda f: f.find_all([f.name().upper()], f.id_().eq(2)),
        ),
        TestExpectation(
            name="lower",
            query=lambda f: f.find_all([f.name().lower()], f.id_().eq(3)),
        ),
        TestExpectation(
            name="strip",
            query=lambda f: f.find_all([f.name().strip()], f.id_().eq(1)),
        ),
        TestExpectation(
            name="lstrip",
            query=lambda f: f.find_all([f.name().lstrip()], f.id_().eq(1)),
        ),
        TestExpectation(
            name="rstrip",
            query=lambda f: f.find_all([f.name().rstrip()], f.id_().eq(1)),
        ),
        TestExpectation(
            name="length",
            query=lambda f: f.find_all([f.name().length()], f.id_().eq(2)),
        ),
        TestExpectation(
            name="reverse",
            query=lambda f: f.find_all([f.name().reverse()], f.id_().eq(2)),
        ),
        TestExpectation(
            name="left_four",
            query=lambda f: f.find_all([f.name().left(4)], f.id_().eq(2)),
        ),
        TestExpectation(
            name="right_four",
            query=lambda f: f.find_all([f.name().right(4)], f.id_().eq(2)),
        ),
        TestExpectation(
            name="repeat_twice",
            query=lambda f: f.find_all([f.category().repeat(2)], f.id_().eq(1)),
        ),
        TestExpectation(
            name="replace",
            query=lambda f: f.find_all([f.name().replace("corp", "inc")], f.id_().eq(2)),
        ),
        TestExpectation(
            name="substring_from_six",
            query=lambda f: f.find_all([f.name().substring(6)], f.id_().eq(3)),
        ),
        TestExpectation(
            name="substring_zero_five",
            query=lambda f: f.find_all([f.name().substring(0, 5)], f.id_().eq(3)),
        ),
        TestExpectation(
            name="expression_is_in",
            query=lambda f: f.find_all([f.id_()], f.name().substring(0, 4).is_in(["beta", "Hell"])).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="expression_is_not_in",
            query=lambda f: f.find_all([f.id_()], f.name().substring(0, 4).is_not_in(["beta", "Hell"])).order_by(f.id_().ascending()),
        ),
        TestExpectation(
            name="slice_stop_only",
            query=lambda f: f.find_all([f.name()[:4]], f.id_().eq(2)),
        ),
        TestExpectation(
            name="slice_negative_start",
            query=lambda f: f.find_all([f.name()[-4:]], f.id_().eq(2)),
        ),
        TestExpectation(
            name="slice_reverse",
            query=lambda f: f.find_all([f.name()[::-1]], f.id_().eq(2)),
        ),
        TestExpectation(
            name="mul_operator",
            query=lambda f: f.find_all([f.name() * 2], f.id_().eq(2)),
        ),
    ],
)
