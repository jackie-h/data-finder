"""The benchmark cases: the definitions (the SQL and its parameters, in tpch_bih_definitions.py) with the rows they return, which are
generated (tpch_bih_expected.py, written by datafinder_examples/tools/build_tpch_bih_cases.py)."""
import dataclasses

from datafinder_examples_tests.tpch_bih_definitions import DEFINITIONS
from datafinder_examples_tests.tpch_bih_expected import ROWS

CASES = {case.id: dataclasses.replace(case, rows=ROWS[case.id]) for case in DEFINITIONS}
