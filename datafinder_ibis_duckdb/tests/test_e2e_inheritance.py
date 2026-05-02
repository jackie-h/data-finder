import os
import shutil
import sys
import tempfile

import duckdb
import pytest

from datafinder import QueryRunnerBase
from datafinder_generator.generator import generate
from datafinder_ibis.ibis_engine import IbisConnect
from mapping_markdown.markdown_mapping import load

_MAPPING_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "orgchart_inheritance_mapping.md"))
_DIAMOND_MAPPING_FILE = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "diamond_mapping.md")
)

_FINDER_MODULES = ["employee_finder"]
_DIAMOND_FINDER_MODULES = ["record_finder"]

#
# employees table columns:
#   emp_id     ← id         (inherited from Person)
#   first_name ← first_name (inherited from Person)
#   last_name  ← last_name  (inherited from Person)
#   email      ← email      (inherited from Contactable)
#   department ← department (own Employee property)
#   manager_id ← manager    (self-referential FK)
#

_EMPLOYEES = [
    (1, "Alice", "Smith",  "alice@example.com",  "Executive", None),
    (2, "Bob",   "Jones",  "bob@example.com",    "Engineering", 1),
    (3, "Carol", "White",  "carol@example.com",  "Engineering", 1),
    (4, "Dave",  "Brown",  "dave@example.com",   "QA",          2),
]


def _build_test_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS hr CASCADE")
    conn.execute("CREATE SCHEMA hr")
    conn.execute("""
        CREATE TABLE hr.employees (
            emp_id     INT,
            first_name VARCHAR,
            last_name  VARCHAR,
            email      VARCHAR,
            department VARCHAR,
            manager_id INT
        )
    """)
    for row in _EMPLOYEES:
        conn.execute("INSERT INTO hr.employees VALUES (?, ?, ?, ?, ?, ?)", row)
    conn.close()


@pytest.fixture(scope="module")
def finders():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)

    temp_dir = tempfile.mkdtemp()
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    sys.path.insert(0, temp_dir)

    mapping = load(_MAPPING_FILE)
    generate(mapping, temp_dir)
    _build_test_db()

    from employee_finder import EmployeeFinder

    yield EmployeeFinder

    sys.path.remove(temp_dir)
    for mod in _FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestGeneratedFinderHasAllProperties:
    """Verify the finder exposes attributes for own and all inherited properties."""

    def test_has_id_from_person(self, finders):
        assert finders.id_() is not None

    def test_has_first_name_from_person(self, finders):
        assert finders.first_name() is not None

    def test_has_last_name_from_person(self, finders):
        assert finders.last_name() is not None

    def test_has_email_from_contactable(self, finders):
        assert finders.email() is not None

    def test_has_department_own_property(self, finders):
        assert finders.department() is not None

    def test_has_manager_self_ref(self, finders):
        assert finders.manager() is not None


class TestQueryInheritedProperties:
    """Queries filtering and projecting on properties from parent classes."""

    def test_all_employees_returned(self, finders):
        result = finders.find_all(
            [finders.first_name(), finders.last_name()],
        ).to_pandas()
        assert len(result) == 4

    def test_filter_by_inherited_first_name(self, finders):
        result = finders.find_all(
            [finders.first_name(), finders.department()],
            finders.first_name().eq("Alice"),
        ).to_pandas()
        assert len(result) == 1
        assert result.iloc[0]["Department"] == "Executive"

    def test_filter_by_inherited_last_name(self, finders):
        result = finders.find_all(
            [finders.first_name()],
            finders.last_name().eq("Jones"),
        ).to_pandas()
        assert result.iloc[0]["First Name"] == "Bob"

    def test_filter_by_email_from_contactable(self, finders):
        result = finders.find_all(
            [finders.first_name(), finders.email()],
            finders.email().eq("carol@example.com"),
        ).to_pandas()
        assert len(result) == 1
        assert result.iloc[0]["First Name"] == "Carol"

    def test_filter_by_own_property_department(self, finders):
        result = finders.find_all(
            [finders.first_name(), finders.department()],
            finders.department().eq("Engineering"),
        ).to_pandas()
        assert set(result["First Name"].tolist()) == {"Bob", "Carol"}

    def test_project_all_inherited_and_own_columns(self, finders):
        result = finders.find_all(
            [finders.id_(), finders.first_name(), finders.last_name(),
             finders.email(), finders.department()],
            finders.first_name().eq("Dave"),
        ).to_pandas()
        row = result.iloc[0]
        assert row["Id"] == 4
        assert row["First Name"] == "Dave"
        assert row["Last Name"] == "Brown"
        assert row["Email"] == "dave@example.com"
        assert row["Department"] == "QA"


class TestSelfReferentialJoin:
    """Verify the self-join through an inherited FK still works."""

    def test_manager_name_via_join(self, finders):
        result = finders.find_all(
            [finders.first_name(), finders.manager().first_name()],
        ).to_pandas()
        by_name = {row["First Name"]: row["Manager First Name"]
                   for _, row in result.iterrows()}
        assert by_name["Bob"] == "Alice"
        assert by_name["Carol"] == "Alice"
        assert by_name["Dave"] == "Bob"

    def test_filter_by_manager_inherited_property(self, finders):
        result = finders.find_all(
            [finders.first_name()],
            finders.manager().first_name().eq("Alice"),
        ).to_pandas()
        assert set(result["First Name"].tolist()) == {"Bob", "Carol"}

    def test_manager_email_via_join(self, finders):
        result = finders.find_all(
            [finders.first_name(), finders.manager().email()],
            finders.first_name().eq("Bob"),
        ).to_pandas()
        assert result.iloc[0]["Manager Email"] == "alice@example.com"


# ---------------------------------------------------------------------------
# Diamond inheritance: Record extends Trackable, Versioned; both extend Auditable
# ---------------------------------------------------------------------------

_DIAMOND_ITEMS = [
    (1, "2024-01-01", "2024-06-01", 3, "Alpha"),
    (2, "2024-02-01", "2024-07-01", 1, "Beta"),
    (3, "2024-03-01", "2024-03-01", 2, "Gamma"),
]


def _build_diamond_db():
    conn = duckdb.connect("test.db")
    conn.execute("DROP SCHEMA IF EXISTS records CASCADE")
    conn.execute("CREATE SCHEMA records")
    conn.execute("""
        CREATE TABLE records.items (
            item_id     INT,
            created_at  VARCHAR,
            updated_at  VARCHAR,
            version     INT,
            record_name VARCHAR
        )
    """)
    for row in _DIAMOND_ITEMS:
        conn.execute("INSERT INTO records.items VALUES (?, ?, ?, ?, ?)", row)
    conn.close()


@pytest.fixture(scope="module")
def diamond_finder():
    QueryRunnerBase.clear()
    QueryRunnerBase.register(IbisConnect)

    temp_dir = tempfile.mkdtemp()
    for mod in _DIAMOND_FINDER_MODULES:
        sys.modules.pop(mod, None)
    sys.path.insert(0, temp_dir)

    mapping = load(_DIAMOND_MAPPING_FILE)
    generate(mapping, temp_dir)
    _build_diamond_db()

    from record_finder import RecordFinder

    yield RecordFinder

    sys.path.remove(temp_dir)
    for mod in _DIAMOND_FINDER_MODULES:
        sys.modules.pop(mod, None)
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestDiamondFinderHasAllProperties:
    """Each property from Auditable should appear exactly once in the generated finder."""

    def test_has_id_from_auditable(self, diamond_finder):
        assert diamond_finder.id_() is not None

    def test_has_created_at_from_auditable(self, diamond_finder):
        assert diamond_finder.created_at() is not None

    def test_has_updated_at_from_trackable(self, diamond_finder):
        assert diamond_finder.updated_at() is not None

    def test_has_version_from_versioned(self, diamond_finder):
        assert diamond_finder.version() is not None

    def test_has_record_name_own(self, diamond_finder):
        assert diamond_finder.record_name() is not None

    def test_no_duplicate_id_accessor(self, diamond_finder):
        # If id appeared twice in the mapping there would be a collision;
        # the finder must still have exactly one id_() method.
        import inspect
        members = [name for name, _ in inspect.getmembers(diamond_finder)
                   if name == "id_"]
        assert len(members) == 1

    def test_no_duplicate_created_at_accessor(self, diamond_finder):
        import inspect
        members = [name for name, _ in inspect.getmembers(diamond_finder)
                   if name == "created_at"]
        assert len(members) == 1


class TestDiamondQueries:
    """Verify queries on all levels of the diamond work correctly."""

    def test_all_items_returned(self, diamond_finder):
        result = diamond_finder.find_all(
            [diamond_finder.id_(), diamond_finder.record_name()],
        ).to_pandas()
        assert len(result) == 3

    def test_filter_by_auditable_id(self, diamond_finder):
        result = diamond_finder.find_all(
            [diamond_finder.record_name()],
            diamond_finder.id_().eq(2),
        ).to_pandas()
        assert result.iloc[0]["Record Name"] == "Beta"

    def test_filter_by_auditable_created_at(self, diamond_finder):
        result = diamond_finder.find_all(
            [diamond_finder.record_name()],
            diamond_finder.created_at().eq("2024-03-01"),
        ).to_pandas()
        assert result.iloc[0]["Record Name"] == "Gamma"

    def test_filter_by_trackable_updated_at(self, diamond_finder):
        result = diamond_finder.find_all(
            [diamond_finder.record_name()],
            diamond_finder.updated_at().eq("2024-06-01"),
        ).to_pandas()
        assert result.iloc[0]["Record Name"] == "Alpha"

    def test_filter_by_versioned_version(self, diamond_finder):
        result = diamond_finder.find_all(
            [diamond_finder.record_name(), diamond_finder.version()],
            diamond_finder.version().eq(1),
        ).to_pandas()
        assert result.iloc[0]["Record Name"] == "Beta"

    def test_project_all_columns_from_full_hierarchy(self, diamond_finder):
        result = diamond_finder.find_all(
            [diamond_finder.id_(), diamond_finder.created_at(), diamond_finder.updated_at(),
             diamond_finder.version(), diamond_finder.record_name()],
            diamond_finder.record_name().eq("Alpha"),
        ).to_pandas()
        row = result.iloc[0]
        assert row["Id"] == 1
        assert row["Created At"] == "2024-01-01"
        assert row["Updated At"] == "2024-06-01"
        assert row["Version"] == 3
        assert row["Record Name"] == "Alpha"
