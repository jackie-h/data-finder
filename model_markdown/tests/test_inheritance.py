import os

from model_markdown.markdown_model import load, loads, to_markdown
from model.m3 import Class, Integer, String, TaggedValue

FIXTURE = os.path.join(os.path.dirname(__file__), "orgchart_inheritance.md")
DIAMOND_FIXTURE = os.path.join(os.path.dirname(__file__), "diamond_model.md")


class TestSuperclassParsing:

    def setup_method(self):
        self.packages = load(FIXTURE)
        self.pkg = self.packages[0]
        self.by_name = {c.name: c for c in self.pkg.children if isinstance(c, Class)}

    def test_no_superclasses_on_base_class(self):
        assert self.by_name["Person"].superclasses == []

    def test_single_superclass_parsed(self):
        employee = self.by_name["Employee"]
        super_names = [s.name for s in employee.superclasses]
        assert "Person" in super_names

    def test_multiple_superclasses_parsed(self):
        employee = self.by_name["Employee"]
        super_names = [s.name for s in employee.superclasses]
        assert super_names == ["Person", "Contactable"]

    def test_superclass_is_class_object(self):
        employee = self.by_name["Employee"]
        assert all(isinstance(s, Class) for s in employee.superclasses)

    def test_superclass_identity(self):
        employee = self.by_name["Employee"]
        assert employee.superclasses[0] is self.by_name["Person"]
        assert employee.superclasses[1] is self.by_name["Contactable"]


class TestAllProperties:

    def setup_method(self):
        self.packages = load(FIXTURE)
        self.pkg = self.packages[0]
        self.by_name = {c.name: c for c in self.pkg.children if isinstance(c, Class)}

    def test_base_class_all_properties_equals_own(self):
        person = self.by_name["Person"]
        assert set(person.all_properties()) == set(person.properties)

    def test_subclass_inherits_parent_properties(self):
        employee = self.by_name["Employee"]
        all_props = employee.all_properties()
        assert "id" in all_props
        assert "first_name" in all_props
        assert "last_name" in all_props

    def test_subclass_inherits_from_all_parents(self):
        employee = self.by_name["Employee"]
        all_props = employee.all_properties()
        assert "email" in all_props  # from Contactable

    def test_subclass_own_properties_present(self):
        employee = self.by_name["Employee"]
        all_props = employee.all_properties()
        assert "department" in all_props
        assert "manager" in all_props

    def test_own_property_overrides_inherited(self):
        content = """\
## Sub-Domain: test

### Class: Base

| Name | Description |
|------|-------------|
| Base |             |

| Property | Id  | Type    | Key | Description |
|----------|-----|---------|-----|-------------|
| Id       | id  | String  | Y   |             |

### Class: Child extends Base

| Name  | Description |
|-------|-------------|
| Child |             |

| Property | Id  | Type    | Key | Description |
|----------|-----|---------|-----|-------------|
| Id       | id  | Integer | Y   |             |
"""
        pkgs = loads(content)
        by_name = {c.name: c for c in pkgs[0].children if isinstance(c, Class)}
        assert by_name["Child"].all_properties()["id"].type == Integer

    def test_inherited_property_type_resolved(self):
        employee = self.by_name["Employee"]
        assert employee.all_properties()["id"].type == Integer

    def test_left_to_right_mro(self):
        content = """\
## Sub-Domain: test

### Class: A

| Name | Description |
|------|-------------|
| A    |             |

| Property | Id | Type   | Key | Description |
|----------|----|--------|-----|-------------|
| X        | x  | String |     |             |

### Class: B

| Name | Description |
|------|-------------|
| B    |             |

| Property | Id | Type    | Key | Description |
|----------|----|---------|-----|-------------|
| X        | x  | Integer |     |             |

### Class: C extends A, B

| Name | Description |
|------|-------------|
| C    |             |

| Property | Id  | Type   | Key | Description |
|----------|-----|--------|-----|-------------|
| Note     | note| String |     |             |
"""
        pkgs = loads(content)
        by_name = {c.name: c for c in pkgs[0].children if isinstance(c, Class)}
        # B overrides A for 'x'; C has no own 'x', so B's wins
        assert by_name["C"].all_properties()["x"].type == Integer


class TestForwardReference:

    def test_superclass_defined_after_subclass(self):
        content = """\
## Sub-Domain: test

### Class: Child extends Parent

| Name  | Description |
|-------|-------------|
| Child |             |

| Property | Id  | Type   | Key | Description |
|----------|-----|--------|-----|-------------|
| Own      | own | String |     |             |

### Class: Parent

| Name   | Description |
|--------|-------------|
| Parent |             |

| Property | Id       | Type    | Key | Description |
|----------|----------|---------|-----|-------------|
| Base Id  | base_id  | Integer | Y   |             |
"""
        pkgs = loads(content)
        by_name = {c.name: c for c in pkgs[0].children if isinstance(c, Class)}
        assert by_name["Child"].superclasses[0] is by_name["Parent"]
        assert "base_id" in by_name["Child"].all_properties()


class TestDiamondInheritance:
    """A extends B, C — both B and C extend D — D's properties must appear exactly once."""

    def setup_method(self):
        packages = load(DIAMOND_FIXTURE)
        self.by_name = {c.name: c for c in packages[0].children if isinstance(c, Class)}

    def test_diamond_base_has_expected_properties(self):
        assert set(self.by_name["Auditable"].properties) == {"id", "created_at"}

    def test_trackable_inherits_from_auditable(self):
        assert self.by_name["Trackable"].superclasses[0] is self.by_name["Auditable"]

    def test_versioned_inherits_from_auditable(self):
        assert self.by_name["Versioned"].superclasses[0] is self.by_name["Auditable"]

    def test_record_inherits_from_trackable_and_versioned(self):
        super_names = [s.name for s in self.by_name["Record"].superclasses]
        assert super_names == ["Trackable", "Versioned"]

    def test_diamond_base_properties_appear_exactly_once(self):
        all_props = self.by_name["Record"].all_properties()
        assert list(all_props.keys()).count("id") == 1
        assert list(all_props.keys()).count("created_at") == 1

    def test_all_properties_count_is_correct(self):
        # id, created_at (from Auditable) + updated_at (Trackable) + version (Versioned) + record_name (own) = 5
        assert len(self.by_name["Record"].all_properties()) == 5

    def test_all_expected_properties_present(self):
        all_props = self.by_name["Record"].all_properties()
        assert set(all_props) == {"id", "created_at", "updated_at", "version", "record_name"}

    def test_diamond_property_identity_is_auditable_instance(self):
        # The 'id' property in Record should be the same object as the one in Auditable
        auditable_id = self.by_name["Auditable"].properties["id"]
        record_id = self.by_name["Record"].all_properties()["id"]
        assert record_id is auditable_id

    def test_diamond_roundtrip(self):
        packages = load(DIAMOND_FIXTURE)
        content = to_markdown("Diamond", packages)
        packages2 = loads(content)
        by_name2 = {c.name: c for c in packages2[0].children if isinstance(c, Class)}
        assert len(by_name2["Record"].all_properties()) == 5
        assert set(by_name2["Record"].all_properties()) == {"id", "created_at", "updated_at", "version", "record_name"}


class TestRoundtrip:

    def test_roundtrip_preserves_superclasses(self):
        packages = load(FIXTURE)
        content = to_markdown("Org Chart", packages)
        packages2 = loads(content)
        by_name2 = {c.name: c for c in packages2[0].children if isinstance(c, Class)}
        super_names = [s.name for s in by_name2["Employee"].superclasses]
        assert super_names == ["Person", "Contactable"]

    def test_roundtrip_no_superclass_unchanged(self):
        packages = load(FIXTURE)
        content = to_markdown("Org Chart", packages)
        packages2 = loads(content)
        by_name2 = {c.name: c for c in packages2[0].children if isinstance(c, Class)}
        assert by_name2["Person"].superclasses == []

    def test_extends_written_to_markdown(self):
        packages = load(FIXTURE)
        content = to_markdown("Org Chart", packages)
        assert "### Class: Employee extends Person, Contactable" in content

    def test_base_class_heading_has_no_extends(self):
        packages = load(FIXTURE)
        content = to_markdown("Org Chart", packages)
        assert "### Class: Person\n" in content or "### Class: Person\r" in content
