import pytest
from model.m3 import Property, String
from datafinder_generator.generator import to_python_name


def _prop(label: str, id: str = None) -> Property:
    return Property(label, id or label.lower().replace(' ', '_'), String)


class TestToPythonName:

    def test_single_word(self):
        assert to_python_name(_prop("Price")) == "price"

    def test_multi_word_label(self):
        assert to_python_name(_prop("Valid From")) == "valid_from"

    def test_multi_word_lowercase(self):
        assert to_python_name(_prop("Is Settled")) == "is_settled"

    def test_keyword_clash_from(self):
        assert to_python_name(_prop("From")) == "from_"

    def test_keyword_clash_type(self):
        assert to_python_name(_prop("Type")) == "type_"

    def test_keyword_clash_class(self):
        assert to_python_name(_prop("Class")) == "class_"

    def test_keyword_clash_import(self):
        assert to_python_name(_prop("Import")) == "import_"

    def test_builtin_clash_id(self):
        assert to_python_name(_prop("Id", "id")) == "id_"

    def test_builtin_clash_list(self):
        assert to_python_name(_prop("List")) == "list_"

    def test_no_clash_normal(self):
        assert to_python_name(_prop("Account")) == "account"

    def test_uses_label_not_id(self):
        prop = Property("Valid From", "valid_from", String)
        assert to_python_name(prop) == "valid_from"
