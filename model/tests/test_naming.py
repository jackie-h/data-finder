from model.m3 import Property, String, _name_to_camel_id, ONE_TO_ONE, ZERO_TO_ONE
from model.naming import to_python_name, to_snake_case


def _prop(label: str, id: str | None = None) -> Property:
    return Property(label, id or _name_to_camel_id(label), String, ONE_TO_ONE)


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
        prop = Property("Valid From", "validFrom", String, ZERO_TO_ONE)
        assert to_python_name(prop) == "valid_from"


class TestToSnakeCase:

    def test_single_word(self):
        assert to_snake_case("Account") == "account"

    def test_pascal_case(self):
        assert to_snake_case("ContractualPosition") == "contractual_position"

    def test_already_lower(self):
        assert to_snake_case("trade") == "trade"

    def test_acronym_prefix(self):
        assert to_snake_case("HTTPServer") == "http_server"

    def test_display_name_with_spaces(self):
        assert to_snake_case("Account Branch City") == "account_branch_city"

    def test_punctuation_collapses_to_one_underscore(self):
        assert to_snake_case("Valid-From (UTC)") == "valid_from_utc"

    def test_repeated_and_edge_separators(self):
        assert to_snake_case(" Account  Name ") == "account_name"


class TestSharedByTheGenerators:
    """The relational and GraphQL generators both name properties with these functions."""

    def test_the_relational_generator_uses_them(self):
        import datafinder_generator.generator as relational
        assert relational.to_python_name is to_python_name
        assert relational.to_snake_case is to_snake_case

    def test_the_graphql_generator_uses_them(self):
        import datafinder_graphql.generator as graphql
        assert graphql.to_python_name is to_python_name
