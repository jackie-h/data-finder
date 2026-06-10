class Package:
    def __init__(self, name: str):
        self.name = name
        self.children: list = []

class TaggedValue:
    DOC = 'doc'
    KEY = 'key'
    def __init__(self, name: str, value):
        self.name = name
        self.value = value

class AnnotatedElement:
    def __init__(self, tagged_values: list[TaggedValue] | None):
        self.tagged_values = {}
        if tagged_values is None:
            tagged_values = []
        for tv in tagged_values:
            self.tagged_values[tv.name] = tv

class PackagableElement(AnnotatedElement):
    def __init__(self, package: Package | None, tagged_values: list[TaggedValue] | None):
        super().__init__(tagged_values)
        self.package = package
        if package is not None:
            package.children.append(self)


def _name_to_camel_id(name: str) -> str:
    words = name.split()
    if not words:
        return name
    if len(words) == 1:
        word = words[0]
        return word.lower() if word.isupper() else word[0].lower() + word[1:]
    return words[0].lower() + ''.join(w.capitalize() for w in words[1:])


class Type:
    name: str

    def __init__(self):
        pass


class PrimitiveType(Type):
    def __init__(self, name: str):
        super().__init__()
        self.name = name


Integer = PrimitiveType("Integer")
String = PrimitiveType("String")
Double = PrimitiveType("Double")
Float = PrimitiveType("Float")
Decimal = PrimitiveType("Decimal")
DateTime = PrimitiveType("DateTime")
Date = PrimitiveType("Date")
Boolean = PrimitiveType("Boolean")


class Property(AnnotatedElement):
    def __init__(self, name: str, id: str, type: Type | None, tagged_values: list[TaggedValue] | None = None):
        super().__init__(tagged_values)
        self.name = name
        self._id = id
        self.type = type
        assert _name_to_camel_id(name) == id, (
            f"Property id must be camelCase of name: '{name}' → expected '{_name_to_camel_id(name)}', got '{id}'"
        )

    @property
    def id(self) -> str:
        return self._id


class Class(PackagableElement, Type):
    def __init__(self, name: str, properties: list[Property], package: Package | None,
                 superclasses: list['Class'] | None = None, tagged_values: list[TaggedValue] | None = None):
        super().__init__(package, tagged_values)
        self.name = name
        self.superclasses: list['Class'] = superclasses or []
        self.properties = {}
        self.properties_from_associations: dict[str, 'Property'] = {}
        for prop in properties:
            self.properties[prop.id] = prop

    def property(self, id: str) -> Property:
        return self.all_properties()[id]

    def all_properties(self) -> dict[str, 'Property']:
        """Return inherited then association-derived then own properties; own takes precedence."""
        result = {}
        for superclass in self.superclasses:
            result.update(superclass.all_properties())
        result.update(self.properties_from_associations)
        result.update(self.properties)
        return result


class Multiplicity:
    ONE = "1"
    MANY = "*"


class Association(PackagableElement):
    def __init__(self, name: str,
                 source: str, source_multiplicity: str,
                 source_property_name: str, source_property_id: str,
                 target: str, target_multiplicity: str,
                 target_property_name: str, target_property_id: str,
                 package: Package | None, tagged_values: list[TaggedValue] | None = None):
        if source_multiplicity not in (Multiplicity.ONE, Multiplicity.MANY):
            raise ValueError(f"Association '{name}' must specify Source Multiplicity ('1' or '*')")
        if target_multiplicity not in (Multiplicity.ONE, Multiplicity.MANY):
            raise ValueError(f"Association '{name}' must specify Target Multiplicity ('1' or '*')")
        if not source_property_id:
            raise ValueError(f"Association '{name}' must specify Source Property")
        if not target_property_id:
            raise ValueError(f"Association '{name}' must specify Target Property")
        super().__init__(package, tagged_values)
        self.name = name
        self.source = source
        self.source_multiplicity = source_multiplicity
        self.source_property = Property(source_property_name, source_property_id, None)
        self.target = target
        self.target_multiplicity = target_multiplicity
        self.target_property = Property(target_property_name, target_property_id, None)
