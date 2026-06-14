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
    def __init__(self, name: str, id: str, type: Type | None,
                 multiplicity: 'Multiplicity | None' = None,
                 tagged_values: list[TaggedValue] | None = None):
        super().__init__(tagged_values)
        self.name = name
        self._id = id
        self.type = type
        self.multiplicity = multiplicity
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
    """Cardinality of an association end (lower bound .. upper bound)."""

    def __init__(self, lower: int):
        if lower < 0:
            raise ValueError(f"Lower bound must be >= 0, got {lower}")
        self._lower = lower

    @property
    def lower(self) -> int:
        return self._lower

    def has_upper_bound(self) -> bool:
        raise NotImplementedError

    @property
    def upper(self) -> int:
        raise NotImplementedError("Check has_upper_bound() before accessing upper")

    def is_many(self) -> bool:
        """True when the end can hold more than one value."""
        return not self.has_upper_bound() or self.upper > 1


class BoundedMultiplicity(Multiplicity):
    """Multiplicity with a known upper bound (e.g. 0..1, 1..1)."""

    def __init__(self, lower: int, upper: int):
        super().__init__(lower)
        if upper < lower:
            raise ValueError(f"Upper bound {upper} must be >= lower bound {lower}")
        self._upper = upper

    def has_upper_bound(self) -> bool:
        return True

    @property
    def upper(self) -> int:
        return self._upper

    def __repr__(self) -> str:
        return str(self._lower) if self._lower == self._upper else f"{self._lower}..{self._upper}"

    def __str__(self) -> str:
        return repr(self)


class UnboundedMultiplicity(Multiplicity):
    """Multiplicity with no upper bound (e.g. 0..*, 1..*)."""

    def __init__(self, lower: int):
        super().__init__(lower)

    def has_upper_bound(self) -> bool:
        return False

    @property
    def upper(self) -> int:
        raise ValueError("UnboundedMultiplicity has no upper bound; check has_upper_bound() first")

    def __repr__(self) -> str:
        return "*" if self._lower == 0 else f"{self._lower}..*"

    def __str__(self) -> str:
        return repr(self)


ONE_TO_ONE = BoundedMultiplicity(1, 1)
ZERO_TO_ONE = BoundedMultiplicity(0, 1)
ONE_TO_MANY = UnboundedMultiplicity(1)
ZERO_TO_MANY = UnboundedMultiplicity(0)


class Association(PackagableElement):
    def __init__(self, name: str,
                 source: str, source_multiplicity: Multiplicity,
                 source_property_name: str, source_property_id: str,
                 target: str, target_multiplicity: Multiplicity,
                 target_property_name: str, target_property_id: str,
                 package: Package | None, tagged_values: list[TaggedValue] | None = None):
        if not isinstance(source_multiplicity, Multiplicity):
            raise ValueError(f"Association '{name}': source_multiplicity must be a Multiplicity instance")
        if not isinstance(target_multiplicity, Multiplicity):
            raise ValueError(f"Association '{name}': target_multiplicity must be a Multiplicity instance")
        if not source_property_id:
            raise ValueError(f"Association '{name}' must specify Source Property")
        if not target_property_id:
            raise ValueError(f"Association '{name}' must specify Target Property")
        super().__init__(package, tagged_values)
        self.name = name
        self.source = source
        self.source_multiplicity = source_multiplicity
        self.source_property = Property(source_property_name, source_property_id, None,
                                        multiplicity=source_multiplicity)
        self.target = target
        self.target_multiplicity = target_multiplicity
        self.target_property = Property(target_property_name, target_property_id, None,
                                        multiplicity=target_multiplicity)
