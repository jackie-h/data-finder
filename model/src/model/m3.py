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
    def __init__(self, tagged_values: list[TaggedValue]):
        self.tagged_values = {}
        if tagged_values is None:
            tagged_values = []
        for tv in tagged_values:
            self.tagged_values[tv.name] = tv

class PackagableElement(AnnotatedElement):
    def __init__(self, package: Package, tagged_values: list[TaggedValue]):
        super().__init__(tagged_values)
        self.package = package
        if package is not None:
            package.children.append(self)


class Type:
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
    def __init__(self, name: str, id: str, type: Type, tagged_values: list[TaggedValue] = None):
        super().__init__(tagged_values)
        self.name = name   # human-readable label, e.g. "Valid From"
        self.id = id       # machine identifier, e.g. "valid_from"
        self.type = type


class Class(PackagableElement, Type):
    def __init__(self, name: str, properties: list[Property], package: Package,
                 superclasses: list['Class'] = None, tagged_values: list[TaggedValue] = None):
        super().__init__(package, tagged_values)
        self.name = name
        self.superclasses: list['Class'] = superclasses or []
        self.properties = {}
        for prop in properties:
            self.properties[prop.id] = prop

    def property(self, id: str) -> Property:
        return self.properties[id]

    def all_properties(self) -> dict[str, 'Property']:
        """Return inherited properties (left-to-right) merged with own; own take precedence."""
        result = {}
        for superclass in self.superclasses:
            result.update(superclass.all_properties())
        result.update(self.properties)
        return result


class Association(PackagableElement):
    def __init__(self, name: str, source: str, target: str, package: Package, tagged_values: list[TaggedValue] = None):
        super().__init__(package, tagged_values)
        self.name = name
        self.source = source
        self.target = target
