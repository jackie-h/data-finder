class Package:
    def __init__(self, name: str):
        self.name = name


class PackagableElement:
    def __init__(self, package: Package):
        self.package = package


class Type:
    def __init(self):
        pass


class PrimitiveType(Type):
    def __init__(self, name: str):
        self.name = name


Integer = PrimitiveType("Integer")
String = PrimitiveType("String")
Float = PrimitiveType("Float")


class Property:
    def __init__(self, name: str, type: Type):
        self.name = name
        self.type = type


class Class(PackagableElement, Type):
    def __init__(self, name: str, properties: list[Property], package: Package):
        super().__init__(package)
        self.name = name
        self.properties = properties


class Association(PackagableElement):
    def __init__(self, name: str, source: str, target: str, package: Package):
        super().__init__(package)
        self.name = name
        self.source = source
        self.target = target