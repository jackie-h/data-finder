from m3 import PrimitiveType, Property
from jinja2 import Environment, PackageLoader

from relational import RelationalClassMapping


def is_primitive(prop: Property) -> bool:
    return isinstance(prop.type, PrimitiveType)


def generate(mappings:list[RelationalClassMapping]):
    environment = Environment(loader=PackageLoader("datafinder_generator"), trim_blocks=True, lstrip_blocks=True)
    template = environment.get_template("finder_template.txt")

    for rcm in mappings:
        filename = f"{rcm.clazz.name.lower()}_finder.py"
        content = template.render(rcm=rcm,is_primitive=is_primitive)
        with open(filename, mode="w", encoding="utf-8") as message:
            message.write(content)
            print(f"... wrote {filename}")


