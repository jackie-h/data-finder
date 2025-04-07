import os

from model.m3 import PrimitiveType, Property
from jinja2 import Environment, PackageLoader

from model.mapping import Mapping


def is_primitive(prop: Property) -> bool:
    return isinstance(prop.type, PrimitiveType)


def generate(mapping:Mapping, output_directory):
    environment = Environment(loader=PackageLoader("datafinder_generator"), trim_blocks=True, lstrip_blocks=True)
    template = environment.get_template("finder_template.txt")

    for rcm in mapping.mappings:
        filename = f"{rcm.clazz.name.lower()}_finder.py"
        filepath = os.path.join(output_directory, filename)
        content = template.render(rcm=rcm,is_primitive=is_primitive)
        with open(filepath, mode="w", encoding="utf-8") as message:
            message.write(content)
            print(f"... wrote {filename}")


