from m3 import PrimitiveType, Property
from trade_mapping import create_mappings
from jinja2 import Environment, FileSystemLoader


def is_primitive(prop: Property) -> bool:
    return isinstance(prop.type, PrimitiveType)

def generate():
    generate_with_path("templates/")

def generate_with_path(path):
    rcms = create_mappings()

    environment = Environment(loader=FileSystemLoader(path),trim_blocks=True,lstrip_blocks=True)
    template = environment.get_template("finder_template.txt")

    for rcm in rcms:
        filename = f"{rcm.clazz.name.lower()}_finder.py"
        content = template.render(rcm=rcm,is_primitive=is_primitive)
        with open(filename, mode="w", encoding="utf-8") as message:
            message.write(content)
            print(f"... wrote {filename}")


if __name__ == '__main__':
    generate()
