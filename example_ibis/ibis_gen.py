from trade_mapping import create_mappings
from jinja2 import Environment, FileSystemLoader

def generate():
    rcms = create_mappings()

    environment = Environment(loader=FileSystemLoader("templates/"))
    template = environment.get_template("finder_template.txt")

    for rcm in rcms:
        filename = f"{rcm.clazz.name.lower()}_finder.py"
        content = template.render(rcm=rcm)
        with open(filename, mode="w", encoding="utf-8") as message:
            message.write(content)
            print(f"... wrote {filename}")


if __name__ == '__main__':
    generate()