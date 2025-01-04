from trade_mapping import create_mapping
from jinja2 import Environment, FileSystemLoader

def generate():
    rcm = create_mapping()

    environment = Environment(loader=FileSystemLoader("templates/"))
    template = environment.get_template("finder_template.txt")

    filename = f"{rcm.clazz.name.lower()}_finder.py"
    content = template.render(rcm=rcm)
    with open(filename, mode="w", encoding="utf-8") as message:
        message.write(content)
        print(f"... wrote {filename}")


if __name__ == '__main__':
    generate()