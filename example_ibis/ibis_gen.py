from datafinder_generator.generator import generate
from m3 import PrimitiveType, Property
from trade_mapping import create_mappings


def is_primitive(prop: Property) -> bool:
    return isinstance(prop.type, PrimitiveType)

def generate_mappings():
    rcms = create_mappings()
    generate(rcms)


if __name__ == '__main__':
    generate_mappings()
