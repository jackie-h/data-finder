import builtins
import keyword
import os

from model.m3 import PrimitiveType, Property

_BUILTIN_NAMES = set(dir(builtins))
from jinja2 import Environment, PackageLoader

from model.mapping import Mapping, MilestonePropertyMapping, ProcessingDateMilestonesPropertyMapping, \
    SingleBusinessDateMilestonePropertyMapping, BusinessDateAndProcessingMilestonePropertyMapping, \
    BiTemporalMilestonePropertyMapping


def is_primitive(prop: Property) -> bool:
    return isinstance(prop.type, PrimitiveType)

def has_processing_temporal(mapping: MilestonePropertyMapping) -> bool:
    return isinstance(mapping, ProcessingDateMilestonesPropertyMapping)

def has_uni_business_temporal(mapping: MilestonePropertyMapping) -> bool:
    return isinstance(mapping, SingleBusinessDateMilestonePropertyMapping)

def has_business_date_and_processing(mapping: MilestonePropertyMapping) -> bool:
    return isinstance(mapping, BusinessDateAndProcessingMilestonePropertyMapping)

def has_bitemporal(mapping: MilestonePropertyMapping) -> bool:
    return isinstance(mapping, BiTemporalMilestonePropertyMapping)

def display_name(prop: Property) -> str:
    return prop.name


def to_python_name(prop: Property) -> str:
    name = prop.name.lower().replace(' ', '_')
    if keyword.iskeyword(name) or name in _BUILTIN_NAMES:
        name += '_'
    return name

def table_qualified_name(table) -> str:
    if table.schema is not None:
        return table.schema.name + '.' + table.name
    return table.name


def _class_package(clazz) -> str:
    """Return the dotted package name for a class, or empty string if none."""
    if clazz.package and clazz.package.name and '.' in clazz.package.name:
        return clazz.package.name
    return ''


def _ensure_package_dirs(base_dir: str, package_name: str) -> str:
    """Create directory hierarchy and __init__.py for a dotted package name.

    Returns the leaf directory path.
    """
    current = base_dir
    for part in package_name.split('.'):
        current = os.path.join(current, part)
        os.makedirs(current, exist_ok=True)
        init_file = os.path.join(current, '__init__.py')
        if not os.path.exists(init_file):
            open(init_file, 'w').close()
    return current


def generate(mapping: Mapping, output_directory):
    environment = Environment(loader=PackageLoader("datafinder_generator"), trim_blocks=True, lstrip_blocks=True)
    template = environment.get_template("finder_template.txt")

    # Build a map from class name → importable module path for cross-package imports.
    class_module_map = {}
    for rcm in mapping.mappings:
        pkg = _class_package(rcm.clazz)
        module_name = f"{rcm.clazz.name.lower()}_finder"
        class_module_map[rcm.clazz.name] = f"{pkg}.{module_name}" if pkg else module_name

    for rcm in mapping.mappings:
        pkg = _class_package(rcm.clazz)
        out_dir = _ensure_package_dirs(output_directory, pkg) if pkg else output_directory

        filename = f"{rcm.clazz.name.lower()}_finder.py"
        filepath = os.path.join(out_dir, filename)
        content = template.render(rcm=rcm,
                                  class_module_map=class_module_map,
                                  is_primitive=is_primitive,
                                  has_processing_temporal=has_processing_temporal,
                                  has_uni_business_temporal=has_uni_business_temporal,
                                  has_business_date_and_processing=has_business_date_and_processing,
                                  has_bitemporal=has_bitemporal,
                                  display_name=display_name, to_python_name=to_python_name,
                                  table_qualified_name=table_qualified_name)
        with open(filepath, mode="w", encoding="utf-8") as message:
            message.write(content)
            print(f"... wrote {filename}")


