import builtins
import keyword
import os

from model.m3 import PrimitiveType, Property

_BUILTIN_NAMES = set(dir(builtins))
from jinja2 import Environment, PackageLoader

from model.m3 import Association, Multiplicity
from model.mapping import Mapping, MilestonePropertyMapping, ProcessingDateMilestonesPropertyMapping, \
    SingleBusinessDateMilestonePropertyMapping, BusinessDateAndProcessingMilestonePropertyMapping, \
    BiTemporalMilestonePropertyMapping
from model.relational_mapping import Join


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
    return table.qualified_name


def reverse_property_name(source_clazz, assoc) -> str:
    """Derive the reverse navigation property name on the target class."""
    name = source_clazz.name.lower()
    if assoc and assoc.source_multiplicity == Multiplicity.MANY:
        name += 's'
    return name


def _build_association_lookup(mapping: Mapping) -> dict:
    """Build (source_name, target_name) -> Association from all packages in the mapping."""
    result = {}
    for rcm in mapping.mappings:
        if rcm.clazz.package:
            for child in rcm.clazz.package.children:
                if isinstance(child, Association):
                    result[(child.source, child.target)] = child
    return result


def _build_reverse_assoc_map(mapping: Mapping, assoc_lookup: dict) -> dict:
    """Return target_class_name -> list of (source_rcm, rpm, assoc) for reverse navigation.

    Only includes entries where the association has source_multiplicity explicitly set.
    """
    reverse_map: dict = {}
    for rcm in mapping.mappings:
        for rpm in rcm.property_mappings:
            if is_primitive(rpm.property) or not isinstance(rpm.target, Join):
                continue
            target_cls = rpm.property.type
            assoc = assoc_lookup.get((rcm.clazz.name, target_cls.name))
            reverse_map.setdefault(target_cls.name, []).append((rcm, rpm, assoc))
    return reverse_map


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

    assoc_lookup = _build_association_lookup(mapping)
    reverse_assoc_map = _build_reverse_assoc_map(mapping, assoc_lookup)

    for rcm in mapping.mappings:
        pkg = _class_package(rcm.clazz)
        out_dir = _ensure_package_dirs(output_directory, pkg) if pkg else output_directory

        filename = f"{rcm.clazz.name.lower()}_finder.py"
        filepath = os.path.join(out_dir, filename)
        reverse_assocs = reverse_assoc_map.get(rcm.clazz.name, [])
        content = template.render(rcm=rcm,
                                  reverse_assocs=reverse_assocs,
                                  class_module_map=class_module_map,
                                  is_primitive=is_primitive,
                                  has_processing_temporal=has_processing_temporal,
                                  has_uni_business_temporal=has_uni_business_temporal,
                                  has_business_date_and_processing=has_business_date_and_processing,
                                  has_bitemporal=has_bitemporal,
                                  display_name=display_name, to_python_name=to_python_name,
                                  table_qualified_name=table_qualified_name,
                                  reverse_property_name=reverse_property_name)
        with open(filepath, mode="w", encoding="utf-8") as message:
            message.write(content)
            print(f"... wrote {filename}")


