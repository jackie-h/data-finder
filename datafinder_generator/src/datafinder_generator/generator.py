import builtins
import keyword
import os
import re

from model.m3 import PrimitiveType, Property

_BUILTIN_NAMES = set(dir(builtins))
from jinja2 import Environment, PackageLoader

from model.m3 import Association
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
    if ' ' in prop.name:
        name = prop.name.lower().replace(' ', '_')
    else:
        name = to_snake_case(prop.name)
    if keyword.iskeyword(name) or name in _BUILTIN_NAMES:
        name += '_'
    return name

def table_qualified_name(table) -> str:
    return table.qualified_name


def to_snake_case(name: str) -> str:
    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', s)
    return s.lower()


def _mapping_to_class_name(name: str) -> str:
    parts = re.split(r'[\s_]+', name)
    return ''.join(p[0].upper() + p[1:] for p in parts if p) + 'Context'


def _mapping_to_filename(name: str) -> str:
    parts = re.split(r'[\s_]+', name)
    return '_'.join(p.lower() for p in parts if p) + '_context.py'


def _build_association_lookup(mapping: Mapping) -> dict:
    """Build (source_name, target_name, target_property) -> Association from all packages in the mapping."""
    result: dict = {}
    for rcm in mapping.mappings:
        if rcm.clazz.package:
            for child in rcm.clazz.package.children:
                if isinstance(child, Association):
                    result[(child.source, child.target, child.target_property.id)] = child
    return result


def _build_reverse_assoc_map(mapping: Mapping, assoc_lookup: dict) -> dict:
    """Return target_class_name -> list of (source_rcm, rpm, assoc, reverse_name)."""
    reverse_map: dict = {}
    for rcm in mapping.mappings:
        for rpm in rcm.property_mappings:
            if is_primitive(rpm.property) or not isinstance(rpm.target, Join):
                continue
            target_cls = rpm.property.type
            assoc = assoc_lookup.get((rcm.clazz.name, target_cls.name, rpm.property.id))
            if assoc is None:
                continue
            reverse_name = to_snake_case(assoc.source_property.id)
            reverse_map.setdefault(target_cls.name, []).append((rcm, rpm, assoc, reverse_name))
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
    finder_template = environment.get_template("finder_template.txt")
    base_template = environment.get_template("finder_base_template.txt")
    context_template = environment.get_template("context_template.txt")

    class_module_map = {}
    class_module_map_base = {}
    for rcm in mapping.mappings:
        pkg = _class_package(rcm.clazz)
        impl_module = f"{rcm.clazz.name.lower()}_finder"
        base_module = f"{rcm.clazz.name.lower()}_finder_base"
        class_module_map[rcm.clazz.name] = f"{pkg}.{impl_module}" if pkg else impl_module
        class_module_map_base[rcm.clazz.name] = f"{pkg}.{base_module}" if pkg else base_module

    assoc_lookup = _build_association_lookup(mapping)
    reverse_assoc_map = _build_reverse_assoc_map(mapping, assoc_lookup)

    shared_context = dict(
        class_module_map=class_module_map,
        class_module_map_base=class_module_map_base,
        is_primitive=is_primitive,
        has_processing_temporal=has_processing_temporal,
        has_uni_business_temporal=has_uni_business_temporal,
        has_business_date_and_processing=has_business_date_and_processing,
        has_bitemporal=has_bitemporal,
        display_name=display_name,
        to_python_name=to_python_name,
        table_qualified_name=table_qualified_name,
        to_snake_case=to_snake_case,
    )

    for rcm in mapping.mappings:
        pkg = _class_package(rcm.clazz)
        out_dir = _ensure_package_dirs(output_directory, pkg) if pkg else output_directory
        reverse_assocs = reverse_assoc_map.get(rcm.clazz.name, [])
        render_ctx = dict(shared_context, rcm=rcm, reverse_assocs=reverse_assocs)

        base_filename = f"{rcm.clazz.name.lower()}_finder_base.py"
        base_filepath = os.path.join(out_dir, base_filename)
        with open(base_filepath, mode="w", encoding="utf-8") as f:
            f.write(base_template.render(**render_ctx))
            print(f"... wrote {base_filename}")

        impl_filename = f"{rcm.clazz.name.lower()}_finder.py"
        impl_filepath = os.path.join(out_dir, impl_filename)
        with open(impl_filepath, mode="w", encoding="utf-8") as f:
            f.write(finder_template.render(**render_ctx))
            print(f"... wrote {impl_filename}")

    context_filename = _mapping_to_filename(mapping.name)
    context_filepath = os.path.join(output_directory, context_filename)
    context_class_name = _mapping_to_class_name(mapping.name)
    with open(context_filepath, mode="w", encoding="utf-8") as f:
        f.write(context_template.render(**shared_context, mapping=mapping,
                                        context_class_name=context_class_name))
        print(f"... wrote {context_filename}")
