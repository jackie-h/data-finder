import builtins
import keyword
import os
import re

from jinja2 import Environment, PackageLoader

from model.graphql_mapping import (
    GraphQLClassMapping,
    GraphQLAssociationMapping,
    GraphQLProcessingMilestone,
    GraphQLBusinessDateMilestone,
    GraphQLBiTemporalMilestone,
)
from model.mapping import Mapping
from model.m3 import PrimitiveType

_BUILTIN_NAMES = set(dir(builtins))

_TYPE_STRINGS = {
    "Integer": "INT",
    "String": "STRING",
    "Float": "FLOAT",
    "Double": "DOUBLE",
    "DateTime": "TIMESTAMP",
    "Date": "DATE",
    "Boolean": "BOOLEAN",
}


def is_primitive_mapping(pm) -> bool:
    return not isinstance(pm, GraphQLAssociationMapping) and isinstance(pm.property.type, PrimitiveType)


def is_association_mapping(pm) -> bool:
    return isinstance(pm, GraphQLAssociationMapping)


def type_str(t) -> str:
    return _TYPE_STRINGS.get(t.name, "STRING")


def to_python_name(prop) -> str:
    if ' ' in prop.name:
        name = prop.name.lower().replace(' ', '_')
    else:
        name = _to_snake_case(prop.name)
    if keyword.iskeyword(name) or name in _BUILTIN_NAMES:
        name += '_'
    return name


def _to_snake_case(name: str) -> str:
    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', s)
    return s.lower()


def display_name(prop) -> str:
    return prop.name


def milestone_class(ms) -> str:
    if isinstance(ms, GraphQLBiTemporalMilestone):
        return "GraphQLBiTemporalMilestone"
    if isinstance(ms, GraphQLBusinessDateMilestone):
        return "GraphQLBusinessDateMilestone"
    if isinstance(ms, GraphQLProcessingMilestone):
        return "GraphQLProcessingMilestone"
    return ""


def milestone_expr(ms) -> str:
    if isinstance(ms, GraphQLBiTemporalMilestone):
        return f'GraphQLBiTemporalMilestone("{ms.business_date_argument}", "{ms.processing_argument}")'
    if isinstance(ms, GraphQLBusinessDateMilestone):
        return f'GraphQLBusinessDateMilestone("{ms.argument_name}")'
    if isinstance(ms, GraphQLProcessingMilestone):
        return f'GraphQLProcessingMilestone("{ms.argument_name}")'
    return "None"


def generate(mapping: Mapping, output_directory: str) -> None:
    env = Environment(
        loader=PackageLoader("datafinder_graphql"),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    tmpl = env.get_template("graphql_finder_template.txt")

    class_mappings_by_name = {
        cm.clazz.name: cm
        for cm in mapping.mappings
        if isinstance(cm, GraphQLClassMapping)
    }

    ctx = dict(
        is_primitive_mapping=is_primitive_mapping,
        is_association_mapping=is_association_mapping,
        type_str=type_str,
        to_python_name=to_python_name,
        display_name=display_name,
        milestone_class=milestone_class,
        milestone_expr=milestone_expr,
        class_mappings_by_name=class_mappings_by_name,
    )

    for cm in mapping.mappings:
        if not isinstance(cm, GraphQLClassMapping):
            continue
        filename = f"{cm.clazz.name.lower()}_finder.py"
        filepath = os.path.join(output_directory, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(tmpl.render(cm=cm, **ctx))
        print(f"... wrote {filename}")
