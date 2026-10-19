import os

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
from model.naming import to_python_name


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


def association_name(pm) -> str:
    return pm.target.name


def association_many(pm) -> bool:
    return pm.property.multiplicity is not None and pm.property.multiplicity.is_many()


def association_path(pm) -> str:
    """The field path segment for an association; a to-many association is marked with "[]"."""
    many = pm.property.multiplicity is not None and pm.property.multiplicity.is_many()
    return pm.target.name + ("[]" if many else "")


def association_display(pm) -> str:
    """The display-name segment for an association: a to-many one is named for its class, as in the relational finders."""
    many = pm.property.multiplicity is not None and pm.property.multiplicity.is_many()
    return pm.property.type.name if many else pm.target.name.title()


def type_str(t) -> str:
    return _TYPE_STRINGS.get(t.name, "STRING")


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


def related_classes(cm, class_mappings_by_name: dict) -> list:
    """The class mappings reachable from cm through associations, each once, in discovery order."""
    found: list = []
    pending = [cm]
    while pending:
        current = pending.pop(0)
        for pm in current.property_mappings:
            if not is_association_mapping(pm):
                continue
            target_cm = class_mappings_by_name.get(pm.property.type.name)
            if target_cm is not None and target_cm not in found:
                found.append(target_cm)
                pending.append(target_cm)
    return found


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
        association_name=association_name,
        association_many=association_many,
        association_path=association_path,
        association_display=association_display,
        to_python_name=to_python_name,
        display_name=display_name,
        milestone_class=milestone_class,
        milestone_expr=milestone_expr,
        class_mappings_by_name=class_mappings_by_name,
        related_classes=lambda cm: related_classes(cm, class_mappings_by_name),
    )

    for cm in mapping.mappings:
        if not isinstance(cm, GraphQLClassMapping):
            continue
        filename = f"{cm.clazz.name.lower()}_finder.py"
        filepath = os.path.join(output_directory, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(tmpl.render(cm=cm, **ctx))
        print(f"... wrote {filename}")
