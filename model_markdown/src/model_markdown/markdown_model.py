import logging
import re
from typing import Optional

from markdown_it import MarkdownIt
from markdown_it.tree import SyntaxTreeNode

import model.m3 as m3
from model.m3 import (
    Class, Package, Property, Association, Multiplicity,
    String, TaggedValue, Type, PrimitiveType,
)

_md_parser = MarkdownIt().enable("table")
_log = logging.getLogger(__name__)

_CLASS_HEADER_COLUMNS = {"Name", "Description"}
_PROPERTY_COLUMNS = {"Property", "Id", "Type", "Key", "Description"}
_ASSOCIATION_COLUMNS = {"Name", "Source", "Source Property Name", "Source Property ID", "Source Multiplicity", "Target", "Target Property Name", "Target Property ID", "Target Multiplicity", "Description"}
_CLASS_HEADING_RE = re.compile(r"^(.+?)(?:\s+extends\s+(.+))?$")


def _warn_unexpected_columns(found: list[str], expected: set[str], context: str) -> None:
    unexpected = set(found) - expected
    if unexpected:
        _log.warning("Unexpected columns in %s table (will be ignored): %s", context, sorted(unexpected))


def _parse_ast_table(node: SyntaxTreeNode) -> list[dict[str, str]]:
    thead, tbody = node.children[0], node.children[1]
    headers = [c.children[0].content for c in thead.children[0].children]
    rows = []
    for tr in tbody.children:
        cells = [c.children[0].content if c.children else "" for c in tr.children]
        rows.append({headers[i]: cells[i] if i < len(cells) else "" for i in range(len(headers))})
    return rows


def _resolve_type(type_str: str, classes_by_name: dict[str, Class]) -> Type:
    candidate = getattr(m3, type_str, None)
    if isinstance(candidate, PrimitiveType):
        return candidate
    if type_str in classes_by_name:
        return classes_by_name[type_str]
    return String


def _type_to_str(t: Type) -> str:
    if isinstance(t, (PrimitiveType, Class)):
        return t.name
    return "String"


# ---------------------------------------------------------------------------
# Load: markdown → m3
# ---------------------------------------------------------------------------

def load(path: str, known_classes: dict[str, Class] = None) -> list[Package]:
    with open(path, encoding="utf-8") as f:
        content = f.read()
    return loads(content, known_classes)


def loads(content: str, known_classes: dict[str, Class] = None) -> list[Package]:
    root = SyntaxTreeNode(_md_parser.parse(content))
    nodes = root.children

    packages: list[Package] = []
    classes_by_name: dict[str, Class] = dict(known_classes) if known_classes else {}
    current_package: Optional[Package] = None
    pending_superclasses: dict[str, list[str]] = {}  # class_name -> [superclass_name, ...]

    i = 0
    while i < len(nodes):
        node = nodes[i]

        if node.type == "heading":
            text = node.children[0].content if node.children else ""
            level = node.tag

            if level == "h2" and text.startswith("Sub-Domain:"):
                current_package = Package(text[len("Sub-Domain:"):].strip())
                packages.append(current_package)

            elif level == "h3" and text.startswith("Class:"):
                m = _CLASS_HEADING_RE.match(text[len("Class:"):].strip())
                class_name = m.group(1).strip()
                superclass_names = [s.strip() for s in m.group(2).split(",")] if m.group(2) else []
                i += 1

                # first table: class header
                description = ""
                if i < len(nodes) and nodes[i].type == "table":
                    rows = _parse_ast_table(nodes[i])
                    if rows:
                        _warn_unexpected_columns(list(rows[0].keys()), _CLASS_HEADER_COLUMNS, f"Class '{class_name}' header")
                        description = rows[0].get("Description", "")
                    i += 1

                tagged_values: list[TaggedValue] = []
                if description:
                    tagged_values.append(TaggedValue(TaggedValue.DOC, description))

                cls = Class(class_name, [], current_package, tagged_values=tagged_values or None)
                classes_by_name[cls.name] = cls
                if superclass_names:
                    pending_superclasses[class_name] = superclass_names

                # second table: properties
                if i < len(nodes) and nodes[i].type == "table":
                    prop_rows = _parse_ast_table(nodes[i])
                    if prop_rows:
                        _warn_unexpected_columns(list(prop_rows[0].keys()), _PROPERTY_COLUMNS, f"Class '{class_name}' properties")
                    for row in prop_rows:
                        label = row.get("Property", "").strip()
                        prop_id = row.get("Id", "").strip()
                        if not label and not prop_id:
                            continue
                        prop_id = prop_id or label
                        label = label or prop_id
                        type_str = row.get("Type", "String").strip()
                        is_key = row.get("Key", "").strip().upper() == "Y"
                        desc = row.get("Description", "").strip()
                        tagged: list[TaggedValue] = []
                        if is_key:
                            tagged.append(TaggedValue(TaggedValue.KEY, True))
                        if desc:
                            tagged.append(TaggedValue(TaggedValue.DOC, desc))
                        prop_type = _resolve_type(type_str, classes_by_name)
                        prop = Property(label, prop_id, prop_type, tagged or None)
                        cls.properties[prop.id] = prop
                    i += 1
                continue

            elif level == "h3" and text.startswith("Association:"):
                assoc_name = text[len("Association:"):].strip()
                i += 1
                if i < len(nodes) and nodes[i].type == "table":
                    rows = _parse_ast_table(nodes[i])
                    if rows:
                        _warn_unexpected_columns(list(rows[0].keys()), _ASSOCIATION_COLUMNS, f"Association '{assoc_name}'")
                        row = rows[0]
                        source = row.get("Source", "")
                        target = row.get("Target", "")
                        src_mult_str = row.get("Source Multiplicity", "").strip()
                        tgt_mult_str = row.get("Target Multiplicity", "").strip()
                        src_prop_name = row.get("Source Property Name", "").strip()
                        src_prop_id = row.get("Source Property ID", "").strip()
                        tgt_prop_name = row.get("Target Property Name", "").strip()
                        tgt_prop_id = row.get("Target Property ID", "").strip()
                        if src_mult_str not in (Multiplicity.ONE, Multiplicity.MANY):
                            raise ValueError(
                                f"Association '{assoc_name}' must specify Source Multiplicity ('1' or '*')"
                            )
                        if tgt_mult_str not in (Multiplicity.ONE, Multiplicity.MANY):
                            raise ValueError(
                                f"Association '{assoc_name}' must specify Target Multiplicity ('1' or '*')"
                            )
                        if not src_prop_id:
                            raise ValueError(
                                f"Association '{assoc_name}' must specify Source Property"
                            )
                        if not tgt_prop_id:
                            raise ValueError(
                                f"Association '{assoc_name}' must specify Target Property"
                            )
                        desc = row.get("Description", "").strip()
                        tagged = [TaggedValue(TaggedValue.DOC, desc)] if desc else None
                        Association(assoc_name, source, src_mult_str,
                                    src_prop_name, src_prop_id,
                                    target, tgt_mult_str,
                                    tgt_prop_name, tgt_prop_id,
                                    current_package, tagged)
                    i += 1
                continue

        i += 1

    # Second pass: resolve string type references to Class objects
    for cls in classes_by_name.values():
        for prop in cls.properties.values():
            if isinstance(prop.type, str) and prop.type in classes_by_name:
                prop.type = classes_by_name[prop.type]

    # Third pass: resolve superclass names to Class objects
    for cls_name, super_names in pending_superclasses.items():
        cls = classes_by_name.get(cls_name)
        if cls is None:
            continue
        for super_name in super_names:
            super_cls = classes_by_name.get(super_name)
            if super_cls:
                cls.superclasses.append(super_cls)
            else:
                _log.warning("Superclass '%s' not found for class '%s'", super_name, cls_name)

    # Fourth pass: validate that explicit class properties don't duplicate association navigation properties
    _validate_no_association_property_conflicts(packages, classes_by_name)

    # Fifth pass: register association-derived navigation properties on classes
    _register_association_properties(packages, classes_by_name)

    return packages


def _validate_no_association_property_conflicts(packages: list, classes_by_name: dict) -> None:
    from model.m3 import Association as _Association
    for pkg in packages:
        for child in pkg.children:
            if not isinstance(child, _Association):
                continue
            assoc = child
            # target_property is a forward navigation on the source class (source → target)
            # source_property is a reverse navigation on the target class (target → source)
            _check_conflict(assoc.source, assoc.target_property.id, assoc.name, classes_by_name, "target_property")
            _check_conflict(assoc.target, assoc.source_property.id, assoc.name, classes_by_name, "source_property")


def _check_conflict(class_name: str, prop_id: str, assoc_name: str, classes_by_name: dict, field: str) -> None:
    cls = classes_by_name.get(class_name)
    if cls is not None and prop_id in cls.properties:
        raise ValueError(
            f"Class '{class_name}' has a property '{prop_id}' that conflicts with "
            f"Association '{assoc_name}' {field}. Remove it from the class; "
            f"the association defines this navigation."
        )


def _register_association_properties(packages: list, classes_by_name: dict) -> None:
    from model.m3 import Association as _Association
    for pkg in packages:
        for child in pkg.children:
            if not isinstance(child, _Association):
                continue
            assoc = child
            source_cls = classes_by_name.get(assoc.source)
            target_cls = classes_by_name.get(assoc.target)
            if source_cls and target_cls:
                source_cls.properties_from_associations[assoc.target_property.id] = Property(
                    assoc.target_property.name, assoc.target_property.id, target_cls
                )
                target_cls.properties_from_associations[assoc.source_property.id] = Property(
                    assoc.source_property.name, assoc.source_property.id, source_cls
                )


# ---------------------------------------------------------------------------
# Save: m3 → markdown
# ---------------------------------------------------------------------------

def save(path: str, title: str, packages: list[Package]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(to_markdown(title, packages))


def to_markdown(title: str, packages: list[Package]) -> str:
    lines: list[str] = []
    lines.append(f"# {title}")
    lines.append("")

    for pkg in packages:
        lines.append(f"## Sub-Domain: {pkg.name}")
        lines.append("")

        for child in pkg.children:
            if isinstance(child, Class):
                heading = f"### Class: {child.name}"
                if child.superclasses:
                    heading += " extends " + ", ".join(s.name for s in child.superclasses)
                lines.append(heading)
                lines.append("")
                description = child.tagged_values.get(TaggedValue.DOC, TaggedValue("", "")).value or ""
                lines.append(_md_table(["Name", "Description"], [[child.name, description]]))
                lines.append("")

                prop_rows = []
                for prop in child.properties.values():
                    is_key = "Y" if TaggedValue.KEY in prop.tagged_values else ""
                    desc = prop.tagged_values.get(TaggedValue.DOC, TaggedValue("", "")).value or ""
                    prop_rows.append([prop.name, prop.id, _type_to_str(prop.type), is_key, desc])
                lines.append(_md_table(["Property", "Id", "Type", "Key", "Description"], prop_rows))
                lines.append("")

            elif isinstance(child, Association):
                lines.append(f"### Association: {child.name}")
                lines.append("")
                description = child.tagged_values.get(TaggedValue.DOC, TaggedValue("", "")).value or ""
                lines.append(_md_table(
                    ["Name", "Source", "Source Property Name", "Source Property ID", "Source Multiplicity",
                     "Target", "Target Property Name", "Target Property ID", "Target Multiplicity", "Description"],
                    [[child.name, child.source,
                      child.source_property.name, child.source_property.id, child.source_multiplicity or "",
                      child.target,
                      child.target_property.name, child.target_property.id, child.target_multiplicity or "",
                      description]],
                ))
                lines.append("")

    return "\n".join(lines)


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    def fmt_row(cells: list[str]) -> str:
        return "| " + " | ".join(c.ljust(col_widths[i]) for i, c in enumerate(cells)) + " |"

    separator = "| " + " | ".join("-" * w for w in col_widths) + " |"
    return "\n".join([fmt_row(headers), separator] + [fmt_row(r) for r in rows])
