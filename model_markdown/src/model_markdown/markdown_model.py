import re
from typing import Optional

from model.m3 import (
    Class, Package, Property, Association,
    String, Integer, Float, Date, DateTime, Boolean,
    TaggedValue, Type, PrimitiveType,
)

_TYPE_MAP: dict[str, Type] = {
    "string": String,
    "integer": Integer,
    "int": Integer,
    "float": Float,
    "double": Float,
    "boolean": Boolean,
    "bool": Boolean,
    "date": Date,
    "datetime": DateTime,
}

_TYPE_NAMES = {
    id(String): "String",
    id(Integer): "Integer",
    id(Float): "Float",
    id(Date): "Date",
    id(DateTime): "DateTime",
    id(Boolean): "Boolean",
}

TAG_KEY = "key"


def _parse_table(lines: list[str]) -> list[dict[str, str]]:
    """Parse a markdown table into a list of dicts keyed by header."""
    rows = [l for l in lines if l.startswith("|") and not re.match(r"^\|[-| :]+\|$", l.strip())]
    if len(rows) < 2:
        return []
    headers = [h.strip() for h in rows[0].strip("|").split("|")]
    result = []
    for row in rows[1:]:
        values = [v.strip() for v in row.strip("|").split("|")]
        result.append({headers[i]: values[i] if i < len(values) else "" for i in range(len(headers))})
    return result


def _resolve_type(type_str: str, classes_by_name: dict[str, Class]) -> Type:
    t = _type_map_lookup(type_str)
    if t is not None:
        return t
    if type_str in classes_by_name:
        return classes_by_name[type_str]
    return String


def _type_map_lookup(type_str: str) -> Optional[Type]:
    return _TYPE_MAP.get(type_str.lower())


def _type_to_str(t: Type, classes_by_name: dict[str, Class]) -> str:
    name = _TYPE_NAMES.get(id(t))
    if name:
        return name
    if isinstance(t, Class):
        return t.name
    return "String"


def _table_rows(lines: list[str], start: int) -> tuple[list[str], int]:
    """Collect consecutive table lines starting at index start."""
    rows = []
    i = start
    while i < len(lines) and (lines[i].startswith("|") or lines[i].strip() == ""):
        if lines[i].startswith("|"):
            rows.append(lines[i])
        elif rows:
            break
        i += 1
    return rows, i


# ---------------------------------------------------------------------------
# Load: markdown → m3
# ---------------------------------------------------------------------------

def load(path: str) -> tuple[list[Package], list[Class], list[Association]]:
    with open(path, encoding="utf-8") as f:
        content = f.read()
    return loads(content)


def loads(content: str) -> tuple[list[Package], list[Class], list[Association]]:
    lines = content.splitlines()
    packages: list[Package] = []
    classes: list[Class] = []
    associations: list[Association] = []
    classes_by_name: dict[str, Class] = {}

    current_package: Optional[Package] = None
    i = 0

    while i < len(lines):
        line = lines[i].rstrip()

        # Sub-Domain
        m = re.match(r"^## Sub-Domain:\s*(.+)$", line)
        if m:
            current_package = Package(m.group(1).strip())
            packages.append(current_package)
            i += 1
            continue

        # Class
        m = re.match(r"^### Class:\s*(.+)$", line)
        if m:
            i += 1
            # first table = class header (name + description)
            table_lines, i = _table_rows(lines, i)
            rows = _parse_table(table_lines)
            description = rows[0].get("Description", "") if rows else ""

            # second table = properties
            table_lines, i = _table_rows(lines, i)
            prop_rows = _parse_table(table_lines)

            properties: list[Property] = []
            for row in prop_rows:
                name = row.get("Property", "").strip()
                if not name:
                    continue
                type_str = row.get("Type", "String").strip()
                is_key = row.get("Key", "").strip().upper() == "Y"
                desc = row.get("Description", "").strip()

                tagged: list[TaggedValue] = []
                if is_key:
                    tagged.append(TaggedValue(TAG_KEY, True))
                if desc:
                    tagged.append(TaggedValue(TaggedValue.DOC, desc))

                prop_type = _type_map_lookup(type_str) or type_str
                properties.append(Property(name, prop_type, tagged or None))

            tagged_values: list[TaggedValue] = []
            if description:
                tagged_values.append(TaggedValue(TaggedValue.DOC, description))

            cls = Class(m.group(1).strip(), properties, current_package, tagged_values or None)
            classes.append(cls)
            classes_by_name[cls.name] = cls
            continue

        # Association
        m = re.match(r"^### Association:\s*(.+)$", line)
        if m:
            assoc_name = m.group(1).strip()
            i += 1
            table_lines, i = _table_rows(lines, i)
            rows = _parse_table(table_lines)
            if rows:
                row = rows[0]
                source = row.get("Source", "")
                target = row.get("Target", "")
                description = row.get("Description", "").strip()
                tagged: list[TaggedValue] = []
                if description:
                    tagged.append(TaggedValue(TaggedValue.DOC, description))
                associations.append(Association(assoc_name, source, target, current_package, tagged or None))
            continue

        i += 1

    # Second pass: resolve string type references to Class objects
    for cls in classes:
        for prop in cls.properties.values():
            if isinstance(prop.type, str) and prop.type in classes_by_name:
                prop.type = classes_by_name[prop.type]

    return packages, classes, associations


# ---------------------------------------------------------------------------
# Save: m3 → markdown
# ---------------------------------------------------------------------------

def save(path: str, title: str, classes: list[Class], associations: list[Association]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(dumps(title, classes, associations))


def dumps(title: str, classes: list[Class], associations: list[Association]) -> str:
    lines: list[str] = []
    lines.append(f"# {title}")
    lines.append("")

    # Group by package
    packages: dict[str, tuple[list[Class], list[Association]]] = {}
    for cls in classes:
        pkg = cls.package.name if cls.package else "default"
        packages.setdefault(pkg, ([], []))
        packages[pkg][0].append(cls)

    for assoc in associations:
        pkg = assoc.package.name if assoc.package else "default"
        packages.setdefault(pkg, ([], []))
        packages[pkg][1].append(assoc)

    classes_by_name = {cls.name: cls for cls in classes}

    for pkg_name, (pkg_classes, pkg_assocs) in packages.items():
        lines.append(f"## Sub-Domain: {pkg_name}")
        lines.append("")

        for cls in pkg_classes:
            lines.append(f"### Class: {cls.name}")
            lines.append("")
            description = cls.tagged_values.get(TaggedValue.DOC, TaggedValue("", "")).value or ""
            lines.append(_md_table(["Name", "Description"], [[cls.name, description]]))
            lines.append("")

            prop_rows = []
            for prop in cls.properties.values():
                type_str = _type_to_str(prop.type, classes_by_name)
                is_key = "Y" if TAG_KEY in prop.tagged_values else ""
                desc = prop.tagged_values.get(TaggedValue.DOC, TaggedValue("", "")).value or ""
                prop_rows.append([prop.name, type_str, is_key, desc])

            lines.append(_md_table(["Property", "Type", "Key", "Description"], prop_rows))
            lines.append("")

        for assoc in pkg_assocs:
            lines.append(f"### Association: {assoc.name}")
            lines.append("")
            description = assoc.tagged_values.get(TaggedValue.DOC, TaggedValue("", "")).value or ""
            lines.append(_md_table(
                ["Name", "Source", "Target", "Description"],
                [[assoc.name, assoc.source, assoc.target, description]],
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
