import logging
import os
import re
from typing import Optional

from markdown_it import MarkdownIt
from markdown_it.tree import SyntaxTreeNode

from model.m3 import Class, PrimitiveType
from model.mapping import Mapping
from model.graphql_mapping import (
    GraphQLEndpoint,
    GraphQLQuery,
    GraphQLField,
    GraphQLPropertyMapping,
    GraphQLClassMapping,
    GraphQLProcessingMilestone,
    GraphQLBusinessDateMilestone,
    GraphQLBiTemporalMilestone,
)

_md_parser = MarkdownIt().enable("table")
_log = logging.getLogger(__name__)

# "accounts → Account" or "instruments → Instrument (milestone: processing, asOf)"
# or "trades → Trade (milestone: bitemporal, businessDate, asOf)"
_QUERY_RE = re.compile(
    r'(\S+)\s*→\s*(\w+)'
    r'(?:\s*\(milestone:\s*(\w+)(?:,\s*(\w+))?(?:,\s*(\w+))?\))?'
)


# ---------------------------------------------------------------------------
# Load: markdown → Mapping
# ---------------------------------------------------------------------------

def load(path: str) -> Mapping:
    with open(path, encoding="utf-8") as f:
        content = f.read()
    base_dir = os.path.dirname(os.path.abspath(path))
    packages = _load_model_references(content, base_dir)
    root = SyntaxTreeNode(_md_parser.parse(content))
    return _loads_from_nodes(root.children, packages)


def loads(content: str, packages: list) -> Mapping:
    root = SyntaxTreeNode(_md_parser.parse(content))
    return _loads_from_nodes(root.children, packages)


def _load_model_references(content: str, base_dir: str) -> list:
    from model_markdown.markdown_model import load as load_model
    root = SyntaxTreeNode(_md_parser.parse(content))
    packages = []
    known_classes: dict = {}
    for node in root.children:
        if node.type == "heading" and node.tag == "h2":
            text = node.children[0].content if node.children else ""
            if text.startswith("Model:"):
                model_file = text[len("Model:"):].strip()
                model_path = os.path.join(base_dir, model_file)
                new_packages = load_model(model_path, known_classes=known_classes)
                packages.extend(new_packages)
                for pkg in new_packages:
                    for child in pkg.children:
                        if isinstance(child, Class):
                            known_classes[child.name] = child
    return packages


def _loads_from_nodes(nodes: list, packages: list) -> Mapping:
    classes_by_name = {
        child.name: child
        for pkg in packages
        for child in pkg.children
        if isinstance(child, Class)
    }

    title = "Mapping"
    current_endpoint: Optional[GraphQLEndpoint] = None
    class_mappings: list[GraphQLClassMapping] = []

    i = 0
    while i < len(nodes):
        node = nodes[i]

        if node.type == "heading":
            text = node.children[0].content if node.children else ""
            level = node.tag

            if level == "h1":
                title = text

            elif level == "h2" and text.startswith("Endpoint:"):
                current_endpoint = GraphQLEndpoint(text[len("Endpoint:"):].strip())

            elif level == "h3" and text.startswith("Query:"):
                if current_endpoint is None:
                    i += 1
                    continue
                m = _QUERY_RE.match(text[len("Query:"):].strip())
                if m is None:
                    i += 1
                    continue

                query_name = m.group(1)
                class_name = m.group(2)
                milestone = _parse_milestone(m.group(3), m.group(4), m.group(5))

                cls = classes_by_name.get(class_name)
                if cls is None:
                    _log.warning("Class '%s' not found in model", class_name)
                    i += 1
                    continue

                query = GraphQLQuery(query_name, current_endpoint, milestone)
                property_mappings: list[GraphQLPropertyMapping] = []
                i += 1

                if i < len(nodes) and nodes[i].type == "table":
                    all_props = cls.all_properties()
                    for row in _parse_table(nodes[i]):
                        field_name = row.get("Field", "").strip()
                        prop_name = row.get("Property", "").strip()
                        if not field_name or not prop_name:
                            continue
                        prop = all_props.get(prop_name)
                        if prop is None:
                            _log.warning("Property '%s' not found in class '%s'", prop_name, class_name)
                            continue
                        property_mappings.append(GraphQLPropertyMapping(prop, GraphQLField(field_name)))
                    i += 1

                class_mappings.append(GraphQLClassMapping(cls, property_mappings, query))
                continue

        i += 1

    return Mapping(title, class_mappings)


def _parse_milestone(milestone_type: str, arg1: str, arg2: str):
    if milestone_type is None:
        return None
    if milestone_type == "processing":
        return GraphQLProcessingMilestone(argument_name=arg1 or "asOf")
    if milestone_type == "business_date":
        return GraphQLBusinessDateMilestone(argument_name=arg1 or "businessDate")
    if milestone_type == "bitemporal":
        return GraphQLBiTemporalMilestone(
            business_date_argument=arg1 or "businessDate",
            processing_argument=arg2 or "asOf",
        )
    _log.warning("Unknown milestone type '%s'", milestone_type)
    return None


def _parse_table(node: SyntaxTreeNode) -> list[dict]:
    thead, tbody = node.children[0], node.children[1]
    headers = [c.children[0].content for c in thead.children[0].children]
    rows = []
    for tr in tbody.children:
        cells = [c.children[0].content if c.children else "" for c in tr.children]
        rows.append({headers[j]: cells[j] if j < len(cells) else "" for j in range(len(headers))})
    return rows


# ---------------------------------------------------------------------------
# Save: Mapping → markdown
# ---------------------------------------------------------------------------

def save(path: str, title: str, mapping: Mapping, model_paths: list[str] = None) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(to_markdown(title, mapping, model_paths))


def to_markdown(title: str, mapping: Mapping, model_paths: list[str] = None) -> str:
    lines: list[str] = [f"# {title}", ""]
    for mp in (model_paths or []):
        lines.append(f"## Model: {mp}")
        lines.append("")

    # Group class mappings by endpoint URL, preserving insertion order
    endpoints_seen: list[str] = []
    endpoint_objs: dict[str, GraphQLEndpoint] = {}
    endpoint_mappings: dict[str, list[GraphQLClassMapping]] = {}

    for cm in mapping.mappings:
        if not isinstance(cm, GraphQLClassMapping):
            continue
        url = cm.query.endpoint.url
        if url not in endpoint_mappings:
            endpoints_seen.append(url)
            endpoint_objs[url] = cm.query.endpoint
            endpoint_mappings[url] = []
        endpoint_mappings[url].append(cm)

    for url in endpoints_seen:
        lines.append(f"## Endpoint: {url}")
        lines.append("")

        for cm in endpoint_mappings[url]:
            heading = f"### Query: {cm.query.name} → {cm.clazz.name}"
            ms = cm.query.milestone
            if ms is not None:
                heading += _milestone_suffix(ms)
            lines.append(heading)
            lines.append("")

            field_rows = [
                [pm.target.name, pm.property.id]
                for pm in cm.property_mappings
                if isinstance(pm.target, GraphQLField)
            ]
            lines.append(_md_table(["Field", "Property"], field_rows))
            lines.append("")

    return "\n".join(lines)


def _milestone_suffix(ms) -> str:
    if isinstance(ms, GraphQLBiTemporalMilestone):
        return f" (milestone: bitemporal, {ms.business_date_argument}, {ms.processing_argument})"
    if isinstance(ms, GraphQLBusinessDateMilestone):
        return f" (milestone: business_date, {ms.argument_name})"
    if isinstance(ms, GraphQLProcessingMilestone):
        return f" (milestone: processing, {ms.argument_name})"
    return ""


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    def fmt_row(cells: list[str]) -> str:
        return "| " + " | ".join(str(c).ljust(col_widths[i]) for i, c in enumerate(cells)) + " |"

    separator = "| " + " | ".join("-" * w for w in col_widths) + " |"
    return "\n".join([fmt_row(headers), separator] + [fmt_row(r) for r in rows])
