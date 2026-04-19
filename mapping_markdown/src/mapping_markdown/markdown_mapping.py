import logging
import os
import re
from typing import Optional

from markdown_it import MarkdownIt
from markdown_it.tree import SyntaxTreeNode

from model.m3 import Class, PrimitiveType, Association
from model.mapping import Mapping
from model.relational import Repository, MilestoningScheme, Column
from model.relational_mapping import RelationalClassMapping, RelationalPropertyMapping, Join

_md_parser = MarkdownIt().enable("table")
_log = logging.getLogger(__name__)
_TABLE_RE = re.compile(r'(\S+)\s*→\s*(\w+)(?:\s*\(milestoning:\s*(\w+)\))?')


# ---------------------------------------------------------------------------
# Load: markdown → Mapping
# ---------------------------------------------------------------------------

def load(path: str, repository: Repository) -> Mapping:
    with open(path, encoding="utf-8") as f:
        content = f.read()
    base_dir = os.path.dirname(os.path.abspath(path))
    packages = _load_model_reference(content, base_dir)
    return loads(content, packages, repository)


def _load_model_reference(content: str, base_dir: str) -> list:
    from model_markdown.markdown_model import load as load_model
    root = SyntaxTreeNode(_md_parser.parse(content))
    for node in root.children:
        if node.type == "heading" and node.tag == "h2":
            text = node.children[0].content if node.children else ""
            if text.startswith("Model:"):
                model_file = text[len("Model:"):].strip()
                model_path = os.path.join(base_dir, model_file)
                return load_model(model_path)
    return []


def loads(content: str, packages: list, repository: Repository) -> Mapping:
    root = SyntaxTreeNode(_md_parser.parse(content))
    nodes = root.children

    classes_by_name = {
        child.name: child
        for pkg in packages
        for child in pkg.children
        if isinstance(child, Class)
    }
    tables_by_name = {
        table.name: table
        for schema in repository.schemas
        for table in schema.tables
    }

    title = "Mapping"
    class_mappings: list[RelationalClassMapping] = []
    # (table_name, col_name) -> (property_mappings_list, prop, lhs_col)
    deferred_joins: dict = {}

    i = 0
    while i < len(nodes):
        node = nodes[i]

        if node.type == "heading":
            text = node.children[0].content if node.children else ""
            level = node.tag

            if level == "h1":
                title = text

            elif level == "h2" and text.startswith("Repository:"):
                i += 1
                if i < len(nodes) and nodes[i].type == "table":
                    for row in _parse_ast_table(nodes[i]):
                        name = row.get("Scheme", "").strip()
                        if name:
                            repository.milestoning_schemes.append(MilestoningScheme(
                                name=name,
                                processing_start=row.get("processing_start", "").strip() or None,
                                processing_end=row.get("processing_end", "").strip() or None,
                                business_date=row.get("business_date", "").strip() or None,
                            ))
                    i += 1
                continue

            elif level == "h4" and text.startswith("Table:"):
                m = _TABLE_RE.match(text[len("Table:"):].strip())
                if m is None:
                    i += 1
                    continue
                table_name, class_name, scheme_name = m.group(1), m.group(2), m.group(3)

                cls = classes_by_name.get(class_name)
                table = tables_by_name.get(table_name)
                if cls is None or table is None:
                    _log.warning("Table '%s' or class '%s' not found", table_name, class_name)
                    i += 1
                    continue

                cols_by_name = {col.name: col for col in table.columns}
                property_mappings: list[RelationalPropertyMapping] = []
                i += 1

                if i < len(nodes) and nodes[i].type == "table":
                    for row in _parse_ast_table(nodes[i]):
                        col_name = row.get("Column", "").strip()
                        prop_name = row.get("Property", "").strip()
                        if not col_name or not prop_name:
                            continue
                        col = cols_by_name.get(col_name)
                        prop = cls.properties.get(prop_name)
                        if col is None or prop is None:
                            _log.warning("Column '%s' or property '%s' not found", col_name, prop_name)
                            continue
                        if isinstance(prop.type, PrimitiveType):
                            property_mappings.append(RelationalPropertyMapping(prop, col))
                        else:
                            deferred_joins[(table_name, col_name)] = (property_mappings, prop, col)
                    i += 1

                # optional milestoning override table (Scheme | Milestoning | Column)
                if i < len(nodes) and nodes[i].type == "table":
                    rows = _parse_ast_table(nodes[i])
                    if rows and "Scheme" in rows[0] and "Milestoning" in rows[0]:
                        i += 1  # consumed but not yet acted on

                class_mappings.append(
                    RelationalClassMapping(cls, property_mappings, milestoning_scheme=scheme_name)
                )
                continue

            elif level == "h4" and text.startswith("Association:"):
                assoc_name = text[len("Association:"):].strip()
                i += 1
                if i < len(nodes) and nodes[i].type == "table":
                    for row in _parse_ast_table(nodes[i]):
                        src_col = row.get("Source Column", "").strip()
                        tgt_table_name = row.get("Target Table", "").strip()
                        tgt_col_name = row.get("Target Column", "").strip()
                        for key in list(deferred_joins.keys()):
                            if key[1] == src_col:
                                pm_list, prop, lhs_col = deferred_joins.pop(key)
                                tgt_table = tables_by_name.get(tgt_table_name)
                                if tgt_table:
                                    tgt_col = next((c for c in tgt_table.columns if c.name == tgt_col_name), None)
                                    if tgt_col:
                                        pm_list.append(RelationalPropertyMapping(prop, Join(lhs_col, tgt_col)))
                                break
                    i += 1
                continue

        i += 1

    return Mapping(title, class_mappings)


def _parse_ast_table(node: SyntaxTreeNode) -> list[dict]:
    thead, tbody = node.children[0], node.children[1]
    headers = [c.children[0].content for c in thead.children[0].children]
    rows = []
    for tr in tbody.children:
        cells = [c.children[0].content if c.children else "" for c in tr.children]
        rows.append({headers[i]: cells[i] if i < len(cells) else "" for i in range(len(headers))})
    return rows


# ---------------------------------------------------------------------------
# Save: Mapping → markdown
# ---------------------------------------------------------------------------

def _find_association_name(source_cls: Class, target_cls: Class) -> str:
    pkg = source_cls.package
    if pkg is not None:
        for child in pkg.children:
            if isinstance(child, Association) and child.source == source_cls.name and child.target == target_cls.name:
                return child.name
    return f"{source_cls.name}{target_cls.name}"


def save(path: str, title: str, mapping: Mapping, model_path: str = None) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(to_markdown(title, mapping, model_path))


def to_markdown(title: str, mapping: Mapping, model_path: str = None) -> str:
    lines: list[str] = [f"# {title}", ""]
    if model_path:
        lines.append(f"## Model: {model_path}")
        lines.append("")

    # Group by repo → schema, preserving insertion order
    repos_seen: list[str] = []
    repo_objs: dict = {}
    repo_schema_map: dict = {}

    for rcm in mapping.mappings:
        table = _primary_table(rcm)
        if table is None:
            continue
        schema = table.schema
        repo = schema.repository if schema else None
        repo_name = repo.name if repo else "(none)"

        if repo_name not in repo_schema_map:
            repos_seen.append(repo_name)
            repo_objs[repo_name] = repo
            repo_schema_map[repo_name] = {}

        schema_name = schema.name if schema else "(none)"
        if schema_name not in repo_schema_map[repo_name]:
            repo_schema_map[repo_name][schema_name] = []
        repo_schema_map[repo_name][schema_name].append(rcm)

    for repo_name in repos_seen:
        repo = repo_objs[repo_name]
        lines.append(f"## Repository: {repo_name}")
        lines.append("")

        if repo and repo.milestoning_schemes:
            scheme_rows = [
                [s.name, s.processing_start or "", s.processing_end or "", s.business_date or ""]
                for s in repo.milestoning_schemes
            ]
            lines.append(_md_table(["Scheme", "processing_start", "processing_end", "business_date"], scheme_rows))
            lines.append("")

        for schema_name, rcms in repo_schema_map[repo_name].items():
            lines.append(f"### Schema: {schema_name}")
            lines.append("")

            for rcm in rcms:
                table = _primary_table(rcm)
                heading = f"#### Table: {table.name} → {rcm.clazz.name}"
                if rcm.milestoning_scheme:
                    heading += f" (milestoning: {rcm.milestoning_scheme})"
                lines.append(heading)
                lines.append("")

                col_rows = []
                for pm in rcm.property_mappings:
                    if isinstance(pm.target, Column):
                        col_rows.append([pm.target.name, pm.property.name])
                    elif isinstance(pm.target, Join):
                        col_rows.append([pm.target.lhs.name, pm.property.name])
                lines.append(_md_table(["Column", "Property"], col_rows))
                lines.append("")

            for rcm in rcms:
                for pm in rcm.property_mappings:
                    if isinstance(pm.target, Join):
                        assoc_name = _find_association_name(rcm.clazz, pm.property.type)
                        lines.append(f"#### Association: {assoc_name}")
                        lines.append("")
                        lines.append(_md_table(
                            ["Source Column", "Target Table", "Target Column"],
                            [[pm.target.lhs.name, pm.target.rhs.table.name, pm.target.rhs.name]],
                        ))
                        lines.append("")

    return "\n".join(lines)


def _primary_table(rcm: RelationalClassMapping) -> Optional[Column]:
    for pm in rcm.property_mappings:
        if isinstance(pm.target, Column) and pm.target.table is not None:
            return pm.target.table
    return None


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    def fmt_row(cells: list[str]) -> str:
        return "| " + " | ".join(c.ljust(col_widths[i]) for i, c in enumerate(cells)) + " |"

    separator = "| " + " | ".join("-" * w for w in col_widths) + " |"
    return "\n".join([fmt_row(headers), separator] + [fmt_row(r) for r in rows])
