import logging
import os
import re
from typing import Optional

from markdown_it import MarkdownIt
from markdown_it.tree import SyntaxTreeNode

from model.m3 import Class, PrimitiveType, Association, Property, DateTime
from model.mapping import Mapping, ProcessingDateMilestonesPropertyMapping, SingleBusinessDateMilestonePropertyMapping, \
    BusinessDateAndProcessingMilestonePropertyMapping, BiTemporalMilestonePropertyMapping
from model.relational import Repository, Schema, Table, MilestoningScheme, Column, ForeignKey
from model.relational_mapping import RelationalClassMapping, RelationalPropertyMapping, Join

_md_parser = MarkdownIt().enable("table")
_log = logging.getLogger(__name__)
_TABLE_RE = re.compile(r'(\S+)\s*→\s*(\w+)(?:\s*\(milestoning:\s*(\w+)\))?')


# ---------------------------------------------------------------------------
# Load: markdown → Mapping
# ---------------------------------------------------------------------------

def load(path: str, repository: Repository = None) -> Mapping:
    with open(path, encoding="utf-8") as f:
        content = f.read()
    base_dir = os.path.dirname(os.path.abspath(path))
    packages = _load_model_reference(content, base_dir)
    root = SyntaxTreeNode(_md_parser.parse(content))
    nodes = _expand_schema_includes(root.children, base_dir)
    if repository is None:
        repository = _build_repository_from_content(nodes)
        if repository is None:
            raise ValueError("No '## Repository:' section found in mapping markdown")
    return _loads_from_nodes(nodes, packages, repository)


def _expand_schema_includes(nodes: list, base_dir: str) -> list:
    """Replace '## Schema: <file>.md' nodes with the parsed nodes from that file."""
    expanded = []
    for node in nodes:
        if node.type == "heading" and node.tag == "h2":
            text = node.children[0].content if node.children else ""
            if text.startswith("Schema:") and text[len("Schema:"):].strip().endswith(".md"):
                filename = text[len("Schema:"):].strip()
                file_path = os.path.join(base_dir, filename)
                with open(file_path, encoding="utf-8") as f:
                    child_content = f.read()
                child_nodes = SyntaxTreeNode(_md_parser.parse(child_content)).children
                expanded.extend(_expand_schema_includes(child_nodes, os.path.dirname(file_path)))
                continue
        expanded.append(node)
    return expanded


def _load_model_reference(content: str, base_dir: str) -> list:
    from model_markdown.markdown_model import load as load_model
    root = SyntaxTreeNode(_md_parser.parse(content))
    packages = []
    known_classes = {}
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


def _build_repository_from_content(nodes: list) -> Repository:
    repo = None
    current_schema = None
    i = 0
    while i < len(nodes):
        node = nodes[i]
        if node.type == "heading":
            text = node.children[0].content if node.children else ""
            level = node.tag
            if level == "h2" and text.startswith("Repository:"):
                repo_name = text[len("Repository:"):].strip()
                repo = Repository(repo_name, "")
                current_schema = None
            elif level == "h3" and text.startswith("Schema:") and repo is not None:
                current_schema = Schema(text[len("Schema:"):].strip(), repo)
            elif level == "h4" and text.startswith("Table:") and current_schema is not None:
                m = _TABLE_RE.match(text[len("Table:"):].strip())
                if m:
                    table_name = m.group(1)
                    i += 1
                    columns = []
                    if i < len(nodes) and nodes[i].type == "table":
                        for row in _parse_ast_table(nodes[i]):
                            col_name = row.get("Column", "").strip()
                            col_type = row.get("Type", "").strip()
                            key = row.get("Key", "").strip().upper()
                            if col_name:
                                columns.append(Column(col_name, col_type or None, primary_key=(key == "PK")))
                    Table(table_name, columns, current_schema)
                    continue
        i += 1
    return repo


def loads(content: str, packages: list, repository: Repository = None) -> Mapping:
    root = SyntaxTreeNode(_md_parser.parse(content))
    nodes = root.children
    if repository is None:
        repository = _build_repository_from_content(nodes)
        if repository is None:
            raise ValueError("No '## Repository:' section found in mapping markdown")
    return _loads_from_nodes(nodes, packages, repository)


def _loads_from_nodes(nodes: list, packages: list, repository: Repository) -> Mapping:

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
                                business_date_from=row.get("business_date_from", "").strip() or None,
                                business_date_to=row.get("business_date_to", "").strip() or None,
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
                        if col is None:
                            _log.warning("Column '%s' not found in table '%s'", col_name, table_name)
                            continue
                        key = row.get("Key", "").strip().upper()
                        if key == "PK":
                            col.primary_key = True
                        prop = cls.properties.get(prop_name)
                        if prop is None:
                            prop = _synthetic_milestoning_property(prop_name, col_name, scheme_name, repository)
                            if prop is None:
                                _log.warning("Property '%s' not found in class '%s'", prop_name, cls.name)
                                continue
                        if isinstance(prop.type, PrimitiveType):
                            property_mappings.append(RelationalPropertyMapping(prop, col))
                        else:
                            deferred_joins[(table_name, col_name)] = (property_mappings, prop, col)
                    i += 1

                milestone_mapping = _build_milestone_mapping(scheme_name, property_mappings, repository)
                class_mappings.append(
                    RelationalClassMapping(cls, property_mappings, milestone_mapping=milestone_mapping)
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
                                        lhs_col.table.foreign_keys.append(ForeignKey(lhs_col, tgt_col))
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


def _synthetic_milestoning_property(prop_name: str, col_name: str, scheme_name: str, repository) -> Property:
    """Return a synthetic Property for a milestoning column not defined in the model, or None."""
    if not scheme_name or repository is None:
        return None
    scheme = next((s for s in repository.milestoning_schemes if s.name == scheme_name), None)
    if scheme is None:
        return None
    milestoning_cols = {
        scheme.processing_start, scheme.processing_end,
        scheme.business_date, scheme.business_date_from, scheme.business_date_to,
    } - {None}
    if col_name in milestoning_cols:
        return Property(prop_name, prop_name, DateTime)
    return None


def _build_milestone_mapping(scheme_name, property_mappings, repository):
    if not scheme_name:
        return None
    scheme = next((s for s in repository.milestoning_schemes if s.name == scheme_name), None)
    if scheme is None:
        return None
    pm_by_col = {pm.target.name: pm for pm in property_mappings if isinstance(pm.target, Column)}

    has_processing = scheme.processing_start and scheme.processing_end
    has_single_date = scheme.business_date and not scheme.business_date_from
    has_range_date = scheme.business_date_from and scheme.business_date_to

    if has_range_date and has_processing:
        _date_from = pm_by_col.get(scheme.business_date_from)
        _date_to = pm_by_col.get(scheme.business_date_to)
        _in = pm_by_col.get(scheme.processing_start)
        _out = pm_by_col.get(scheme.processing_end)
        if _date_from and _date_to and _in and _out:
            return BiTemporalMilestonePropertyMapping(_date_from, _date_to, _in, _out)
    elif has_single_date and has_processing:
        _date = pm_by_col.get(scheme.business_date)
        _in = pm_by_col.get(scheme.processing_start)
        _out = pm_by_col.get(scheme.processing_end)
        if _date and _in and _out:
            return BusinessDateAndProcessingMilestonePropertyMapping(_date, _in, _out)
    elif has_processing:
        _in = pm_by_col.get(scheme.processing_start)
        _out = pm_by_col.get(scheme.processing_end)
        if _in and _out:
            return ProcessingDateMilestonesPropertyMapping(_in, _out)
    elif has_single_date:
        _date = pm_by_col.get(scheme.business_date)
        if _date:
            return SingleBusinessDateMilestonePropertyMapping(_date)
    return None


def _milestone_scheme_name(rcm: RelationalClassMapping, repo: Repository) -> Optional[str]:
    mm = rcm.milestone_mapping
    if mm is None or repo is None:
        return None
    if isinstance(mm, BiTemporalMilestonePropertyMapping):
        date_from_col = mm._date_from.target.name if isinstance(mm._date_from.target, Column) else None
        date_to_col = mm._date_to.target.name if isinstance(mm._date_to.target, Column) else None
        in_col = mm._in.target.name if isinstance(mm._in.target, Column) else None
        out_col = mm._out.target.name if isinstance(mm._out.target, Column) else None
        for s in repo.milestoning_schemes:
            if s.business_date_from == date_from_col and s.business_date_to == date_to_col \
                    and s.processing_start == in_col and s.processing_end == out_col:
                return s.name
    elif isinstance(mm, BusinessDateAndProcessingMilestonePropertyMapping):
        date_col = mm._date.target.name if isinstance(mm._date.target, Column) else None
        in_col = mm._in.target.name if isinstance(mm._in.target, Column) else None
        out_col = mm._out.target.name if isinstance(mm._out.target, Column) else None
        for s in repo.milestoning_schemes:
            if s.business_date == date_col and s.processing_start == in_col \
                    and s.processing_end == out_col and not s.business_date_from:
                return s.name
    elif isinstance(mm, ProcessingDateMilestonesPropertyMapping):
        in_col = mm._in.target.name if isinstance(mm._in.target, Column) else None
        out_col = mm._out.target.name if isinstance(mm._out.target, Column) else None
        for s in repo.milestoning_schemes:
            if s.processing_start == in_col and s.processing_end == out_col \
                    and not s.business_date and not s.business_date_from:
                return s.name
    elif isinstance(mm, SingleBusinessDateMilestonePropertyMapping):
        date_col = mm._date.target.name if isinstance(mm._date.target, Column) else None
        for s in repo.milestoning_schemes:
            if s.business_date == date_col and not s.processing_start and not s.business_date_from:
                return s.name
    return None


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


def save(path: str, title: str, mapping: Mapping, model_paths: list[str] = None) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(to_markdown(title, mapping, model_paths))


def to_markdown(title: str, mapping: Mapping, model_paths: list[str] = None) -> str:
    lines: list[str] = [f"# {title}", ""]
    for mp in (model_paths or []):
        lines.append(f"## Model: {mp}")
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
                [s.name, s.processing_start or "", s.processing_end or "",
                 s.business_date or "", s.business_date_from or "", s.business_date_to or ""]
                for s in repo.milestoning_schemes
            ]
            lines.append(_md_table(
                ["Scheme", "processing_start", "processing_end", "business_date", "business_date_from", "business_date_to"],
                scheme_rows))
            lines.append("")

        for schema_name, rcms in repo_schema_map[repo_name].items():
            lines.append(f"### Schema: {schema_name}")
            lines.append("")

            for rcm in rcms:
                table = _primary_table(rcm)
                heading = f"#### Table: {table.name} → {rcm.clazz.name}"
                scheme_name = _milestone_scheme_name(rcm, repo)
                if scheme_name:
                    heading += f" (milestoning: {scheme_name})"
                lines.append(heading)
                lines.append("")

                fk_cols = {fk.column.name for fk in table.foreign_keys}
                col_rows = []
                for pm in rcm.property_mappings:
                    if isinstance(pm.target, Column):
                        key = "PK" if pm.target.primary_key else ""
                        col_rows.append([pm.target.name, pm.target.type or "", key, pm.property.id])
                    elif isinstance(pm.target, Join):
                        lhs = pm.target.lhs
                        key = "FK" if lhs.name in fk_cols else ""
                        col_rows.append([lhs.name, lhs.type or "", key, pm.property.id])
                lines.append(_md_table(["Column", "Type", "Key", "Property"], col_rows))
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


def draft_from_repository(title: str, repo: Repository) -> str:
    """Generate a draft mapping markdown with all columns populated and properties left blank."""
    lines: list[str] = [f"# {title}", ""]
    lines.append(f"## Repository: {repo.name}")
    lines.append("")

    if repo.milestoning_schemes:
        scheme_rows = [
            [s.name, s.processing_start or "", s.processing_end or "", s.business_date or ""]
            for s in repo.milestoning_schemes
        ]
        lines.append(_md_table(["Scheme", "processing_start", "processing_end", "business_date"], scheme_rows))
        lines.append("")

    for schema in repo.schemas:
        lines.append(f"### Schema: {schema.name}")
        lines.append("")
        for table in schema.tables:
            lines.append(f"#### Table: {table.name} → ?")
            lines.append("")
            col_rows = [
                [col.name, col.type or "", "PK" if col.primary_key else "", ""]
                for col in table.columns
            ]
            lines.append(_md_table(["Column", "Type", "Key", "Property"], col_rows))
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
