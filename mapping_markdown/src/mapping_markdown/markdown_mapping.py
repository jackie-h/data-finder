import logging
import os
import re
from typing import Optional

from markdown_it import MarkdownIt
from markdown_it.tree import SyntaxTreeNode

from model.m3 import Class, PrimitiveType, Association, Property, DateTime
from model.mapping import Mapping, ProcessingDateMilestonesPropertyMapping, SingleBusinessDateMilestonePropertyMapping, \
    BusinessDateAndProcessingMilestonePropertyMapping, BiTemporalMilestonePropertyMapping
from model.relational import DataStore, Database, DataCatalog, Schema, Table, MilestoningScheme, Column, ForeignKey
from model.relational_mapping import RelationalClassMapping, RelationalPropertyMapping, Join

_md_parser = MarkdownIt().enable("table")
_log = logging.getLogger(__name__)
_TABLE_RE = re.compile(r'(\S+)\s*→\s*(\w+)(?:\s*\(milestoning:\s*(\w+)\))?')


# ---------------------------------------------------------------------------
# Load: markdown → Mapping
# ---------------------------------------------------------------------------

def load(path: str, datastore: DataStore = None) -> Mapping:
    with open(path, encoding="utf-8") as f:
        content = f.read()
    base_dir = os.path.dirname(os.path.abspath(path))
    packages = _load_model_reference(content, base_dir)
    root = SyntaxTreeNode(_md_parser.parse(content))
    nodes = _expand_schema_includes(root.children, base_dir)
    if datastore is None:
        datastore = _build_repository_from_content(nodes)
        if datastore is None:
            raise ValueError("No '## DataStore:' section found in mapping markdown")
    return _loads_from_nodes(nodes, packages, datastore)


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


def _build_repository_from_content(nodes: list) -> DataStore:
    repo = None
    current_schema = None
    i = 0
    while i < len(nodes):
        node = nodes[i]
        if node.type == "heading":
            text = node.children[0].content if node.children else ""
            level = node.tag
            if level == "h2" and text.startswith("DataStore:"):
                heading_text = text[len("DataStore:"):].strip()
                catalog_match = re.match(r'^(.+?)\s*\(Catalog\)\s*$', heading_text)
                database_match = re.match(r'^(.+?)\s*\(Database\)\s*$', heading_text)
                if catalog_match:
                    repo = DataCatalog(catalog_match.group(1))
                elif database_match:
                    repo = Database(database_match.group(1), "")
                else:
                    raise ValueError(
                        f"DataStore '{heading_text}' must specify a type: (Database) or (Catalog)"
                    )
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
                        for row in _parse_ast_table(nodes[i], _COLUMN_MAPPING_HEADERS):
                            col_name = row.get("Column", "").strip()
                            col_type = row.get("Type", "").strip()
                            key = row.get("Key", "").strip().upper()
                            if col_name:
                                columns.append(Column(col_name, col_type or None, primary_key=(key == "PK")))
                    Table(table_name, columns, current_schema)
                    continue
        i += 1
    return repo


def loads(content: str, packages: list, datastore: DataStore = None) -> Mapping:
    root = SyntaxTreeNode(_md_parser.parse(content))
    nodes = root.children
    if datastore is None:
        datastore = _build_repository_from_content(nodes)
        if datastore is None:
            raise ValueError("No '## DataStore:' section found in mapping markdown")
    return _loads_from_nodes(nodes, packages, datastore)


def _build_assoc_nav_lookup(packages: list) -> dict:
    """Build (source_class_name, target_property_id) -> target_Class from all associations."""
    result = {}
    for pkg in packages:
        for child in pkg.children:
            if isinstance(child, Association):
                target_cls_name = child.target
                # resolved later; we store the name and resolve after classes_by_name is built
                result[(child.source, child.target_property.id)] = child.target
    return result


def _loads_from_nodes(nodes: list, packages: list, repository: DataStore) -> Mapping:

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
    # (source_class_name, target_property_id) -> target class name
    assoc_nav_lookup = _build_assoc_nav_lookup(packages)

    title = "Mapping"
    class_mappings: list[RelationalClassMapping] = []

    # class_name → (property_mappings, cols_by_name) built as Table sections are processed.
    # Association sections use this to resolve joins without relying on ordering.
    class_context: dict[str, tuple] = {}

    i = 0
    while i < len(nodes):
        node = nodes[i]

        if node.type == "heading":
            text = node.children[0].content if node.children else ""
            level = node.tag

            if level == "h1":
                title = text

            elif level == "h2" and text.startswith("DataStore:"):
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
                                infinite_datetime=row.get("infinite_datetime", "").strip() or None,
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
                all_props = cls.all_properties()
                property_mappings: list[RelationalPropertyMapping] = []
                i += 1

                if i < len(nodes) and nodes[i].type == "table":
                    for row in _parse_ast_table(nodes[i], _COLUMN_MAPPING_HEADERS):
                        col_name = row.get("Column", "").strip()
                        prop_name = row.get("Property ID", "").strip()
                        if not col_name or not prop_name:
                            continue
                        col = cols_by_name.get(col_name)
                        if col is None:
                            _log.warning("Column '%s' not found in table '%s'", col_name, table_name)
                            continue
                        key = row.get("Key", "").strip().upper()
                        if key == "PK":
                            col.primary_key = True
                        prop = all_props.get(prop_name)
                        if prop is None:
                            prop = _synthetic_milestoning_property(prop_name, col_name, scheme_name, repository)
                        if prop is None:
                            target_cls_name = assoc_nav_lookup.get((cls.name, prop_name))
                            if target_cls_name is not None:
                                target_cls = classes_by_name.get(target_cls_name)
                                if target_cls is not None:
                                    from model.m3 import Property as _Property
                                    prop = _Property(prop_name, prop_name, target_cls)
                        if prop is None:
                            _log.warning("Property '%s' not found in class '%s'", prop_name, cls.name)
                            continue
                        if isinstance(prop.type, PrimitiveType):
                            property_mappings.append(RelationalPropertyMapping(prop, col))
                        # non-primitive (association) properties are resolved in the Association section
                    i += 1

                milestone_mapping = _build_milestone_mapping(scheme_name, property_mappings, repository,
                                                              table_name=table_name, class_name=class_name)
                class_mappings.append(
                    RelationalClassMapping(cls, property_mappings, milestone_mapping=milestone_mapping)
                )
                class_context[cls.name] = (property_mappings, cols_by_name)
                continue

            elif level == "h4" and text.startswith("Association:"):
                assoc_name = text[len("Association:"):].strip()
                i += 1

                # Find the Association in the model to determine source class and nav property.
                assoc_def = None
                for pkg in packages:
                    for child in pkg.children:
                        if isinstance(child, Association) and child.name == assoc_name:
                            assoc_def = child
                            break
                    if assoc_def:
                        break

                if assoc_def is None:
                    _log.warning("Association '%s': not found in model packages", assoc_name)
                    if i < len(nodes) and nodes[i].type == "table":
                        i += 1
                    continue

                src_cls_name = assoc_def.source
                nav_prop_id = assoc_def.target_property.id
                pm_list, cols_by_name = class_context.get(src_cls_name, (None, {}))
                target_cls_name = assoc_nav_lookup.get((src_cls_name, nav_prop_id))
                target_cls = classes_by_name.get(target_cls_name) if target_cls_name else None

                if pm_list is None:
                    _log.warning(
                        "Association '%s': source class '%s' has no processed Table section",
                        assoc_name, src_cls_name,
                    )
                    if i < len(nodes) and nodes[i].type == "table":
                        i += 1
                    continue

                if i < len(nodes) and nodes[i].type == "table":
                    for row in _parse_ast_table(nodes[i], _ASSOCIATION_HEADERS):
                        src_col = row.get("Source Column", "").strip()
                        tgt_table_name = row.get("Target Table", "").strip()
                        tgt_col_name = row.get("Target Column", "").strip()

                        lhs_col = cols_by_name.get(src_col)
                        tgt_table = tables_by_name.get(tgt_table_name)

                        if lhs_col is None:
                            _log.warning(
                                "Association '%s': source column '%s' not found in table for class '%s'",
                                assoc_name, src_col, src_cls_name,
                            )
                            continue
                        if tgt_table is None:
                            _log.warning(
                                "Association '%s': target table '%s' not found",
                                assoc_name, tgt_table_name,
                            )
                            continue

                        tgt_col = next((c for c in tgt_table.columns if c.name == tgt_col_name), None)

                        if tgt_col and nav_prop_id and target_cls:
                            prop = Property(assoc_def.target_property.name, nav_prop_id, target_cls)
                            pm_list.append(RelationalPropertyMapping(prop, Join(lhs_col, tgt_col)))
                            lhs_col.table.foreign_keys.append(ForeignKey(lhs_col, tgt_col))
                        else:
                            _log.warning(
                                "Association '%s': could not resolve navigation property "
                                "for source class '%s'",
                                assoc_name, src_cls_name,
                            )
                    i += 1
                continue

        i += 1

    return Mapping(title, class_mappings)


_COLUMN_MAPPING_HEADERS = ["Column", "Type", "Key", "Property ID"]
_ASSOCIATION_HEADERS = ["Source Column", "Target Table", "Target Column"]


def _parse_ast_table(node: SyntaxTreeNode, expected_headers: list[str] = None) -> list[dict]:
    thead, tbody = node.children[0], node.children[1]
    headers = [c.children[0].content for c in thead.children[0].children]
    if expected_headers is not None and headers != expected_headers:
        raise ValueError(
            f"Mapping table has unexpected headers {headers!r} — expected {expected_headers!r}"
        )
    rows = []
    for tr in tbody.children:
        cells = [c.children[0].content if c.children else "" for c in tr.children]
        rows.append({headers[i]: cells[i] if i < len(cells) else "" for i in range(len(headers))})
    return rows


def _camel_to_display_name(s: str) -> str:
    """Convert a camelCase identifier to a friendly display name: 'validFrom' → 'Valid From'."""
    result = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1 \2', s)
    result = re.sub(r'([a-z\d])([A-Z])', r'\1 \2', result)
    return result[0].upper() + result[1:]


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
        return Property(_camel_to_display_name(prop_name), prop_name, DateTime)
    return None


def _build_milestone_mapping(scheme_name, property_mappings, repository, table_name: str, class_name: str):
    if not scheme_name:
        return None
    scheme = next((s for s in repository.milestoning_schemes if s.name == scheme_name), None)
    if scheme is None:
        raise ValueError(
            f"Table '{table_name}' → '{class_name}': "
            f"milestoning scheme '{scheme_name}' not found in DataStore"
        )
    pm_by_col = {pm.target.name: pm for pm in property_mappings if isinstance(pm.target, Column)}

    def _require(col_name):
        pm = pm_by_col.get(col_name)
        if pm is None:
            raise ValueError(
                f"Table '{table_name}' → '{class_name}' uses milestoning scheme '{scheme_name}' "
                f"but is missing required column '{col_name}' in its property mappings"
            )
        return pm

    has_processing = scheme.processing_start and scheme.processing_end
    has_single_date = scheme.business_date and not scheme.business_date_from
    has_range_date = scheme.business_date_from and scheme.business_date_to

    if has_range_date and has_processing:
        return BiTemporalMilestonePropertyMapping(
            _require(scheme.business_date_from), _require(scheme.business_date_to),
            _require(scheme.processing_start), _require(scheme.processing_end),
            infinite_datetime=scheme.infinite_datetime,
        )
    elif has_single_date and has_processing:
        return BusinessDateAndProcessingMilestonePropertyMapping(
            _require(scheme.business_date),
            _require(scheme.processing_start), _require(scheme.processing_end),
            infinite_datetime=scheme.infinite_datetime,
        )
    elif has_processing:
        return ProcessingDateMilestonesPropertyMapping(
            _require(scheme.processing_start), _require(scheme.processing_end),
            infinite_datetime=scheme.infinite_datetime,
        )
    elif has_single_date:
        return SingleBusinessDateMilestonePropertyMapping(_require(scheme.business_date))
    return None


def _milestone_scheme_name(rcm: RelationalClassMapping, repo: DataStore) -> Optional[str]:
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
        repo = schema.datastore if schema else None
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
        if isinstance(repo, DataCatalog):
            lines.append(f"## DataStore: {repo_name} (Catalog)")
        else:
            lines.append(f"## DataStore: {repo_name} (Database)")
        lines.append("")

        if repo and repo.milestoning_schemes:
            scheme_rows = [
                [s.name, s.processing_start or "", s.processing_end or "",
                 s.business_date or "", s.business_date_from or "", s.business_date_to or "",
                 s.infinite_datetime or ""]
                for s in repo.milestoning_schemes
            ]
            lines.append(_md_table(
                ["Scheme", "processing_start", "processing_end", "business_date",
                 "business_date_from", "business_date_to", "infinite_datetime"],
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
                lines.append(_md_table(["Column", "Type", "Key", "Property ID"], col_rows))
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


def draft_from_repository(title: str, repo: DataStore) -> str:
    """Generate a draft mapping markdown with all columns populated and properties left blank."""
    lines: list[str] = [f"# {title}", ""]
    if isinstance(repo, DataCatalog):
        lines.append(f"## DataStore: {repo.name} (Catalog)")
    else:
        lines.append(f"## DataStore: {repo.name} (Database)")
    lines.append("")

    if repo.milestoning_schemes:
        scheme_rows = [
            [s.name, s.processing_start or "", s.processing_end or "", s.business_date or "",
             s.business_date_from or "", s.business_date_to or "", s.infinite_datetime or ""]
            for s in repo.milestoning_schemes
        ]
        lines.append(_md_table(
            ["Scheme", "processing_start", "processing_end", "business_date",
             "business_date_from", "business_date_to", "infinite_datetime"],
            scheme_rows,
        ))
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
            lines.append(_md_table(["Column", "Type", "Key", "Property ID"], col_rows))
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
