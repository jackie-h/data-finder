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
from model.relational_mapping import RelationalClassMapping, RelationalPropertyMapping, Join, EmbeddedSetMapping

_md_parser = MarkdownIt().enable("table")
_log = logging.getLogger(__name__)
_TABLE_RE = re.compile(r'(\S+)\s*→\s*(\w+)(?:\s*\(milestoning:\s*(\w+)\))?')


# ---------------------------------------------------------------------------
# Load: markdown → Mapping
# ---------------------------------------------------------------------------

def load(path: str, datastore: DataStore | None = None) -> Mapping:
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


def _build_repository_from_content(nodes: list) -> DataStore | None:
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
                                columns.append(Column(col_name, col_type, primary_key=(key == "PK")))
                    Table(table_name, columns, current_schema)
                    continue
        i += 1
    return repo


def loads(content: str, packages: list, datastore: DataStore | None = None) -> Mapping:
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
    class_mappings: list = []

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
                embedded_rows: list[tuple[Column, list[str]]] = []
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
                        if "." in prop_name:
                            embedded_rows.append((col, prop_name.split(".")))
                            continue
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

                embedded_by_nav = (
                    _build_embedded_mappings(embedded_rows, cls, classes_by_name, assoc_nav_lookup)
                    if embedded_rows else {}
                )

                milestone_mapping = _build_milestone_mapping(scheme_name, property_mappings, repository,
                                                              table_name=table_name, class_name=class_name)
                class_mappings.append(
                    RelationalClassMapping(cls, property_mappings, milestone_mapping=milestone_mapping)
                )
                class_context[cls.name] = (property_mappings, cols_by_name, embedded_by_nav)
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
                pm_list, cols_by_name, embedded_by_nav = class_context.get(src_cls_name, (None, {}, {}))
                target_cls_name = assoc_nav_lookup.get((src_cls_name, nav_prop_id))
                target_cls = classes_by_name.get(target_cls_name) if target_cls_name else None

                if pm_list is None:
                    raise ValueError(
                        f"Association '{assoc_name}': source class '{src_cls_name}' "
                        f"has no Table section defined before this Association"
                    )

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
                            if any(pm.property.id == nav_prop_id for pm in pm_list):
                                raise ValueError(
                                    f"Association '{assoc_name}': navigation property '{nav_prop_id}' "
                                    f"on class '{src_cls_name}' is already mapped"
                                )
                            prop = Property(assoc_def.target_property.name, nav_prop_id, target_cls)
                            embedded_entry = embedded_by_nav.pop(nav_prop_id, None)
                            embedded = embedded_entry[1] if embedded_entry is not None else None
                            pm_list.append(RelationalPropertyMapping(prop, Join(lhs_col, tgt_col, embedded=embedded)))
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

    for cls_name, (_, _, embedded_by_nav) in class_context.items():
        if embedded_by_nav:
            unresolved = ", ".join(sorted(embedded_by_nav))
            raise ValueError(
                f"Class '{cls_name}': embedded mapping(s) for '{unresolved}' have no matching "
                f"Association section to fall back to — an embedded mapping requires a real join"
            )

    return Mapping(title, class_mappings)


def _resolve_nav_property(cls_name: str, seg: str, classes_by_name: dict, assoc_nav_lookup: dict,
                           all_props: dict) -> Property | None:
    """Resolve a single dotted-path segment to a non-primitive (navigable) Property."""
    prop = all_props.get(seg)
    if prop is not None and not isinstance(prop.type, PrimitiveType):
        return prop
    target_cls_name = assoc_nav_lookup.get((cls_name, seg))
    if target_cls_name is not None:
        target_cls = classes_by_name.get(target_cls_name)
        if target_cls is not None:
            return Property(seg, seg, target_cls)
    return None


def _build_embedded_mappings(embedded_rows: list, cls: Class, classes_by_name: dict,
                              assoc_nav_lookup: dict) -> dict:
    """Group dotted-path (column, segments) rows by their first segment and build nested
    EmbeddedSetMappings recursively.

    Returns ``{root_nav_property_id: (nav_Property, EmbeddedSetMapping)}``. Raises ValueError for
    any segment that can't be resolved (unknown nav property, or a leaf that isn't a primitive
    property) — a bad embedded path is a mapping authoring error, not a tolerable partial mapping.
    """
    groups: dict[str, list] = {}
    for col, segments in embedded_rows:
        groups.setdefault(segments[0], []).append((col, segments[1:]))

    all_props = cls.all_properties()
    result: dict = {}
    for seg0, rows in groups.items():
        nav_prop = _resolve_nav_property(cls.name, seg0, classes_by_name, assoc_nav_lookup, all_props)
        if nav_prop is None:
            raise ValueError(
                f"Embedded mapping: could not resolve navigation property '{seg0}' on class '{cls.name}'"
            )
        target_cls = nav_prop.type
        if not isinstance(target_cls, Class):
            raise ValueError(
                f"Embedded mapping: navigation property '{seg0}' on class '{cls.name}' "
                f"does not resolve to a class"
            )
        target_all_props = target_cls.all_properties()

        nested_pms: list[RelationalPropertyMapping] = []
        deeper: list = []
        for col, segs in rows:
            if len(segs) == 1:
                leaf_id = segs[0]
                leaf_prop = target_all_props.get(leaf_id)
                if leaf_prop is None or not isinstance(leaf_prop.type, PrimitiveType):
                    raise ValueError(
                        f"Embedded mapping: property '{leaf_id}' not found (or not primitive) "
                        f"on class '{target_cls.name}'"
                    )
                nested_pms.append(RelationalPropertyMapping(leaf_prop, col))
            else:
                deeper.append((col, segs))

        if deeper:
            nested = _build_embedded_mappings(deeper, target_cls, classes_by_name, assoc_nav_lookup)
            for nested_prop, nested_esm in nested.values():
                nested_pms.append(RelationalPropertyMapping(nested_prop, nested_esm))

        result[seg0] = (nav_prop, EmbeddedSetMapping(target_cls, nested_pms))
    return result


def _flatten_embedded_rows(esm: EmbeddedSetMapping, prefix: str) -> list[list[str]]:
    """Inverse of _build_embedded_mappings: flatten a nested EmbeddedSetMapping back into
    dotted Property ID rows for round-tripping through to_markdown."""
    rows: list[list[str]] = []
    for pm in esm.property_mappings:
        dotted = f"{prefix}.{pm.property.id}"
        if isinstance(pm.target, Column):
            rows.append([pm.target.name, pm.target.type or "", "", dotted])
        elif isinstance(pm.target, EmbeddedSetMapping):
            rows.extend(_flatten_embedded_rows(pm.target, dotted))
    return rows


_COLUMN_MAPPING_HEADERS = ["Column", "Type", "Key", "Property ID"]
_ASSOCIATION_HEADERS = ["Source Column", "Target Table", "Target Column"]


def _parse_ast_table(node: SyntaxTreeNode, expected_headers: list[str] | None = None) -> list[dict]:
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


def _synthetic_milestoning_property(prop_name: str, col_name: str, scheme_name: str, repository) -> Property | None:
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


def save(path: str, title: str, mapping: Mapping, model_paths: list[str] | None = None) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(to_markdown(title, mapping, model_paths))


def to_markdown(title: str, mapping: Mapping, model_paths: list[str] | None = None) -> str:
    lines: list[str] = [f"# {title}", ""]
    for mp in (model_paths or []):
        lines.append(f"## Model: {mp}")
        lines.append("")

    # Group by repo → schema, preserving insertion order
    repos_seen: list[str] = []
    repo_objs: dict = {}
    repo_schema_map: dict = {}

    for rcm in mapping.mappings:
        if not isinstance(rcm, RelationalClassMapping):
            continue
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
                if table is None:
                    continue
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
                        if pm.target.embedded is not None:
                            col_rows.extend(_flatten_embedded_rows(pm.target.embedded, pm.property.id))
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
                            [[pm.target.lhs.name, pm.target.rhs.table.name if pm.target.rhs.table else "", pm.target.rhs.name]],
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


def _primary_table(rcm: RelationalClassMapping) -> Optional[Table]:
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
