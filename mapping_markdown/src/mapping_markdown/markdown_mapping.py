import logging
import os
import re
from typing import Optional

from markdown_it import MarkdownIt
from markdown_it.tree import SyntaxTreeNode

from model.m3 import Class, PrimitiveType, Association, Property, DateTime, ONE_TO_ONE, ZERO_TO_ONE
from model.mapping import Mapping, ProcessingDateMilestonesPropertyMapping, SingleBusinessDateMilestonePropertyMapping, \
    BusinessDateAndProcessingMilestonePropertyMapping, BiTemporalMilestonePropertyMapping, \
    BusinessDateRangeMilestonePropertyMapping, SingleProcessingDatetimeMilestonePropertyMapping
from mapping_markdown.join_condition import parse_condition
from model.relational import BinaryOperation, DataStore, Database, DataCatalog, Schema, Table, MilestoningScheme, Column, \
    ForeignKey, Operation, UnaryOperation
from model.relational_mapping import RelationalClassMapping, RelationalPropertyMapping, Join, EmbeddedSetMapping

_md_parser = MarkdownIt().enable("table")
_log = logging.getLogger(__name__)
_TABLE_RE = re.compile(r'(\S+)(?:\s*→\s*(\w+))?(?:\s*\(milestoning:\s*(\w+)\))?\s*$')


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
                    table = Table(table_name, [], current_schema)
                    if i < len(nodes) and nodes[i].type == "table":
                        for row in _parse_ast_table(nodes[i], _COLUMN_MAPPING_HEADERS):
                            col_name = row.get("Column", "").strip()
                            col_type = row.get("Type", "").strip()
                            key = row.get("Key", "").strip().upper()
                            if col_name:
                                table.add_column(col_name, col_type, primary_key=(key == "PK"),
                                                 nullable=_parse_nullable(row, col_name))
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
    """Build (source_class_name, source_property_id) -> (target class name, multiplicity) from all associations."""
    result = {}
    for pkg in packages:
        for child in pkg.children:
            if isinstance(child, Association):
                # the class is resolved later, after classes_by_name is built
                result[(child.source, child.source_property.id)] = (child.target, child.source_multiplicity)
    return result


def _infinities(row: dict) -> dict:
    """What the end of a row that has not ended holds on each axis, from a scheme row. A scheme has one for each axis. The
    ``infinite_datetime`` column gives one value to both, and ``processing_infinite_datetime`` and
    ``business_infinite_datetime`` each its own, which a table needs that ends one axis in a datetime and the other in a
    date, or one in an infinity and the other in NULL. A column left blank gives its axis no infinity, unless
    ``infinite_datetime`` gives one."""
    shared = row.get("infinite_datetime", "").strip() or None
    return {"processing_infinite_datetime": row.get("processing_infinite_datetime", "").strip() or shared,
            "business_infinite_datetime": row.get("business_infinite_datetime", "").strip() or shared}


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
    # (source_class_name, source_property_id) -> (target class name, multiplicity)
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
                                **_infinities(row),
                                processing_datetime=row.get("processing_datetime", "").strip() or None,
                            ))
                    i += 1
                continue

            elif level == "h4" and text.startswith("Table:"):
                m = _TABLE_RE.match(text[len("Table:"):].strip())
                if m is None:
                    i += 1
                    continue
                table_name, class_name, scheme_name = m.group(1), m.group(2), m.group(3)

                if class_name is None:
                    # a table no class is mapped to: it is only reached by a join (a link table, say), and says how
                    # its rows are milestoned if they are
                    link = tables_by_name.get(table_name)
                    if link is not None and scheme_name:
                        link.milestoning_scheme = next((sc for sc in repository.milestoning_schemes if sc.name == scheme_name), None)
                        if link.milestoning_scheme is None:
                            raise ValueError(f"Table '{table_name}': milestoning scheme '{scheme_name}' is not defined")
                    i += 2 if i + 1 < len(nodes) and nodes[i + 1].type == "table" else 1
                    continue
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
                        if not _parse_nullable(row, col_name) or key == "PK":
                            col.nullable = False
                        if "." in prop_name:
                            embedded_rows.append((col, prop_name.split(".")))
                            continue
                        prop = all_props.get(prop_name)
                        if prop is None:
                            prop = _synthetic_milestoning_property(prop_name, col_name, scheme_name, repository)
                        if prop is None:
                            nav = assoc_nav_lookup.get((cls.name, prop_name))
                            if nav is not None:
                                target_cls = classes_by_name.get(nav[0])
                                if target_cls is not None:
                                    from model.m3 import Property as _Property
                                    prop = _Property(prop_name, prop_name, target_cls, nav[1])
                        if prop is None:
                            raise ValueError(f"Property '{prop_name}' not found in class '{cls.name}'")
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
                heading = text[len("Association:"):].strip()
                # ``Association: Name (embedded in Class.path)`` reads a property of an object embedded in Class's row
                embedded_in = _EMBEDDED_IN_RE.search(heading)
                assoc_name = heading[:embedded_in.start()].strip() if embedded_in else heading
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

                nav_prop_id = assoc_def.source_property.id
                owner_cls_name = embedded_in.group(1) if embedded_in else assoc_def.source
                src_cls_name = assoc_def.source
                pm_list, cols_by_name, embedded_by_nav = class_context.get(owner_cls_name, (None, {}, {}))
                nav = assoc_nav_lookup.get((src_cls_name, nav_prop_id))
                target_cls = classes_by_name.get(nav[0]) if nav else None

                if pm_list is None:
                    raise ValueError(
                        f"Association '{assoc_name}': source class '{owner_cls_name}' "
                        f"has no Table section defined before this Association"
                    )

                if i < len(nodes) and nodes[i].type == "table":
                    hops, conditions, condition_texts = _association_hops(
                        assoc_name, _parse_ast_table(nodes[i], _ASSOCIATION_HEADERS, _ASSOCIATION_OPTIONAL_HEADERS),
                        cols_by_name, tables_by_name)
                    if hops and target_cls is not None:
                        first_lhs = hops[0][0]
                        last_lhs, last_rhs = hops[-1]
                        via = hops[:-1]
                        if embedded_in is not None:
                            _resolve_embedded_join(assoc_name, embedded_by_nav, embedded_in.group(2).split("."),
                                                   nav_prop_id, last_lhs, last_rhs, via, conditions, condition_texts)
                        elif any(pm.property.id == nav_prop_id for pm in pm_list):
                            raise ValueError(
                                f"Association '{assoc_name}': navigation property '{nav_prop_id}' "
                                f"on class '{owner_cls_name}' is already mapped"
                            )
                        else:
                            prop = Property(assoc_def.source_property.name, nav_prop_id, target_cls, assoc_def.source_multiplicity)
                            embedded_entry = embedded_by_nav.pop(nav_prop_id, None)
                            embedded = embedded_entry[1] if embedded_entry is not None else None
                            pm_list.append(RelationalPropertyMapping(prop, Join(
                                last_lhs, last_rhs, embedded=embedded, via=via, conditions=conditions,
                                condition_texts=condition_texts)))
                        for lhs, rhs in hops:
                            if lhs.table is not None:
                                lhs.table.foreign_keys.append(ForeignKey(lhs, rhs))
                    else:
                        _log.warning(
                            "Association '%s': could not resolve navigation property "
                            "for source class '%s'",
                            assoc_name, owner_cls_name,
                        )
                    i += 1
                continue

        i += 1

    # An embedded mapping with no Association section has nothing to fall back to: the properties live in the
    # class's own row, so the navigation property maps straight to the embedded columns.
    for pm_list, _, embedded_by_nav in class_context.values():
        for nav_prop, embedded in embedded_by_nav.values():
            _check_embedded_joins(nav_prop.id, embedded)
            pm_list.append(RelationalPropertyMapping(nav_prop, embedded))

    return Mapping(title, class_mappings)


def _association_hops(assoc_name: str, rows: list[dict], first_columns: dict, tables_by_name: dict) \
        -> tuple[list[tuple[Column, Column]], list[Operation | None], list[str | None]]:
    """The (from, to) columns of each row of an Association table, in order, and the condition of each row, read, and as
    it was written. A row after the first starts from a column of the table the row before it reaches, so several rows
    are a path through several tables. A row can have a ``Condition`` of its two tables besides its key columns."""
    hops: list[tuple[Column, Column]] = []
    conditions: list[Operation | None] = []
    texts: list[str | None] = []
    source_columns = first_columns
    for row in rows:
        src_col = row.get("Source Column", "").strip()
        tgt_table_name = row.get("Target Table", "").strip()
        tgt_col_name = row.get("Target Column", "").strip()
        lhs = source_columns.get(src_col)
        tgt_table = tables_by_name.get(tgt_table_name)
        if lhs is None:
            _log.warning("Association '%s': source column '%s' not found in table for class", assoc_name, src_col)
            return [], [], []
        if tgt_table is None:
            _log.warning("Association '%s': target table '%s' not found", assoc_name, tgt_table_name)
            return [], [], []
        rhs = next((c for c in tgt_table.columns if c.name == tgt_col_name), None)
        if rhs is None:
            _log.warning("Association '%s': target column '%s' not found in table '%s'", assoc_name, tgt_col_name,
                         tgt_table_name)
            return [], [], []
        text = row.get("Condition", "").strip() or None
        condition = None
        if text is not None:
            source_table = lhs.table
            assert source_table is not None
            if source_table is tgt_table or source_table.qualified_name == tgt_table.qualified_name:
                raise ValueError(f"Association '{assoc_name}': a condition on a join of '{tgt_table.name}' to itself is "
                                 f"not supported yet")
            condition = parse_condition(text, [source_table, tgt_table], f"Association '{assoc_name}'")
        hops.append((lhs, rhs))
        conditions.append(condition)
        texts.append(text)
        source_columns = {c.name: c for c in tgt_table.columns}
    return hops, conditions, texts


def _check_embedded_joins(path: str, esm: EmbeddedSetMapping) -> None:
    """Every join of an embedded object has been given its columns by an Association section."""
    for pm in esm.property_mappings:
        if isinstance(pm.target, Join) and not pm.target.resolved:
            raise ValueError(
                f"'{path}.{pm.property.id}' is mapped to a column but reaches another table: add an Association "
                f"section headed '(embedded in <Class>.{path})' that says which table")
        if isinstance(pm.target, EmbeddedSetMapping):
            _check_embedded_joins(f"{path}.{pm.property.id}", pm.target)


def _resolve_embedded_join(assoc_name: str, embedded_by_nav: dict, path: list[str], nav_prop_id: str,
                           lhs: Column, rhs: Column, via: list[tuple[Column, Column]],
                           conditions: list[Operation | None], condition_texts: list[str | None]) -> None:
    """Point the pending join of the embedded object at ``path`` (e.g. classification), named ``nav_prop_id``, at its
    columns."""
    entry = embedded_by_nav.get(path[0])
    esm = entry[1] if entry is not None else None
    for seg in path[1:]:
        esm = next((pm.target for pm in esm.property_mappings
                    if pm.property.id == seg and isinstance(pm.target, EmbeddedSetMapping)), None) if esm else None
    pending = next((pm.target for pm in esm.property_mappings
                    if pm.property.id == nav_prop_id and isinstance(pm.target, Join)), None) if esm else None
    if pending is None:
        raise ValueError(
            f"Association '{assoc_name}': no property '{'.'.join(path)}.{nav_prop_id}' is mapped on a column of the "
            f"table; give the foreign key column that Property ID")
    pending.lhs, pending.rhs, pending.via = lhs, rhs, via
    pending.conditions, pending.condition_texts = list(conditions), list(condition_texts)
    pending.resolved = True


def _resolve_nav_property(cls_name: str, seg: str, classes_by_name: dict, assoc_nav_lookup: dict,
                           all_props: dict) -> Property | None:
    """Resolve a single dotted-path segment to a non-primitive (navigable) Property."""
    prop = all_props.get(seg)
    if prop is not None and not isinstance(prop.type, PrimitiveType):
        return prop
    nav = assoc_nav_lookup.get((cls_name, seg))
    if nav is not None:
        target_cls = classes_by_name.get(nav[0])
        if target_cls is not None:
            return Property(seg, seg, target_cls, nav[1])
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
                    nav_leaf = _resolve_nav_property(target_cls.name, leaf_id, classes_by_name, assoc_nav_lookup, target_all_props)
                    if nav_leaf is not None:
                        # a property of the embedded class that reaches another table: its Association section, headed
                        # ``(embedded in ...)``, gives the join; until then the foreign key column stands in for it
                        pending = Join(col, col)
                        pending.resolved = False
                        nested_pms.append(RelationalPropertyMapping(nav_leaf, pending))
                        continue
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
            rows.append([pm.target.name, pm.target.type or "", "", _nullable_cell(pm.target), dotted])
        elif isinstance(pm.target, Join):
            first = _first_column(pm.target)
            rows.append([first.name, first.type or "", "FK", _nullable_cell(first), dotted])
        elif isinstance(pm.target, EmbeddedSetMapping):
            rows.extend(_flatten_embedded_rows(pm.target, dotted))
    return rows


def _embedded_joins(esm: EmbeddedSetMapping, path: list[str]) -> list[tuple[EmbeddedSetMapping, Property, Join, list[str]]]:
    """The joins of the objects embedded in a row, each with the object that holds it and the path to that object."""
    found = []
    for pm in esm.property_mappings:
        if isinstance(pm.target, Join):
            found.append((esm, pm.property, pm.target, path))
        elif isinstance(pm.target, EmbeddedSetMapping):
            found.extend(_embedded_joins(pm.target, path + [pm.property.id]))
    return found


def _first_column(join: Join) -> Column:
    """The column of the owning row a join starts from: the first hop's, whether or not it has hops before the last."""
    return join.via[0][0] if join.via else join.lhs


def _association_rows(join: Join) -> list[list[str]]:
    """The rows of an Association table: a row for each table the join passes through, then the one it ends at. A
    Condition column follows when some hop has one."""
    rows = [[lhs.name, rhs.table.name if rhs.table else "", rhs.name] for lhs, rhs in join.via + [(join.lhs, join.rhs)]]
    if any(join.condition_texts):
        rows = [row + [text or ""] for row, text in zip(rows, join.condition_texts)]
    return rows


def _association_headers(join: Join) -> list[str]:
    return _ASSOCIATION_HEADERS + (_ASSOCIATION_OPTIONAL_HEADERS if any(join.condition_texts) else [])


def _condition_columns(mapping: Mapping) -> set[tuple[str, str]]:
    """The (table, column) of each column a join condition reads, which the table section has to name for the condition
    to be read back, whether or not a property is mapped to the column."""
    found: set[tuple[str, str]] = set()

    def walk(op) -> None:
        if isinstance(op, Column):
            if op.table is not None:
                found.add((op.table.qualified_name, op.name))
        elif isinstance(op, (BinaryOperation, UnaryOperation)):
            for child in ((op.left, op.right) if isinstance(op, BinaryOperation) else (op.element,)):
                walk(child)

    for rcm in mapping.mappings:
        joins = [pm.target for pm in rcm.property_mappings if isinstance(pm.target, Join)]
        for pm in rcm.property_mappings:
            if isinstance(pm.target, EmbeddedSetMapping):
                joins.extend(j for _, _, j, _ in _embedded_joins(pm.target, [pm.property.id]))
        for join in joins:
            for condition in join.conditions:
                walk(condition)
    return found


def _link_tables(rcm: RelationalClassMapping) -> list[Table]:
    """The tables the joins of a class mapping pass through on the way to another class's table."""
    joins = [pm.target for pm in rcm.property_mappings if isinstance(pm.target, Join)]
    for pm in rcm.property_mappings:
        if isinstance(pm.target, EmbeddedSetMapping):
            joins.extend(j for _, _, j, _ in _embedded_joins(pm.target, [pm.property.id]))
    return [rhs.table for j in joins for _, rhs in j.via if rhs.table is not None]


_COLUMN_MAPPING_HEADERS = ["Column", "Type", "Key", "Nullable", "Property ID"]
_EMBEDDED_IN_RE = re.compile(r'\(\s*embedded in\s+(\w+)\.([\w.]+)\s*\)\s*$')
_ASSOCIATION_HEADERS = ["Source Column", "Target Table", "Target Column"]
_ASSOCIATION_OPTIONAL_HEADERS = ["Condition"]


def _parse_nullable(row: dict, col_name: str) -> bool:
    """Read a column table row's Nullable cell: Y (nullable) or N (not null)."""
    value = row.get("Nullable", "").strip().upper()
    if value not in ("Y", "N"):
        raise ValueError(f"Column '{col_name}' has Nullable {row.get('Nullable', '')!r} — expected Y or N")
    return value == "Y"


def _nullable_cell(col: Column) -> str:
    return "Y" if col.nullable else "N"


def _parse_ast_table(node: SyntaxTreeNode, expected_headers: list[str] | None = None,
                     optional_headers: list[str] | None = None) -> list[dict]:
    """The rows of a table. ``expected_headers`` are its columns, and ``optional_headers`` may follow them."""
    thead, tbody = node.children[0], node.children[1]
    headers = [c.children[0].content for c in thead.children[0].children]
    if expected_headers is not None and headers != expected_headers \
            and headers != expected_headers + (optional_headers or []):
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
        scheme.business_date, scheme.business_date_from, scheme.business_date_to, scheme.processing_datetime,
    } - {None}
    if col_name in milestoning_cols:
        # An end column holds NULL, rather than an infinity date, when its axis has no infinity in the scheme.
        infinity = scheme.business_infinite_datetime if col_name == scheme.business_date_to \
            else scheme.processing_infinite_datetime
        open_ended = infinity is None and col_name in (scheme.processing_end, scheme.business_date_to)
        return Property(_camel_to_display_name(prop_name), prop_name, DateTime,
                        ZERO_TO_ONE if open_ended else ONE_TO_ONE)
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

    if scheme.processing_datetime:
        others = [name for name, value in (
            ("processing_start", scheme.processing_start), ("processing_end", scheme.processing_end),
            ("business_date", scheme.business_date), ("business_date_from", scheme.business_date_from),
            ("business_date_to", scheme.business_date_to)) if value]
        if others:
            raise ValueError(
                f"Milestoning scheme '{scheme_name}' has a processing_datetime, a processing snapshot, which cannot "
                f"be combined with {', '.join(others)}")
        return SingleProcessingDatetimeMilestonePropertyMapping(_require(scheme.processing_datetime))

    has_processing = scheme.processing_start and scheme.processing_end
    has_single_date = scheme.business_date and not scheme.business_date_from
    has_range_date = scheme.business_date_from and scheme.business_date_to

    if has_range_date and has_processing:
        return BiTemporalMilestonePropertyMapping(
            _require(scheme.business_date_from), _require(scheme.business_date_to),
            _require(scheme.processing_start), _require(scheme.processing_end),
            processing_infinite_datetime=scheme.processing_infinite_datetime,
            business_infinite_datetime=scheme.business_infinite_datetime,
        )
    elif has_single_date and has_processing:
        return BusinessDateAndProcessingMilestonePropertyMapping(
            _require(scheme.business_date),
            _require(scheme.processing_start), _require(scheme.processing_end),
            processing_infinite_datetime=scheme.processing_infinite_datetime,
        )
    elif has_processing:
        return ProcessingDateMilestonesPropertyMapping(
            _require(scheme.processing_start), _require(scheme.processing_end),
            processing_infinite_datetime=scheme.processing_infinite_datetime,
        )
    elif has_single_date:
        return SingleBusinessDateMilestonePropertyMapping(_require(scheme.business_date))
    elif has_range_date:
        return BusinessDateRangeMilestonePropertyMapping(
            _require(scheme.business_date_from), _require(scheme.business_date_to),
            business_infinite_datetime=scheme.business_infinite_datetime,
        )
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
    elif isinstance(mm, BusinessDateRangeMilestonePropertyMapping):
        date_from_col = mm._date_from.target.name if isinstance(mm._date_from.target, Column) else None
        date_to_col = mm._date_to.target.name if isinstance(mm._date_to.target, Column) else None
        for s in repo.milestoning_schemes:
            if s.business_date_from == date_from_col and s.business_date_to == date_to_col and not s.processing_start:
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
    elif isinstance(mm, SingleProcessingDatetimeMilestonePropertyMapping):
        datetime_col = mm._datetime.target.name if isinstance(mm._datetime.target, Column) else None
        for s in repo.milestoning_schemes:
            if s.processing_datetime == datetime_col:
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

def _find_association_name(source_cls: Class, target_cls: Class, property_id: str) -> str:
    pkg = source_cls.package
    if pkg is not None:
        for child in pkg.children:
            if (isinstance(child, Association) and child.source == source_cls.name
                    and child.target == target_cls.name and child.source_property.id == property_id):
                return child.name
    return f"{source_cls.name}{target_cls.name}"


def save(path: str, title: str, mapping: Mapping, model_paths: list[str] | None = None) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(to_markdown(title, mapping, model_paths))


def _scheme_table(schemes: list) -> str:
    """The DataStore's milestoning schemes as a table. The ``processing_datetime`` column is written only when a scheme
    uses it, so mappings without a processing snapshot are written as they always were."""
    snapshot = any(s.processing_datetime for s in schemes)
    # one infinite_datetime column when no scheme ends its axes differently, as it always was; else one for each axis
    per_axis = any(s.processing_infinite_datetime != s.business_infinite_datetime for s in schemes)
    infinity_headers = ["processing_infinite_datetime", "business_infinite_datetime"] if per_axis else ["infinite_datetime"]
    headers = ["Scheme", "processing_start", "processing_end"] + (["processing_datetime"] if snapshot else []) + [
        "business_date", "business_date_from", "business_date_to"] + infinity_headers
    rows = [[s.name, s.processing_start or "", s.processing_end or ""] + ([s.processing_datetime or ""] if snapshot else []) + [
        s.business_date or "", s.business_date_from or "", s.business_date_to or "", s.processing_infinite_datetime or ""] + (
        [s.business_infinite_datetime or ""] if per_axis else [])
        for s in schemes]
    return _md_table(headers, rows)


def to_markdown(title: str, mapping: Mapping, model_paths: list[str] | None = None) -> str:
    lines: list[str] = [f"# {title}", ""]
    for mp in (model_paths or []):
        lines.append(f"## Model: {mp}")
        lines.append("")

    # tables a join passes through that no class is mapped to, by (repo, schema) and name
    link_tables: dict = {}
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
        for link in _link_tables(rcm):
            link_schema = link.schema.name if link.schema else "(none)"
            link_tables.setdefault((repo_name, link_schema), {})[link.name] = link
            repo_schema_map[repo_name].setdefault(link_schema, [])

    condition_columns = _condition_columns(mapping)
    for repo_name in repos_seen:
        repo = repo_objs[repo_name]
        if isinstance(repo, DataCatalog):
            lines.append(f"## DataStore: {repo_name} (Catalog)")
        else:
            lines.append(f"## DataStore: {repo_name} (Database)")
        lines.append("")

        if repo and repo.milestoning_schemes:
            lines.append(_scheme_table(repo.milestoning_schemes))
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
                # A join's column may also be a property's column (a trade's symbol is also the key of its instrument)
                mapped_cols = {pm.target.name for pm in rcm.property_mappings if isinstance(pm.target, Column)}
                for pm in rcm.property_mappings:
                    if isinstance(pm.target, Column):
                        key = "PK" if pm.target.primary_key else ""
                        col_rows.append([pm.target.name, pm.target.type or "", key, _nullable_cell(pm.target), pm.property.id])
                    elif isinstance(pm.target, Join):
                        lhs = _first_column(pm.target)
                        if lhs.name in mapped_cols:
                            if pm.target.embedded is not None:
                                col_rows.extend(_flatten_embedded_rows(pm.target.embedded, pm.property.id))
                            continue
                        key = "FK" if lhs.name in fk_cols else ""
                        col_rows.append([lhs.name, lhs.type or "", key, _nullable_cell(lhs), pm.property.id])
                        mapped_cols.add(lhs.name)  # a column is one row, whatever joins start from it
                        if pm.target.embedded is not None:
                            col_rows.extend(_flatten_embedded_rows(pm.target.embedded, pm.property.id))
                    elif isinstance(pm.target, EmbeddedSetMapping):
                        col_rows.extend(_flatten_embedded_rows(pm.target, pm.property.id))
                written = {row[0] for row in col_rows}
                for column in table.columns:
                    if (table.qualified_name, column.name) in condition_columns and column.name not in written:
                        col_rows.append([column.name, column.type or "", "PK" if column.primary_key else "",
                                         _nullable_cell(column), ""])
                lines.append(_md_table(_COLUMN_MAPPING_HEADERS, col_rows))
                lines.append("")

            for link in link_tables.get((repo_name, schema_name), {}).values():
                heading = f"#### Table: {link.name}"
                if link.milestoning_scheme is not None:
                    heading += f" (milestoning: {link.milestoning_scheme.name})"
                lines.append(heading)
                lines.append("")
                lines.append(_md_table(_COLUMN_MAPPING_HEADERS, [
                    [c.name, c.type or "", "PK" if c.primary_key else "", _nullable_cell(c), ""] for c in link.columns]))
                lines.append("")

            for rcm in rcms:
                for pm in rcm.property_mappings:
                    if isinstance(pm.target, Join):
                        assoc_name = _find_association_name(rcm.clazz, pm.property.type, pm.property.id)
                        lines.append(f"#### Association: {assoc_name}")
                        lines.append("")
                        lines.append(_md_table(_association_headers(pm.target), _association_rows(pm.target)))
                        lines.append("")
                    elif isinstance(pm.target, EmbeddedSetMapping):
                        for holder, prop, join, path in _embedded_joins(pm.target, [pm.property.id]):
                            assert isinstance(prop.type, Class)
                            assoc_name = _find_association_name(holder.clazz, prop.type, prop.id)
                            lines.append(f"#### Association: {assoc_name} (embedded in {rcm.clazz.name}.{'.'.join(path)})")
                            lines.append("")
                            lines.append(_md_table(_association_headers(join), _association_rows(join)))
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
        lines.append(_scheme_table(repo.milestoning_schemes))
        lines.append("")

    for schema in repo.schemas:
        lines.append(f"### Schema: {schema.name}")
        lines.append("")
        for table in schema.tables:
            lines.append(f"#### Table: {table.name} → ?")
            lines.append("")
            col_rows = [
                [col.name, col.type or "", "PK" if col.primary_key else "", _nullable_cell(col), ""]
                for col in table.columns
            ]
            lines.append(_md_table(_COLUMN_MAPPING_HEADERS, col_rows))
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
