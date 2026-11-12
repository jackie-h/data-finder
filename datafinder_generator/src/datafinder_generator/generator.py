import os
import re

from model.m3 import Class, PrimitiveType, Property
from jinja2 import Environment, PackageLoader

from model.m3 import Association
from model.naming import to_python_name, to_snake_case
from model.mapping import Mapping, MilestonePropertyMapping, ProcessingDateMilestonesPropertyMapping, \
    SingleBusinessDateMilestonePropertyMapping, BusinessDateAndProcessingMilestonePropertyMapping, \
    BiTemporalMilestonePropertyMapping, BusinessDateRangeMilestonePropertyMapping, \
    SingleProcessingDatetimeMilestonePropertyMapping
from model.relational import (
    Column, ComparisonOperation, DecimalConstantOperation, query_of, FloatConstantOperation, IntegerConstantOperation,
    IsNotNullOperation, IsNullOperation, LogicalOperation, NamedRelation, NotOperation, StringConstantOperation, Table, TableAlias,
)
from model.relational_mapping import Join, EmbeddedSetMapping


def is_primitive(prop: Property) -> bool:
    return isinstance(prop.type, PrimitiveType)


def flatten_embedded(join: Join) -> list[tuple[str, Property, Column]]:
    """Flatten a Join's embedded EmbeddedSetMapping (if any) into (dotted_path, leaf_property, column)
    triples, relative to the related class (e.g. 'name', 'branch.city' — not 'account.name').

    Returns [] when the join has no embedded mapping, so templates can gate codegen on it.
    """
    if join is None or join.embedded is None:
        return []
    result: list[tuple[str, Property, Column]] = []

    def _walk(esm: EmbeddedSetMapping, prefix: str):
        for pm in esm.property_mappings:
            dotted = f"{prefix}.{pm.property.id}" if prefix else pm.property.id
            if isinstance(pm.target, Column):
                result.append((dotted, pm.property, pm.target))
            elif isinstance(pm.target, EmbeddedSetMapping):
                _walk(pm.target, dotted)

    _walk(join.embedded, '')
    return result

def embedded_optional(join: Join) -> dict[str, bool]:
    """For each dotted path ``flatten_embedded`` gives, whether the value can be missing because the leaf property or
    one of the embedded properties on the way to it is optional (its multiplicity starts at 0)."""
    result: dict[str, bool] = {}
    if join is None or join.embedded is None:
        return result

    def _walk(esm: EmbeddedSetMapping, prefix: str, optional: bool):
        for pm in esm.property_mappings:
            dotted = f"{prefix}.{pm.property.id}" if prefix else pm.property.id
            here = optional or pm.property.multiplicity.lower == 0
            if isinstance(pm.target, Column):
                result[dotted] = here
            elif isinstance(pm.target, EmbeddedSetMapping):
                _walk(pm.target, dotted, here)

    _walk(join.embedded, '', False)
    return result


class _ClassView:
    """A class mapping as the finder templates see it: the properties reached by a join or held in a column, with the
    embedded (join-less) ones apart in ``embedded_mappings``."""

    def __init__(self, rcm):
        self.clazz = rcm.clazz
        self.milestone_mapping = rcm.milestone_mapping
        self.property_mappings = [pm for pm in rcm.property_mappings if not isinstance(pm.target, EmbeddedSetMapping)]
        self.embedded_mappings = [pm for pm in rcm.property_mappings if isinstance(pm.target, EmbeddedSetMapping)]
        self.main_relation = rcm.main_relation


class EmbeddedClass:
    """A class embedded in its owner's row: its properties are columns of the owner's table, so it needs no join."""

    def __init__(self, name: str, label: str, columns: list[tuple[Property, Column]],
                 children: list[tuple[Property, str]], optional: bool = False,
                 joins: list[tuple[Property, Join]] | None = None):
        self.name = name
        self.label = label
        self.columns = columns
        self.children = children
        # the properties of this class that reach another table, each with the join that does
        self.joins = joins or []
        # whether the embedded property that holds this class, or one above it, is optional
        self.optional = optional


def embedded_classes(rcm: _ClassView) -> list[EmbeddedClass]:
    """The embedded classes of a class mapping, nested ones before the classes that hold them."""
    found: list[EmbeddedClass] = []

    def _walk(esm: EmbeddedSetMapping, stem: str, label: str, optional: bool) -> str:
        columns, children, joins = [], [], []
        for pm in esm.property_mappings:
            if isinstance(pm.target, Column):
                columns.append((pm.property, pm.target))
            elif isinstance(pm.target, Join):
                joins.append((pm.property, pm.target))
            elif isinstance(pm.target, EmbeddedSetMapping):
                child = _walk(pm.target, stem + pm.property.id[:1].upper() + pm.property.id[1:], f"{label} {pm.property.name}",
                              optional or pm.property.multiplicity.lower == 0)
                children.append((pm.property, child))
        found.append(EmbeddedClass(stem + "Embedded", label, columns, children, optional, joins))
        return stem + "Embedded"

    for pm in rcm.embedded_mappings:
        _walk(pm.target, f"_{rcm.clazz.name}{pm.property.id[:1].upper()}{pm.property.id[1:]}", pm.property.name,
              pm.property.multiplicity.lower == 0)
    return found


def has_processing_temporal(mapping: MilestonePropertyMapping) -> bool:
    return isinstance(mapping, ProcessingDateMilestonesPropertyMapping)

def has_uni_business_temporal(mapping: MilestonePropertyMapping) -> bool:
    return isinstance(mapping, SingleBusinessDateMilestonePropertyMapping)

def has_business_date_and_processing(mapping: MilestonePropertyMapping) -> bool:
    return isinstance(mapping, BusinessDateAndProcessingMilestonePropertyMapping)

def has_bitemporal(mapping: MilestonePropertyMapping) -> bool:
    return isinstance(mapping, BiTemporalMilestonePropertyMapping)

def has_business_date_range(mapping: MilestonePropertyMapping) -> bool:
    return isinstance(mapping, BusinessDateRangeMilestonePropertyMapping) and not isinstance(mapping, BiTemporalMilestonePropertyMapping)

def has_processing_snapshot(mapping: MilestonePropertyMapping) -> bool:
    return isinstance(mapping, SingleProcessingDatetimeMilestonePropertyMapping)

def _query_constant(alias: TableAlias) -> str:
    """The name, in a generated finder module, of the query a class is mapped to."""
    return "_QUERY_" + re.sub(r"\W", "_", alias.name).upper()


def owner_code(table) -> str:
    """Python source for the relation a column or an attribute belongs to: a table of that name, whose columns this finder
    does not need to know; or, for a query a class is mapped to, the query, which has to be known to be written."""
    if (query := query_of(table)) is not None:
        return _query_constant(query)
    return f"Table('{table_qualified_name(table)}', [])"


def column_code(column: Column) -> str:
    """Python source that builds ``column`` in a generated finder."""
    key = ", primary_key=True" if column.primary_key else ""
    return f"Column('{column.name}', '{column.type}', {owner_code(column.table)}{key})"


def _scheme_columns_code(table: Table | TableAlias, owned_by: str | None = None) -> str:
    """Python source for the milestoning columns of a table a join passes through, from its milestoning scheme. For a query that a class is
    mapped to the columns are the query's own, which ``owned_by`` (a function of a name) gives."""
    scheme = table.milestoning_scheme
    assert scheme is not None
    by_name = {c.name: c for c in table.columns if isinstance(c, Column)}

    def col(name: str | None) -> str:
        column = by_name.get(name or "")
        if column is None:
            raise ValueError(f"Table '{table.name}': milestoning column '{name}' of scheme '{scheme.name}' is not a column")
        if owned_by is not None:
            return f"{owned_by}('{column.name}')"
        return f"Column('{column.name}', '{column.type}')"

    processing_infinite = (f", processing_infinite_datetime='{scheme.processing_infinite_datetime}'"
                           if scheme.processing_infinite_datetime else "")
    business_infinite = (f", business_infinite_datetime='{scheme.business_infinite_datetime}'"
                         if scheme.business_infinite_datetime else "")
    if scheme.processing_datetime:
        return f"SingleProcessingDatetimeColumn({col(scheme.processing_datetime)})"
    if scheme.processing_start and (scheme.business_date_from or scheme.business_date_to):
        return (f"BiTemporalColumns({col(scheme.business_date_from)}, {col(scheme.business_date_to)}, "
                f"{col(scheme.processing_start)}, {col(scheme.processing_end)}{processing_infinite}{business_infinite})")
    if scheme.processing_start and scheme.business_date:
        return (f"BusinessDateAndProcessingTemporalColumns({col(scheme.business_date)}, {col(scheme.processing_start)}, "
                f"{col(scheme.processing_end)}{processing_infinite})")
    if scheme.processing_start:
        return f"ProcessingTemporalColumns({col(scheme.processing_start)}, {col(scheme.processing_end)}{processing_infinite})"
    if scheme.business_date_from or scheme.business_date_to:
        return f"BusinessDateRangeColumns({col(scheme.business_date_from)}, {col(scheme.business_date_to)}{business_infinite})"
    if scheme.business_date:
        return f"SingleBusinessDateColumn({col(scheme.business_date)})"
    raise ValueError(f"Milestoning scheme '{scheme.name}' has no columns")


def table_code(table: NamedRelation) -> str:
    """Python source that builds ``table`` in a generated finder."""
    if isinstance(table, TableAlias):
        return _query_constant(table)
    assert isinstance(table, Table), f"Not a table or a query: {table!r}"
    columns = ", ".join(f"('{c.name}', '{c.type}')" for c in table.columns)
    keys = ", ".join(f"'{c.name}'" for c in table.columns if c.primary_key)
    key = f", primary_key=[{keys}]" if keys else ""
    if table.milestoning_scheme is not None:
        return f"MilestonedTable('{table_qualified_name(table)}', [{columns}], {_scheme_columns_code(table)}{key})"
    return f"Table('{table_qualified_name(table)}', [{columns}]{key})"


def _is_self_join(lhs: Column, rhs: Column) -> bool:
    """Whether a hop joins a table to itself, so a condition on it needs to say which side each column is on."""
    return lhs.table is not None and rhs.table is not None and lhs.table.qualified_name == rhs.table.qualified_name


def condition_code(op, self_join: bool = False) -> str:
    """Python source that builds the condition ``op`` of a join in a generated finder. For a join of a table to itself it
    is a function of the two sides, ``lambda plain, target: ...``, which tell the columns of the two ends apart."""
    def code(node) -> str:
        if isinstance(node, Column):
            if not self_join:
                return column_code(node)
            side = "target" if isinstance(node.owner, TableAlias) and node.owner.name == "{target}" else "plain"
            return f"{side}.column('{node.name}', '{node.type}')"
        if isinstance(node, ComparisonOperation):
            return f"ComparisonOperation({code(node.left)}, ComparisonOperator.{node.operator.name}, {code(node.right)})"
        if isinstance(node, LogicalOperation):
            return f"LogicalOperation({code(node.left)}, LogicalOperator.{node.operator.name}, {code(node.right)})"
        if isinstance(node, (IsNullOperation, IsNotNullOperation, NotOperation)):
            return f"{type(node).__name__}({code(node.element)})"
        if isinstance(node, DecimalConstantOperation):
            return f"DecimalConstantOperation(Decimal('{node.value}'))"
        if isinstance(node, (StringConstantOperation, IntegerConstantOperation, FloatConstantOperation)):
            return f"{type(node).__name__}({node.value!r})"
        raise ValueError(f"A join condition cannot be generated from {node!r}")

    return ("lambda plain, target: " if self_join else "") + code(op)


def query_assignment(rcm) -> str:
    """The line, for a finder class body after its ``__table``, that makes the class's table the query it is mapped to, so that a query
    over the class reads the SQL as a derived table."""
    query = query_of(rcm.main_relation)
    return f"    __table = {_query_constant(query)}" if query is not None else ""


def _queries_of(mapping) -> list[TableAlias]:
    """The queries classes of the mapping are mapped to, and those they read, each after the queries it reads."""
    found: list[TableAlias] = []

    def add(alias: TableAlias) -> None:
        if any(alias is f for f in found):
            return
        for read in alias.reads:
            if (read_query := query_of(read)) is not None:
                add(read_query)
        found.append(alias)

    for rcm in mapping.mappings:
        query = query_of(rcm.main_relation)
        if query is not None:
            add(query)
    return found


def query_definitions(mapping) -> str:
    """Python source, for the top of a finder module, that makes each query of the mapping, so that the module can read its columns and
    write it into a query. A query is built from its SQL where it is used, and not kept anywhere else."""
    lines = []
    for alias in _queries_of(mapping):
        reads = ", ".join(table_code(t) for t in alias.reads)
        columns = ", ".join(f"Column('{c.name}', '{c.type}'{', primary_key=True' if c.primary_key else ''}"
                            f"{'' if c.nullable or c.primary_key else ', nullable=False'})"
                            for c in alias.columns if isinstance(c, Column))
        milestoning = f", milestoning=lambda c: {_scheme_columns_code(alias, 'c')}" if alias.milestoning_scheme else ""
        lines.append(f"{_query_constant(alias)} = derived_table('{alias.name}', {alias.sql!r}, [{reads}], [{columns}], "
                     f"\"Query '{alias.name}'\"{milestoning})")
    return "\n".join(lines)


def condition_kw(join: Join, reverse: bool = False) -> str:
    """The ``condition=`` argument that gives a finder the condition of the join it follows, or nothing. ``reverse`` is that
    the finder is the one the join is followed to from its other end, which matters only to a join of a table to itself."""
    if join.condition is None:
        return ""
    self_join = _is_self_join(join.lhs, join.rhs)
    return (f", condition={condition_code(join.condition, self_join)}"
            + (", condition_reverse=True" if reverse and self_join else ""))


def _table_of(column: Column) -> Table:
    assert isinstance(column.table, Table), f"column '{column.name}' belongs to no table"
    return column.table


def parent_expr(join: Join, parent: str, with_dates: bool = False) -> str:
    """The parent step of the join to a property's class: ``parent`` itself, or, when the property is mapped through
    other tables first, ``parent`` followed by a step through each of them. ``with_dates`` gives each of those steps the
    ``business_date`` and ``processing_time`` the property was asked for with."""
    if not join.via:
        return parent
    hops = ", ".join(
        f"({column_code(lhs)}, {table_code(_table_of(rhs))}, {column_code(rhs)}"
        + (f", {condition_code(condition, _is_self_join(lhs, rhs))}" if condition is not None else "") + ")"
        for (lhs, rhs), condition in zip(join.via, join.via_conditions))
    dates = (", business_date=None if business_date is None else convert_date(business_date), "
             "processing_datetime=None if processing_time is None else convert_date_time(processing_time)") if with_dates else ""
    return f"via_joins({parent}, [{hops}]{dates})"


def display_name(prop: Property) -> str:
    return prop.name


def table_qualified_name(table) -> str:
    return table.qualified_name


def _mapping_to_class_name(name: str) -> str:
    parts = re.split(r'[\s_]+', name)
    return ''.join(p[0].upper() + p[1:] for p in parts if p) + 'Context'


def _mapping_to_filename(name: str) -> str:
    parts = re.split(r'[\s_]+', name)
    return '_'.join(p.lower() for p in parts if p) + '_context.py'


def _build_association_lookup(mapping: Mapping) -> dict:
    """Build (source_name, target_name, source_property) -> Association from all packages in the mapping."""
    result: dict = {}
    for rcm in mapping.mappings:
        if rcm.clazz.package:
            for child in rcm.clazz.package.children:
                if isinstance(child, Association):
                    result[(child.source, child.target, child.source_property.id)] = child
    return result


def _build_reverse_assoc_map(mapping: Mapping, assoc_lookup: dict) -> dict:
    """Return target_class_name -> list of (source_rcm, rpm, assoc, reverse_name)."""
    reverse_map: dict = {}
    for rcm in mapping.mappings:
        for rpm in rcm.property_mappings:
            if is_primitive(rpm.property) or not isinstance(rpm.target, Join) or rpm.target.via:
                continue
            target_cls = rpm.property.type
            if not isinstance(target_cls, Class):
                continue
            assoc = assoc_lookup.get((rcm.clazz.name, target_cls.name, rpm.property.id))
            if assoc is None:
                continue
            if assoc.target_property is None:
                continue
            reverse_name = to_snake_case(assoc.target_property.id)
            reverse_map.setdefault(target_cls.name, []).append((rcm, rpm, assoc, reverse_name))
    return reverse_map


def _class_package(clazz) -> str:
    """Return the dotted package name for a class, or empty string if none."""
    if clazz.package and clazz.package.name and '.' in clazz.package.name:
        return clazz.package.name
    return ''


def _ensure_package_dirs(base_dir: str, package_name: str) -> str:
    """Create directory hierarchy and __init__.py for a dotted package name.

    Returns the leaf directory path.
    """
    current = base_dir
    for part in package_name.split('.'):
        current = os.path.join(current, part)
        os.makedirs(current, exist_ok=True)
        init_file = os.path.join(current, '__init__.py')
        if not os.path.exists(init_file):
            open(init_file, 'w').close()
    return current


def generate(mapping: Mapping, output_directory):
    environment = Environment(loader=PackageLoader("datafinder_generator"), trim_blocks=True, lstrip_blocks=True)
    finder_template = environment.get_template("finder_template.txt")
    base_template = environment.get_template("finder_base_template.txt")
    context_template = environment.get_template("context_template.txt")

    class_module_map = {}
    class_module_map_base = {}
    for rcm in mapping.mappings:
        pkg = _class_package(rcm.clazz)
        impl_module = f"{rcm.clazz.name.lower()}_finder"
        base_module = f"{rcm.clazz.name.lower()}_finder_base"
        class_module_map[rcm.clazz.name] = f"{pkg}.{impl_module}" if pkg else impl_module
        class_module_map_base[rcm.clazz.name] = f"{pkg}.{base_module}" if pkg else base_module

    assoc_lookup = _build_association_lookup(mapping)
    reverse_assoc_map = _build_reverse_assoc_map(mapping, assoc_lookup)

    shared_context = dict(
        query_definitions=query_definitions(mapping),
        class_module_map=class_module_map,
        class_module_map_base=class_module_map_base,
        is_primitive=is_primitive,
        has_processing_temporal=has_processing_temporal,
        has_uni_business_temporal=has_uni_business_temporal,
        has_business_date_and_processing=has_business_date_and_processing,
        has_bitemporal=has_bitemporal,
        has_business_date_range=has_business_date_range,
        has_processing_snapshot=has_processing_snapshot,
        display_name=display_name,
        to_python_name=to_python_name,
        table_qualified_name=table_qualified_name,
        to_snake_case=to_snake_case,
        flatten_embedded=flatten_embedded,
        embedded_optional=embedded_optional,
        embedded_classes=embedded_classes,
        parent_expr=parent_expr,
        condition_kw=condition_kw,
        query_assignment=query_assignment,
        column_code=column_code,
        owner_code=owner_code,
    )

    for rcm in mapping.mappings:
        pkg = _class_package(rcm.clazz)
        out_dir = _ensure_package_dirs(output_directory, pkg) if pkg else output_directory
        reverse_assocs = reverse_assoc_map.get(rcm.clazz.name, [])
        render_ctx = dict(shared_context, rcm=_ClassView(rcm), reverse_assocs=reverse_assocs)

        base_filename = f"{rcm.clazz.name.lower()}_finder_base.py"
        base_filepath = os.path.join(out_dir, base_filename)
        with open(base_filepath, mode="w", encoding="utf-8") as f:
            f.write(base_template.render(**render_ctx))
            print(f"... wrote {base_filename}")

        impl_filename = f"{rcm.clazz.name.lower()}_finder.py"
        impl_filepath = os.path.join(out_dir, impl_filename)
        with open(impl_filepath, mode="w", encoding="utf-8") as f:
            f.write(finder_template.render(**render_ctx))
            print(f"... wrote {impl_filename}")

    context_filename = _mapping_to_filename(mapping.name)
    context_filepath = os.path.join(output_directory, context_filename)
    context_class_name = _mapping_to_class_name(mapping.name)
    with open(context_filepath, mode="w", encoding="utf-8") as f:
        f.write(context_template.render(**shared_context, mapping=mapping,
                                        context_class_name=context_class_name))
        print(f"... wrote {context_filename}")
