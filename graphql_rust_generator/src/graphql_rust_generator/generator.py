"""
Generates a complete, compilable Rust async-graphql / axum project from a
relational Mapping object.

Supported backends
------------------
postgres    Uses sqlx + PgPool. Connect via DATABASE_URL env var.
databricks  Uses reqwest against the Databricks SQL Statement Execution API.
            Connect via DATABRICKS_HOST, DATABRICKS_WAREHOUSE_ID, DATABRICKS_TOKEN.

The generated project exposes:
  GET  /           — GraphiQL browser playground
  POST /graphql    — GraphQL endpoint
  GET  /graphql    — GraphQL endpoint (for tools that use GET)
"""
import os
import re

from jinja2 import Environment, FileSystemLoader

from model.mapping import (
    Mapping,
    ProcessingDateMilestonesPropertyMapping,
    SingleBusinessDateMilestonePropertyMapping,
    BusinessDateAndProcessingMilestonePropertyMapping,
    BiTemporalMilestonePropertyMapping,
)
from model.m3 import PrimitiveType
from model.relational_mapping import RelationalClassMapping, Join

BACKENDS = ("postgres", "databricks")

# ---------------------------------------------------------------------------
# Type tables
# ---------------------------------------------------------------------------

_SQL_TO_RUST: dict[str, str] = {
    "INT":       "i32",
    "INTEGER":   "i32",
    "BIGINT":    "i64",
    "SMALLINT":  "i16",
    "VARCHAR":   "String",
    "STRING":    "String",
    "TEXT":      "String",
    "CHAR":      "String",
    "DOUBLE":    "f64",
    "FLOAT":     "f64",
    "REAL":      "f64",
    "NUMERIC":   "f64",
    "DECIMAL":   "f64",
    "BOOLEAN":   "bool",
    "BOOL":      "bool",
    "DATE":      "chrono::NaiveDate",
    "TIMESTAMP": "chrono::NaiveDateTime",
}

_RUST_KEYWORDS = {
    "as", "break", "const", "continue", "crate", "else", "enum", "extern",
    "false", "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod",
    "move", "mut", "pub", "ref", "return", "self", "static", "struct",
    "super", "trait", "true", "type", "unsafe", "use", "where", "while",
    "async", "await", "dyn",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rust_type(col_type: str, nullable: bool = False) -> str:
    base = _SQL_TO_RUST.get(col_type.upper(), "String")
    return f"Option<{base}>" if nullable else base


def _deser_fn(rust_type: str) -> str:
    """Name of the JSON-value coercion function for a Rust type (Databricks backend)."""
    if "Option<chrono::NaiveDateTime>" in rust_type:
        return "as_opt_naive_dt"
    if "NaiveDateTime" in rust_type:
        return "as_naive_dt"
    if "NaiveDate" in rust_type:
        return "as_naive_date"
    if "i64" in rust_type:
        return "as_i64"
    if "i32" in rust_type:
        return "as_i32"
    if "f64" in rust_type:
        return "as_f64"
    if "bool" in rust_type:
        return "as_bool"
    return "as_string"


def _to_snake(name: str) -> str:
    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', s)
    return s.lower()


def _safe(name: str) -> str:
    return f"{name}_" if name in _RUST_KEYWORDS else name


def _resolver_name(class_name: str) -> str:
    return _to_snake(class_name) + "s"


def _nullable_prop_ids(cm: RelationalClassMapping) -> set[str]:
    result: set[str] = set()
    mm = cm.milestone_mapping
    if isinstance(mm, ProcessingDateMilestonesPropertyMapping):
        result.add(mm._out.property.id)
    if isinstance(mm, BiTemporalMilestonePropertyMapping):
        result.add(mm._date_to.property.id)
    return result


def _milestone_kind(cm: RelationalClassMapping) -> dict:
    mm = cm.milestone_mapping
    if mm is None:
        return {"kind": None}
    if isinstance(mm, BusinessDateAndProcessingMilestonePropertyMapping):
        return {"kind": "business_date_processing",
                "in_col": mm._in.target.name, "out_col": mm._out.target.name,
                "date_col": mm._date.target.name}
    if isinstance(mm, BiTemporalMilestonePropertyMapping):
        return {"kind": "bitemporal",
                "in_col": mm._in.target.name, "out_col": mm._out.target.name,
                "date_from_col": mm._date_from.target.name,
                "date_to_col": mm._date_to.target.name}
    if isinstance(mm, ProcessingDateMilestonesPropertyMapping):
        return {"kind": "processing",
                "in_col": mm._in.target.name, "out_col": mm._out.target.name}
    if isinstance(mm, SingleBusinessDateMilestonePropertyMapping):
        return {"kind": "business_date", "date_col": mm._date.target.name}
    return {"kind": None}

# ---------------------------------------------------------------------------
# SQL builders (run in Python so templates stay clean)
# ---------------------------------------------------------------------------

def _select_sql(fields: list[dict], fk_fields: list[dict], table: str) -> str:
    cols = [f"{f['col_name']} AS {f['rust_name']}" for f in fields]
    cols += [f"{f['col_name']} AS {f['rust_name']}" for f in fk_fields]
    return f"SELECT {', '.join(cols)} FROM {table}"


def _assoc_select_sql(target_fields: list[dict], target_table: str,
                      target_pk_col: str) -> str:
    cols = [f"{f['col_name']} AS {f['rust_name']}" for f in target_fields]
    return f"SELECT {', '.join(cols)} FROM {target_table} WHERE {target_pk_col} = $1"


def _assoc_select_sql_databricks(target_fields: list[dict], target_table: str,
                                  target_pk_col: str) -> str:
    cols = [f"{f['col_name']} AS {f['rust_name']}" for f in target_fields]
    return f"SELECT {', '.join(cols)} FROM {target_table} WHERE {target_pk_col} = :pk"

# ---------------------------------------------------------------------------
# Per-class context builder
# ---------------------------------------------------------------------------

def _class_ctx(cm: RelationalClassMapping, class_index: dict) -> dict:
    nullable = _nullable_prop_ids(cm)

    prim_pms = [pm for pm in cm.property_mappings
                if isinstance(pm.property.type, PrimitiveType)]
    join_pms = [pm for pm in cm.property_mappings
                if isinstance(pm.target, Join)]

    fields = []
    for pm in prim_pms:
        snake    = _safe(_to_snake(pm.property.id))
        rt       = _rust_type(pm.target.type, pm.property.id in nullable)
        fields.append({
            "rust_name": snake,
            "rust_type": rt,
            "col_name":  pm.target.name,
            "deser_fn":  _deser_fn(rt),
        })

    fk_fields = []
    for pm in join_pms:
        lhs = pm.target.lhs
        rt  = _rust_type(lhs.type)
        fk_fields.append({
            "rust_name": _to_snake(pm.property.id) + "_fk",
            "rust_type": rt,
            "col_name":  lhs.name,
            "deser_fn":  _deser_fn(rt),
        })

    table     = prim_pms[0].target.table.qualified_name if prim_pms else ""
    milestone = _milestone_kind(cm)
    has_proc  = milestone["kind"] in ("processing", "business_date_processing", "bitemporal")
    has_bd    = milestone["kind"] in ("business_date", "business_date_processing", "bitemporal")

    associations = []
    for pm in join_pms:
        target_cls = pm.property.type
        target_cm  = class_index.get(target_cls.name)
        if target_cm is None:
            continue
        target_ctx   = _class_ctx(target_cm, class_index)
        target_table = pm.target.rhs.table.qualified_name
        target_pk    = pm.target.rhs.name
        associations.append({
            "nav_name":      _safe(_to_snake(pm.property.id)),
            "target_struct": target_cls.name,
            "fk_rust_name":  _to_snake(pm.property.id) + "_fk",
            "fk_rust_type":  _rust_type(pm.target.lhs.type),
            "fk_deser_fn":   _deser_fn(_rust_type(pm.target.lhs.type)),
            "assoc_sql":     _assoc_select_sql(
                                 target_ctx["fields"], target_table, target_pk),
            "assoc_sql_db":  _assoc_select_sql_databricks(
                                 target_ctx["fields"], target_table, target_pk),
        })

    return {
        "struct_name":    cm.clazz.name,
        "resolver_name":  _resolver_name(cm.clazz.name),
        "fields":         fields,
        "fk_fields":      fk_fields,
        "has_assoc":      bool(associations),
        "associations":   associations,
        "table":          table,
        "select_sql":     _select_sql(fields, fk_fields, table),
        "milestone":      milestone,
        "has_processing": has_proc,
        "has_bd":         has_bd,
    }

# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate(mapping: Mapping, output_directory: str,
             project_name: str | None = None,
             backend: str = "postgres") -> None:
    """Write a complete Rust GraphQL server project to *output_directory*.

    Args:
        mapping:          Loaded relational Mapping.
        output_directory: Root of the Rust project to create.
        project_name:     Cargo package name (defaults to snake_case mapping name).
        backend:          One of 'postgres' or 'databricks'.
    """
    if backend not in BACKENDS:
        raise ValueError(f"Unknown backend {backend!r}. Choose one of: {BACKENDS}")

    templates_dir = os.path.join(os.path.dirname(__file__), "templates", backend)
    env = Environment(
        loader=FileSystemLoader(templates_dir),
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )

    if project_name is None:
        project_name = _to_snake(mapping.name).replace(" ", "-")

    class_index = {
        cm.clazz.name: cm
        for cm in mapping.mappings
        if isinstance(cm, RelationalClassMapping)
    }

    classes = [
        _class_ctx(cm, class_index)
        for cm in mapping.mappings
        if isinstance(cm, RelationalClassMapping)
    ]

    ctx = dict(
        project_name = project_name,
        mapping_name = mapping.name,
        classes      = classes,
        need_proc    = any(c["has_processing"] for c in classes),
        need_bd      = any(c["has_bd"]         for c in classes),
    )

    src_dir = os.path.join(output_directory, "src")
    os.makedirs(src_dir, exist_ok=True)

    for tmpl_name, out_path in [
        ("Cargo.toml.j2", os.path.join(output_directory, "Cargo.toml")),
        ("main.rs.j2",    os.path.join(src_dir, "main.rs")),
        ("schema.rs.j2",  os.path.join(src_dir, "schema.rs")),
    ]:
        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write(env.get_template(tmpl_name).render(**ctx))
        print(f"... wrote {out_path}")
