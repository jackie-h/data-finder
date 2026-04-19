import duckdb

from model.relational import Repository, Schema, Table, Column

_SYSTEM_SCHEMAS = {"information_schema", "pg_catalog"}


def read_repository_from_duckdb(db_path: str, repo_name: str = None) -> Repository:
    """Build a Repository by introspecting a DuckDB database schema."""
    name = repo_name or db_path
    repo = Repository(name, f"duckdb://{db_path}")

    conn = duckdb.connect(db_path, read_only=True)
    try:
        rows = conn.execute("""
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name, ordinal_position
        """).fetchall()
    finally:
        conn.close()

    schemas: dict[str, Schema] = {}
    table_cols: dict[tuple, list] = {}

    for schema_name, table_name, col_name, data_type in rows:
        if schema_name not in schemas:
            schemas[schema_name] = Schema(schema_name, repo)
        key = (schema_name, table_name)
        if key not in table_cols:
            table_cols[key] = []
        table_cols[key].append((col_name, data_type))

    for (schema_name, table_name), cols in table_cols.items():
        columns = [Column(col_name, dtype) for col_name, dtype in cols]
        Table(table_name, columns, schemas[schema_name])

    return repo
