import logging
import re
from typing import Optional

from model.relational import Repository, Table

_log = logging.getLogger(__name__)
_TABLE_HEADING_RE = re.compile(r"^####\s+Table:\s+(\S+)\s*→")
_SCHEMA_HEADING_RE = re.compile(r"^###\s+Schema:\s+(.+)$")


def refresh_mapping(mapping_path: str, new_repo: Repository, output_path: str = None) -> str:
    """
    Refresh an existing mapping markdown from a new repository schema snapshot.

    - New columns (in new_repo but not in mapping) are appended with empty Property
    - Deleted columns (in mapping but not in new_repo) are removed
    - Existing columns keep their Property/Key; Type is updated from new schema
    - Table headings (→ ClassName, milestoning) and associations are preserved unchanged
    - Tables in new_repo not yet in mapping are appended as draft sections
    - Tables in mapping not in new_repo are kept with a warning

    If output_path is supplied the result is also written to that file.
    Returns the updated markdown string.
    """
    with open(mapping_path, encoding="utf-8") as f:
        content = f.read()
    result = refresh_mapping_content(content, new_repo)
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(result)
    return result


def refresh_mapping_content(content: str, new_repo: Repository) -> str:
    """Core logic — operates on strings (no file I/O)."""
    new_schema_tables: dict[str, dict[str, Table]] = {
        schema.name: {t.name: t for t in schema.tables}
        for schema in new_repo.schemas
    }

    lines = content.splitlines()
    preamble, sections = _parse_into_sections(lines)

    result_lines = list(preamble)
    schemas_seen = {name for name, _ in sections}
    all_tables_processed: set[str] = set()

    for schema_name, schema_lines in sections:
        new_tables = new_schema_tables.get(schema_name, {})
        updated_lines, tables_processed = _process_schema_section(schema_lines, new_tables)
        all_tables_processed.update(tables_processed)

        for table_name, table in new_tables.items():
            if table_name not in tables_processed:
                _log.info("Adding new table '%s' to schema '%s'", table_name, schema_name)
                if updated_lines and updated_lines[-1].strip() != "":
                    updated_lines.append("")
                updated_lines.extend(_draft_table_lines(table))
                all_tables_processed.add(table_name)

        result_lines.extend(updated_lines)

    for schema in new_repo.schemas:
        if schema.name in schemas_seen:
            continue
        new_in_schema = [t for t in schema.tables if t.name not in all_tables_processed]
        if new_in_schema:
            result_lines.append("")
            result_lines.append(f"### Schema: {schema.name}")
            result_lines.append("")
            for table in new_in_schema:
                result_lines.extend(_draft_table_lines(table))
                all_tables_processed.add(table.name)

    return "\n".join(result_lines)


def _parse_into_sections(
    lines: list[str],
) -> tuple[list[str], list[tuple[str, list[str]]]]:
    """Split document lines into (preamble, [(schema_name, schema_lines), ...])."""
    preamble: list[str] = []
    sections: list[tuple[str, list[str]]] = []
    current_name: Optional[str] = None
    current_lines: list[str] = []

    for line in lines:
        m = _SCHEMA_HEADING_RE.match(line)
        if m:
            if current_name is not None:
                sections.append((current_name, current_lines))
            current_name = m.group(1).strip()
            current_lines = [line]
        elif current_name is not None:
            current_lines.append(line)
        else:
            preamble.append(line)

    if current_name is not None:
        sections.append((current_name, current_lines))

    return preamble, sections


def _process_schema_section(
    schema_lines: list[str],
    new_tables: dict[str, Table],
) -> tuple[list[str], set[str]]:
    """Process lines within one schema section, returning (updated_lines, tables_processed)."""
    output: list[str] = []
    tables_processed: set[str] = set()
    i = 0

    while i < len(schema_lines):
        line = schema_lines[i]
        m = _TABLE_HEADING_RE.match(line)

        if m:
            table_name = m.group(1)
            output.append(line)
            i += 1

            while i < len(schema_lines) and schema_lines[i].strip() == "":
                output.append(schema_lines[i])
                i += 1

            if i < len(schema_lines) and schema_lines[i].lstrip().startswith("|"):
                table_lines: list[str] = []
                while i < len(schema_lines) and schema_lines[i].lstrip().startswith("|"):
                    table_lines.append(schema_lines[i])
                    i += 1

                if table_name in new_tables:
                    tables_processed.add(table_name)
                    output.extend(_update_column_table(table_lines, new_tables[table_name]).splitlines())
                else:
                    _log.warning("Table '%s' not found in new repository — keeping as-is", table_name)
                    output.extend(table_lines)
        else:
            output.append(line)
            i += 1

    return output, tables_processed


def _update_column_table(table_lines: list[str], new_table: Table) -> str:
    """
    Merge existing column-table lines with new_table schema:
    - Keep rows for columns still in new_table (Type updated; Property and Key preserved)
    - Remove rows for columns absent from new_table
    - Append rows for columns new in new_table (empty Property)
    """
    if len(table_lines) < 2:
        return "\n".join(table_lines)

    headers = [h.strip() for h in table_lines[0].strip().strip("|").split("|")]
    if "Column" not in headers:
        return "\n".join(table_lines)

    existing_rows: list[dict[str, str]] = []
    for line in table_lines[2:]:  # skip header and separator
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        row = {headers[j]: cells[j] if j < len(cells) else "" for j in range(len(headers))}
        if row.get("Column"):
            existing_rows.append(row)

    new_cols = {col.name: col for col in new_table.columns}
    existing_col_names = {r["Column"] for r in existing_rows}

    merged: list[dict[str, str]] = []
    for row in existing_rows:
        col_name = row["Column"]
        if col_name in new_cols:
            updated = dict(row)
            if "Type" in updated:
                updated["Type"] = new_cols[col_name].type or ""
            merged.append(updated)
        else:
            _log.info("Removing deleted column '%s' from table '%s'", col_name, new_table.name)

    for col in new_table.columns:
        if col.name not in existing_col_names:
            _log.info("Adding new column '%s' to table '%s'", col.name, new_table.name)
            new_row: dict[str, str] = {h: "" for h in headers}
            new_row["Column"] = col.name
            if "Type" in new_row:
                new_row["Type"] = col.type or ""
            if "Key" in new_row:
                new_row["Key"] = "PK" if col.primary_key else ""
            merged.append(new_row)

    return _md_table(headers, [[r.get(h, "") for h in headers] for r in merged])


def _draft_table_lines(table: Table) -> list[str]:
    col_rows = [
        [col.name, col.type or "", "PK" if col.primary_key else "", ""]
        for col in table.columns
    ]
    lines = [f"#### Table: {table.name} → ?", ""]
    lines.extend(_md_table(["Column", "Type", "Key", "Property"], col_rows).splitlines())
    lines.append("")
    return lines


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(cell))

    def fmt_row(cells: list[str]) -> str:
        return "| " + " | ".join(c.ljust(col_widths[i]) for i, c in enumerate(cells)) + " |"

    separator = "| " + " | ".join("-" * w for w in col_widths) + " |"
    return "\n".join([fmt_row(headers), separator] + [fmt_row(r) for r in rows])
