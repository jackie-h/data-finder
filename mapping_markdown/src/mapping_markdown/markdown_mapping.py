import logging
import re
from dataclasses import dataclass, field
from typing import Optional

from markdown_it import MarkdownIt
from markdown_it.tree import SyntaxTreeNode

_md_parser = MarkdownIt().enable("table")
_log = logging.getLogger(__name__)

_TABLE_RE = re.compile(r'(\S+)\s*→\s*(\w+)(?:\s*\(milestoning:\s*(\w+)\))?')


@dataclass
class MilestoningScheme:
    name: str
    processing_start: Optional[str] = None
    processing_end: Optional[str] = None
    business_date: Optional[str] = None


@dataclass
class ColumnMapping:
    column: str
    property: str


@dataclass
class MilestoningOverride:
    scheme: str
    milestoning: str
    column: str


@dataclass
class AssociationMapping:
    name: str
    source_column: str
    target_table: str
    target_column: str


@dataclass
class TableMapping:
    table: str
    cls: str
    milestoning_scheme: Optional[str] = None
    column_mappings: list = field(default_factory=list)
    milestoning_overrides: list = field(default_factory=list)


@dataclass
class SchemaMapping:
    schema: str
    table_mappings: list = field(default_factory=list)
    association_mappings: list = field(default_factory=list)


@dataclass
class RepositoryMapping:
    name: str
    milestoning_schemes: list = field(default_factory=list)
    schema_mappings: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Load: markdown → model
# ---------------------------------------------------------------------------

def load(path: str) -> list[RepositoryMapping]:
    with open(path, encoding="utf-8") as f:
        content = f.read()
    return loads(content)


def loads(content: str) -> list[RepositoryMapping]:
    root = SyntaxTreeNode(_md_parser.parse(content))
    nodes = root.children

    repositories: list[RepositoryMapping] = []
    current_repo: Optional[RepositoryMapping] = None
    current_schema: Optional[SchemaMapping] = None

    i = 0
    while i < len(nodes):
        node = nodes[i]

        if node.type == "heading":
            text = node.children[0].content if node.children else ""
            level = node.tag

            if level == "h2" and text.startswith("Repository:"):
                current_repo = RepositoryMapping(text[len("Repository:"):].strip())
                repositories.append(current_repo)
                i += 1
                if i < len(nodes) and nodes[i].type == "table":
                    for row in _parse_ast_table(nodes[i]):
                        current_repo.milestoning_schemes.append(MilestoningScheme(
                            name=row.get("Scheme", "").strip(),
                            processing_start=row.get("processing_start", "").strip() or None,
                            processing_end=row.get("processing_end", "").strip() or None,
                            business_date=row.get("business_date", "").strip() or None,
                        ))
                    i += 1
                continue

            elif level == "h3" and text.startswith("Schema:"):
                current_schema = SchemaMapping(text[len("Schema:"):].strip())
                if current_repo is not None:
                    current_repo.schema_mappings.append(current_schema)

            elif level == "h4" and text.startswith("Table:"):
                m = _TABLE_RE.match(text[len("Table:"):].strip())
                if m is None:
                    i += 1
                    continue
                table_mapping = TableMapping(
                    table=m.group(1),
                    cls=m.group(2),
                    milestoning_scheme=m.group(3),
                )
                if current_schema is not None:
                    current_schema.table_mappings.append(table_mapping)
                i += 1

                if i < len(nodes) and nodes[i].type == "table":
                    for row in _parse_ast_table(nodes[i]):
                        col = row.get("Column", "").strip()
                        prop = row.get("Property", "").strip()
                        if col:
                            table_mapping.column_mappings.append(ColumnMapping(col, prop))
                    i += 1

                if i < len(nodes) and nodes[i].type == "table":
                    rows = _parse_ast_table(nodes[i])
                    if rows and "Scheme" in rows[0] and "Milestoning" in rows[0]:
                        for row in rows:
                            table_mapping.milestoning_overrides.append(MilestoningOverride(
                                scheme=row.get("Scheme", "").strip(),
                                milestoning=row.get("Milestoning", "").strip(),
                                column=row.get("Column", "").strip(),
                            ))
                        i += 1
                continue

            elif level == "h4" and text.startswith("Association:"):
                assoc_name = text[len("Association:"):].strip()
                i += 1
                if i < len(nodes) and nodes[i].type == "table":
                    for row in _parse_ast_table(nodes[i]):
                        if current_schema is not None:
                            current_schema.association_mappings.append(AssociationMapping(
                                name=assoc_name,
                                source_column=row.get("Source Column", "").strip(),
                                target_table=row.get("Target Table", "").strip(),
                                target_column=row.get("Target Column", "").strip(),
                            ))
                    i += 1
                continue

        i += 1

    return repositories


def _parse_ast_table(node: SyntaxTreeNode) -> list[dict]:
    thead, tbody = node.children[0], node.children[1]
    headers = [c.children[0].content for c in thead.children[0].children]
    rows = []
    for tr in tbody.children:
        cells = [c.children[0].content if c.children else "" for c in tr.children]
        rows.append({headers[i]: cells[i] if i < len(cells) else "" for i in range(len(headers))})
    return rows


# ---------------------------------------------------------------------------
# Save: model → markdown
# ---------------------------------------------------------------------------

def save(path: str, title: str, repositories: list[RepositoryMapping]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(to_markdown(title, repositories))


def to_markdown(title: str, repositories: list[RepositoryMapping]) -> str:
    lines: list[str] = [f"# {title}", ""]

    for repo in repositories:
        lines.append(f"## Repository: {repo.name}")
        lines.append("")

        if repo.milestoning_schemes:
            scheme_rows = [
                [s.name, s.processing_start or "", s.processing_end or "", s.business_date or ""]
                for s in repo.milestoning_schemes
            ]
            lines.append(_md_table(["Scheme", "processing_start", "processing_end", "business_date"], scheme_rows))
            lines.append("")

        for schema in repo.schema_mappings:
            lines.append(f"### Schema: {schema.schema}")
            lines.append("")

            for table in schema.table_mappings:
                heading = f"#### Table: {table.table} → {table.cls}"
                if table.milestoning_scheme:
                    heading += f" (milestoning: {table.milestoning_scheme})"
                lines.append(heading)
                lines.append("")

                col_rows = [[cm.column, cm.property] for cm in table.column_mappings]
                lines.append(_md_table(["Column", "Property"], col_rows))
                lines.append("")

                if table.milestoning_overrides:
                    override_rows = [[mo.scheme, mo.milestoning, mo.column] for mo in table.milestoning_overrides]
                    lines.append(_md_table(["Scheme", "Milestoning", "Column"], override_rows))
                    lines.append("")

            for assoc in schema.association_mappings:
                lines.append(f"#### Association: {assoc.name}")
                lines.append("")
                lines.append(_md_table(
                    ["Source Column", "Target Table", "Target Column"],
                    [[assoc.source_column, assoc.target_table, assoc.target_column]],
                ))
                lines.append("")

    return "\n".join(lines)


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    def fmt_row(cells: list[str]) -> str:
        return "| " + " | ".join(c.ljust(col_widths[i]) for i, c in enumerate(cells)) + " |"

    separator = "| " + " | ".join("-" * w for w in col_widths) + " |"
    return "\n".join([fmt_row(headers), separator] + [fmt_row(r) for r in rows])
