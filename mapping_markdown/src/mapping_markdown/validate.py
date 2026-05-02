import os
from dataclasses import dataclass, field

from markdown_it import MarkdownIt
from markdown_it.tree import SyntaxTreeNode

_md_parser = MarkdownIt().enable("table")


@dataclass
class ValidationError:
    location: str
    message: str

    def __str__(self) -> str:
        return f"{self.location}: {self.message}"


@dataclass
class ValidationResult:
    errors: list[ValidationError] = field(default_factory=list)

    @property
    def valid(self) -> bool:
        return len(self.errors) == 0

    def __str__(self) -> str:
        if self.valid:
            return "OK"
        return "\n".join(str(e) for e in self.errors)


def validate_file(path: str) -> ValidationResult:
    with open(path, encoding="utf-8") as f:
        content = f.read()
    base_dir = os.path.dirname(os.path.abspath(path))
    return validate(content, base_dir=base_dir, source=os.path.basename(path))


def validate(content: str, base_dir: str = None, source: str = "<mapping>") -> ValidationResult:
    root = SyntaxTreeNode(_md_parser.parse(content))
    nodes = root.children
    result = ValidationResult()
    _validate_nodes(nodes, base_dir, source, result)
    return result


def _validate_nodes(nodes: list, base_dir: str, source: str, result: ValidationResult) -> None:
    i = 0
    while i < len(nodes):
        node = nodes[i]
        if node.type == "heading":
            text = node.children[0].content if node.children else ""
            level = node.tag

            if level == "h2" and text.startswith("Schema:") and text[len("Schema:"):].strip().endswith(".md"):
                if base_dir is not None:
                    filename = text[len("Schema:"):].strip()
                    file_path = os.path.join(base_dir, filename)
                    if not os.path.exists(file_path):
                        result.errors.append(ValidationError(
                            source, f"Schema include file not found: '{filename}'"
                        ))
                    else:
                        child_result = validate_file(file_path)
                        result.errors.extend(child_result.errors)

            elif level == "h4" and text.startswith("Table:"):
                table_text = text[len("Table:"):].strip()
                i += 1
                if i < len(nodes) and nodes[i].type == "table":
                    col_names = _column_names_from_table(nodes[i])
                    seen = set()
                    for col_name in col_names:
                        if col_name in seen:
                            result.errors.append(ValidationError(
                                source, f"Duplicate column '{col_name}' in table '{table_text}'"
                            ))
                        seen.add(col_name)
                continue

        i += 1


def _column_names_from_table(node: SyntaxTreeNode) -> list[str]:
    thead, tbody = node.children[0], node.children[1]
    headers = [c.children[0].content for c in thead.children[0].children]
    if "Column" not in headers:
        return []
    col_idx = headers.index("Column")
    names = []
    for tr in tbody.children:
        cells = [c.children[0].content if c.children else "" for c in tr.children]
        name = cells[col_idx].strip() if col_idx < len(cells) else ""
        if name:
            names.append(name)
    return names
