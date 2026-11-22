"""
The specs as files that any language can read: a JSON file for each expectation, and a CSV of the result it expects.

``export`` writes them from the Python specs, which stay the place where a spec is written: the expected result is worked out there,
independently of the engine. ``load`` reads them back as expectations that run through the same checks as the Python ones.

    <suite>/<name>.json   the query (see query_json), the finder and mapping, the file of the result, and the checks
    <suite>/<name>.csv    the expected result, with the type of each column (see typed_csv)

Run ``uv run datafinder_examples/tools/write_spec_files.py`` to write them into datafinder_specs/specs.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from datafinder_examples_tests import query_json, typed_csv
from datafinder_examples_tests.spec import FinderSpec, TestExpectation


def suites() -> dict[str, list[FinderSpec]]:
    """The specs that are written as files, by the name of the folder they go in."""
    from datafinder_examples_tests.companies_specs import COMPANY_FINDER_SPECS
    return {"companies": [COMPANY_FINDER_SPECS]}


def _types(expectation: TestExpectation) -> list[str]:
    if expectation.expected_types is not None:
        return expectation.expected_types
    result = expectation.expected_result
    if result.shape[0] == 0:
        raise ValueError(f"[{expectation.name}] An empty result has no type to work out: declare it in expected_types")
    return [typed_csv.column_type(list(result[:, c])) for c in range(result.shape[1])]


def _json(document: dict) -> str:
    """The document with a key on each line, and the query tree, which is deep, on one line."""
    lines = []
    for key, value in document.items():
        compact = key == "query"
        text = json.dumps(value, ensure_ascii=False, separators=(",", ":")) if compact else json.dumps(value, ensure_ascii=False, indent=2)
        lines.append(f"  {json.dumps(key)}: {text.replace(chr(10), chr(10) + '  ')}")
    return "{\n" + ",\n".join(lines) + "\n}\n"


def export(suite: str, specs: list[FinderSpec]) -> dict[str, str]:
    """The files of a suite, by their path under the specs folder."""
    files: dict[str, str] = {}
    for spec in specs:
        for expectation in spec.expectations:
            path = f"{suite}/{expectation.name}"
            if f"{path}.json" in files:
                raise ValueError(f"The name {expectation.name!r} is used twice in the suite {suite}")
            query = query_json.record(expectation.query)
            document: dict = {"name": expectation.name, "finder": spec.finder_name, "mapping": spec.mapping_file,
                              "query_text": query_json.render(query), "query": query, "result": f"{expectation.name}.csv"}
            if expectation.approximate:
                document["approximate"] = True
            if expectation.expected_sql:
                document["sql_contains"] = expectation.expected_sql
            if expectation.unexpected_sql:
                document["sql_not_contains"] = expectation.unexpected_sql
            if expectation.skip_backends:
                document["skip"] = expectation.skip_backends
            files[f"{path}.json"] = _json(document)
            files[f"{path}.csv"] = typed_csv.write(expectation.expected_columns, _types(expectation), expectation.expected_result)
    return files


def export_all() -> dict[str, str]:
    return {path: text for suite, specs in suites().items() for path, text in export(suite, specs).items()}


def specs_folder() -> Path:
    from datafinder_examples import specs_root
    return specs_root() / "specs"


def load(suite: str) -> tuple[str, list[TestExpectation]]:
    """The mapping a suite reads and its expectations, from the files in the specs folder."""
    folder = specs_folder() / suite
    mapping = None
    expectations = []
    for path in sorted(folder.glob("*.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        mapping = document["mapping"] if mapping is None else mapping
        if document["mapping"] != mapping:
            raise ValueError(f"The suite {suite} reads more than one mapping")
        columns, _, rows = typed_csv.read((folder / document["result"]).read_text(encoding="utf-8"))
        node = document["query"]
        expectations.append(TestExpectation(
            name=document["name"], query=lambda f, node=node: query_json.replay(node, f), expected_columns=columns,
            expected_result=np.asarray(rows, dtype=object), expected_sql=document.get("sql_contains", {}),
            unexpected_sql=document.get("sql_not_contains", {}), skip_backends=document.get("skip", {}),
            approximate=document.get("approximate", False)))
    if mapping is None:
        raise ValueError(f"No specs in {folder}")
    return mapping, expectations


def write_all() -> None:
    root = specs_folder()
    for suite in suites():
        folder = root / suite
        for old in folder.glob("*") if folder.exists() else []:
            old.unlink()
    for path, text in export_all().items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    write_all()
