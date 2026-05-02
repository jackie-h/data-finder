"""
refresh_mapping.py — update a mapping markdown from a live DuckDB schema.

Usage:
    uv run example/refresh_mapping.py <db_path> <mapping_path> [--out <output_path>] [--repo-name <name>]

If --out is omitted the mapping file is updated in-place.
"""
import argparse
import sys

from datafinder_duckdb.duckdb_reader import read_repository_from_duckdb
from mapping_markdown.refresh import refresh_mapping


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh a mapping markdown from a DuckDB catalog/schema."
    )
    parser.add_argument("db_path", help="Path to the DuckDB database file")
    parser.add_argument("mapping_path", help="Path to the existing mapping markdown file")
    parser.add_argument("--out", dest="output_path", default=None,
                        help="Output path (default: overwrite mapping_path)")
    parser.add_argument("--repo-name", dest="repo_name", default=None,
                        help="Override the repository name (default: derived from db_path)")
    args = parser.parse_args()

    output_path = args.output_path or args.mapping_path

    print(f"Reading schema from {args.db_path} …")
    repo = read_repository_from_duckdb(args.db_path, args.repo_name)
    schemas = [s.name for s in repo.schemas]
    tables = [t.name for s in repo.schemas for t in s.tables]
    print(f"  Found {len(schemas)} schema(s), {len(tables)} table(s): {', '.join(tables)}")

    print(f"Refreshing {args.mapping_path} …")
    refresh_mapping(args.mapping_path, repo, output_path=output_path)
    print(f"  Written to {output_path}")


if __name__ == "__main__":
    main()
