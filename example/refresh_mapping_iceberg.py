"""
refresh_mapping_iceberg.py — update a mapping markdown from an Iceberg REST catalog.

Usage:
    uv run example/refresh_mapping_iceberg.py <catalog_uri> <mapping_path> \
        [--out <output_path>] [--repo-name <name>] \
        [--credential KEY=VALUE ...] [--skip-errors]

If --out is omitted the mapping file is updated in-place.

Examples:
    uv run example/refresh_mapping_iceberg.py http://localhost:8181 finance_mapping.md
    uv run example/refresh_mapping_iceberg.py http://catalog/api mapping.md \\
        --credential token=secret --repo-name finance_db
"""
import argparse
import sys

from datafinder_iceberg.iceberg_catalog_reader import read_repository_from_iceberg_catalog
from mapping_markdown.refresh import refresh_mapping


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh a mapping markdown from an Iceberg REST catalog."
    )
    parser.add_argument("catalog_uri", help="Iceberg REST catalog URI (e.g. http://localhost:8181)")
    parser.add_argument("mapping_path", help="Path to the existing mapping markdown file")
    parser.add_argument("--out", dest="output_path", default=None,
                        help="Output path (default: overwrite mapping_path)")
    parser.add_argument("--repo-name", dest="repo_name", default=None,
                        help="Override the repository name (default: catalog URI)")
    parser.add_argument("--credential", metavar="KEY=VALUE", action="append", default=[],
                        help="Credential property passed to the catalog (repeatable)")
    parser.add_argument("--skip-errors", dest="skip_errors", action="store_true",
                        help="Warn and skip tables that fail to load instead of aborting")
    args = parser.parse_args()

    credentials = {}
    for kv in args.credential:
        if "=" not in kv:
            print(f"Error: --credential must be KEY=VALUE, got: {kv!r}", file=sys.stderr)
            sys.exit(1)
        k, v = kv.split("=", 1)
        credentials[k] = v

    output_path = args.output_path or args.mapping_path

    print(f"Connecting to Iceberg catalog at {args.catalog_uri} …")
    repo = read_repository_from_iceberg_catalog(
        catalog_uri=args.catalog_uri,
        repo_name=args.repo_name,
        credentials=credentials or None,
        fail_on_error=not args.skip_errors,
    )
    schemas = [s.name for s in repo.schemas]
    tables = [t.name for s in repo.schemas for t in s.tables]
    print(f"  Found {len(schemas)} namespace(s), {len(tables)} table(s): {', '.join(tables)}")

    print(f"Refreshing {args.mapping_path} …")
    refresh_mapping(args.mapping_path, repo, output_path=output_path)
    print(f"  Written to {output_path}")


if __name__ == "__main__":
    main()
