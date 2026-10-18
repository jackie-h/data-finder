import pathlib
import re
from typing import Iterable

from datafinder import ViewDefinition

_TOKEN = re.compile(r'^[A-Za-z0-9_.@-]+$')

CHANGELOG_HEADER = "--liquibase formatted sql"


def _check_token(kind: str, value: str) -> None:
    if not isinstance(value, str) or not _TOKEN.match(value):
        raise ValueError(f"{kind} {value!r} may only contain letters, digits and _ . @ -")


def to_liquibase_changeset(view: ViewDefinition, author: str, dialect: str | None = None,
                           changeset_id: str | None = None) -> str:
    """A Liquibase formatted-SQL changeset that (re)creates ``view``.

    The id defaults to the view's qualified name. ``runOnChange`` lets a changed projection
    redeploy under the same id, and the statement isn't split on ';' so constants in a view
    filter can't truncate it. A changeset needs the ``--liquibase formatted sql`` header
    in its file; use :func:`to_liquibase_changelog` for a complete file.
    """
    changeset_id = changeset_id or view.qualified_name
    _check_token("Changeset author", author)
    _check_token("Changeset id", changeset_id)
    return (f"--changeset {author}:{changeset_id} runOnChange:true splitStatements:false\n"
            f"{view.to_sql(dialect)}\n"
            f"--rollback DROP VIEW IF EXISTS {view.qualified_name}\n")


def to_liquibase_changelog(views: Iterable[ViewDefinition], author: str, dialect: str | None = None) -> str:
    """A complete Liquibase formatted-SQL changelog with one changeset per view."""
    views = list(views)
    names = [v.qualified_name.lower() for v in views]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ValueError(f"Views defined more than once: {duplicates}")
    return CHANGELOG_HEADER + "\n\n" + "\n".join(to_liquibase_changeset(v, author, dialect) for v in views)


def write_changelog(views: Iterable[ViewDefinition], path: str | pathlib.Path, author: str,
                    dialect: str | None = None) -> pathlib.Path:
    """Write the changelog to ``path`` (use a ``.sql`` extension so Liquibase reads it as formatted SQL)."""
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(to_liquibase_changelog(views, author, dialect), encoding="utf-8")
    return path


def to_liquibase_master_changelog(changelog_files: Iterable[str]) -> str:
    """A YAML master changelog including each file, resolved relative to the master."""
    lines = ["databaseChangeLog:"]
    for file in changelog_files:
        lines += ["  - include:", f"      file: {file}", "      relativeToChangelogFile: true"]
    return "\n".join(lines) + "\n"
