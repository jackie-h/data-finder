import fnmatch
import json
import logging
from typing import Optional

from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IcebergType, StructType,
    StringType, BooleanType,
    IntegerType, LongType,
    FloatType, DoubleType, DecimalType,
    DateType, TimeType,
    TimestampType, TimestamptzType,
    TimestampNanoType, TimestamptzNanoType,
    BinaryType, FixedType, UUIDType,
    ListType, MapType,
)

from model.relational import DataCatalog, Schema as RelationalSchema, Table, Column

_log = logging.getLogger(__name__)


def _map_type(iceberg_type: IcebergType) -> str:
    if isinstance(iceberg_type, StringType):
        return "VARCHAR"
    if isinstance(iceberg_type, BooleanType):
        return "BOOLEAN"
    if isinstance(iceberg_type, IntegerType):
        return "INT"
    if isinstance(iceberg_type, LongType):
        return "BIGINT"
    if isinstance(iceberg_type, FloatType):
        return "FLOAT"
    if isinstance(iceberg_type, DoubleType):
        return "DOUBLE"
    if isinstance(iceberg_type, DecimalType):
        return f"DECIMAL({iceberg_type.precision},{iceberg_type.scale})"
    if isinstance(iceberg_type, DateType):
        return "DATE"
    if isinstance(iceberg_type, TimeType):
        return "TIME"
    if isinstance(iceberg_type, (TimestampType, TimestamptzType,
                                  TimestampNanoType, TimestamptzNanoType)):
        return "TIMESTAMP"
    if isinstance(iceberg_type, (BinaryType, FixedType)):
        return "BINARY"
    if isinstance(iceberg_type, UUIDType):
        return "VARCHAR"
    if isinstance(iceberg_type, StructType):
        return "STRUCT"
    if isinstance(iceberg_type, ListType):
        return "LIST"
    if isinstance(iceberg_type, MapType):
        return "MAP"
    return "VARCHAR"


def schema_to_table(schema: Schema, table_name: str) -> Table:
    """Convert a pyiceberg Schema to a relational Table."""
    identifier_ids = set(schema.identifier_field_ids)
    columns = [
        Column(field.name, _map_type(field.field_type),
               primary_key=field.field_id in identifier_ids)
        for field in schema.fields
    ]
    return Table(table_name, columns)


def load_schema_from_dict(schema_dict: dict, table_name: str) -> Table:
    """Load a Table from an Iceberg schema represented as a dict."""
    schema = Schema.model_validate(schema_dict)
    return schema_to_table(schema, table_name)


def load_schema_from_json(path: str, table_name: str) -> Table:
    """Load a Table from an Iceberg schema JSON file."""
    with open(path) as f:
        schema_dict = json.load(f)
    return load_schema_from_dict(schema_dict, table_name)


def load_schema_from_catalog(
    catalog_uri: str,
    namespace: str,
    table_name: str,
    credentials: Optional[dict] = None,
) -> Table:
    """Load a Table by fetching its schema from an Iceberg REST catalog."""
    properties = {"uri": catalog_uri}
    if credentials:
        properties.update(credentials)
    catalog = RestCatalog("catalog", **properties)
    iceberg_table = catalog.load_table((namespace, table_name))
    return schema_to_table(iceberg_table.schema(), table_name)


def _schema_included(name: str, include: list[str], exclude: list[str]) -> bool:
    if include and not any(fnmatch.fnmatch(name, p) for p in include):
        return False
    if any(fnmatch.fnmatch(name, p) for p in exclude):
        return False
    return True


def read_repository_from_catalog(
    catalog,
    fail_on_error: bool = True,
    include_schemas: list[str] | None = None,
    exclude_schemas: list[str] | None = None,
) -> DataCatalog:
    """Build a DataCatalog from an already-constructed pyiceberg Catalog instance.

    include_schemas: glob patterns — only matching namespaces are loaded (e.g. ["*_trusted"])
    exclude_schemas: glob patterns — matching namespaces are skipped  (e.g. ["*_raw"])
    When both are supplied, include is tested first then exclude.
    """
    include = include_schemas or []
    exclude = exclude_schemas or []
    repo = DataCatalog(catalog.name)

    for namespace in catalog.list_namespaces():
        namespace_name = ".".join(str(part) for part in namespace)
        if not _schema_included(namespace_name, include, exclude):
            _log.debug("Skipping namespace '%s' (filtered)", namespace_name)
            continue
        schema = RelationalSchema(namespace_name, repo)
        for table_id in catalog.list_tables(namespace):
            table_name = table_id[-1]
            try:
                iceberg_table = catalog.load_table(table_id)
                table = schema_to_table(iceberg_table.schema(), table_name)
                table.schema = schema
                schema.tables.append(table)
            except Exception as e:
                if fail_on_error:
                    raise
                _log.warning("Skipping table %s.%s: %s", namespace_name, table_name, e)

    return repo


def read_repository_from_iceberg_catalog(
    catalog_uri: str,
    catalog_name: str,
    credentials: Optional[dict] = None,
    fail_on_error: bool = True,
    include_schemas: list[str] | None = None,
    exclude_schemas: list[str] | None = None,
) -> DataCatalog:
    """Build a DataCatalog by connecting to an Iceberg REST catalog by URI."""
    properties = {"uri": catalog_uri}
    if credentials:
        properties.update(credentials)
    catalog = RestCatalog(catalog_name, **properties)
    return read_repository_from_catalog(
        catalog,
        fail_on_error=fail_on_error,
        include_schemas=include_schemas,
        exclude_schemas=exclude_schemas,
    )
