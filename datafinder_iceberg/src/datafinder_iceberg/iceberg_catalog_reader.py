import logging
from typing import Optional

from pyiceberg.catalog.rest import RestCatalog

_log = logging.getLogger(__name__)
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

from model.relational import Repository, Schema, Table, Column


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


def read_repository_from_catalog(
    catalog,
    repo_name: str = None,
    fail_on_error: bool = True,
) -> Repository:
    """Build a Repository from an already-constructed pyiceberg Catalog instance."""
    name = repo_name or catalog.name
    repo = Repository(name, "")

    for namespace in catalog.list_namespaces():
        namespace_name = ".".join(str(part) for part in namespace)
        schema = Schema(namespace_name, repo, prefix=catalog.name)
        for table_id in catalog.list_tables(namespace):
            table_name = table_id[-1]
            try:
                iceberg_table = catalog.load_table(table_id)
                columns = [
                    Column(field.name, _map_type(field.field_type))
                    for field in iceberg_table.schema().fields
                ]
                Table(table_name, columns, schema)
            except Exception as e:
                if fail_on_error:
                    raise
                _log.warning("Skipping table %s.%s: %s", namespace_name, table_name, e)

    return repo


def read_repository_from_iceberg_catalog(
    catalog_uri: str,
    repo_name: str = None,
    credentials: Optional[dict] = None,
    fail_on_error: bool = True,
) -> Repository:
    """Build a Repository by connecting to an Iceberg REST catalog by URI."""
    properties = {"uri": catalog_uri}
    if credentials:
        properties.update(credentials)
    catalog = RestCatalog("catalog", **properties)
    return read_repository_from_catalog(catalog, repo_name=repo_name or catalog_uri, fail_on_error=fail_on_error)
