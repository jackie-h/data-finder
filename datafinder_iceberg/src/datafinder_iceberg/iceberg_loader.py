from typing import Union

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

from model.relational import Column, Table


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
    columns = [
        Column(field.name, _map_type(field.field_type))
        for field in schema.fields
    ]
    return Table(table_name, columns)


def load_schema_from_dict(schema_dict: dict, table_name: str) -> Table:
    """Load a Table from an Iceberg schema represented as a dict."""
    schema = Schema.from_dict(schema_dict)
    return schema_to_table(schema, table_name)


def load_schema_from_json(path: str, table_name: str) -> Table:
    """Load a Table from an Iceberg schema JSON file."""
    import json
    with open(path) as f:
        schema_dict = json.load(f)
    return load_schema_from_dict(schema_dict, table_name)
