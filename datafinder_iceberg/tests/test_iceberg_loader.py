import pytest

from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, LongType,
    DoubleType, FloatType, BooleanType, DateType,
    TimestampType, DecimalType, StructType, ListType, MapType,
)

from datafinder_iceberg.iceberg_loader import schema_to_table


def make_schema(*fields):
    return Schema(*fields)


class TestIcebergLoader:

    def test_table_name(self):
        schema = make_schema(NestedField(1, "id", IntegerType(), required=True))
        table = schema_to_table(schema, "trades")
        assert table.name == "trades"

    def test_string_column(self):
        schema = make_schema(NestedField(1, "symbol", StringType()))
        table = schema_to_table(schema, "t")
        assert table.columns[0].name == "symbol"
        assert table.columns[0].type == "VARCHAR"

    def test_integer_column(self):
        schema = make_schema(NestedField(1, "id", IntegerType()))
        assert schema_to_table(schema, "t").columns[0].type == "INT"

    def test_long_column(self):
        schema = make_schema(NestedField(1, "qty", LongType()))
        assert schema_to_table(schema, "t").columns[0].type == "BIGINT"

    def test_double_column(self):
        schema = make_schema(NestedField(1, "price", DoubleType()))
        assert schema_to_table(schema, "t").columns[0].type == "DOUBLE"

    def test_float_column(self):
        schema = make_schema(NestedField(1, "rate", FloatType()))
        assert schema_to_table(schema, "t").columns[0].type == "FLOAT"

    def test_boolean_column(self):
        schema = make_schema(NestedField(1, "active", BooleanType()))
        assert schema_to_table(schema, "t").columns[0].type == "BOOLEAN"

    def test_date_column(self):
        schema = make_schema(NestedField(1, "trade_date", DateType()))
        assert schema_to_table(schema, "t").columns[0].type == "DATE"

    def test_timestamp_column(self):
        schema = make_schema(NestedField(1, "created_at", TimestampType()))
        assert schema_to_table(schema, "t").columns[0].type == "TIMESTAMP"

    def test_decimal_column(self):
        schema = make_schema(NestedField(1, "amount", DecimalType(18, 4)))
        assert schema_to_table(schema, "t").columns[0].type == "DECIMAL(18,4)"

    def test_struct_column(self):
        inner = StructType(NestedField(1, "street", StringType()))
        schema = make_schema(NestedField(1, "address", inner))
        assert schema_to_table(schema, "t").columns[0].type == "STRUCT"

    def test_list_column(self):
        schema = make_schema(NestedField(1, "tags", ListType(1, StringType())))
        assert schema_to_table(schema, "t").columns[0].type == "LIST"

    def test_map_column(self):
        schema = make_schema(NestedField(1, "props", MapType(1, StringType(), 2, StringType())))
        assert schema_to_table(schema, "t").columns[0].type == "MAP"

    def test_multiple_columns(self):
        schema = make_schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "symbol", StringType()),
            NestedField(3, "price", DoubleType()),
            NestedField(4, "trade_date", DateType()),
        )
        table = schema_to_table(schema, "trades")
        assert len(table.columns) == 4
        assert table.columns[0].name == "id"
        assert table.columns[1].name == "symbol"
        assert table.columns[2].name == "price"
        assert table.columns[3].name == "trade_date"

    def test_column_owner_set(self):
        schema = make_schema(NestedField(1, "id", IntegerType()))
        table = schema_to_table(schema, "trades")
        assert table.columns[0].table is table
