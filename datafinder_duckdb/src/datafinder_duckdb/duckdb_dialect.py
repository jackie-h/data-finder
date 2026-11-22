"""DuckDB as sqlglot writes it, with the vector functions of DuckDB for the distances of the SQL we generate (pgvector's). A ``VECTOR(n)`` is DuckDB's
``FLOAT[n]``, an array of n floats, and its ``ARRAY_*`` functions are used: they give a missing distance for a missing vector, where the ``LIST_*`` functions
(which sqlglot writes) raise an error. They give a float, which is cast to a double as the other engines give."""
from sqlglot import exp
from sqlglot.dialects.duckdb import DuckDB
from sqlglot.generators.duckdb import DuckDBGenerator

from datafinder import register_dialect


class _DuckDB(DuckDB):
    class Generator(DuckDBGenerator):
        TRANSFORMS = {**DuckDBGenerator.TRANSFORMS,
                      exp.CosineDistance: lambda self, e: f"CAST({self.func('ARRAY_COSINE_DISTANCE', e.this, e.expression)} AS DOUBLE)",
                      exp.EuclideanDistance: lambda self, e: f"CAST({self.func('ARRAY_DISTANCE', e.this, e.expression)} AS DOUBLE)",
                      exp.DotProduct: lambda self, e: f"CAST({self.func('ARRAY_INNER_PRODUCT', e.this, e.expression)} AS DOUBLE)"}

        def datatype_sql(self, expression: exp.DataType) -> str:
            if expression.is_type(exp.DType.VECTOR):  # a ``VECTOR(n)`` is an array of n floats
                size = expression.expressions[0].this.sql() if expression.expressions else ""
                return f"FLOAT[{size}]"
            return super().datatype_sql(expression)


register_dialect("duckdb", _DuckDB)
