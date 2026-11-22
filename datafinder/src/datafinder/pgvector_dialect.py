"""Postgres as the SQL we generate is written and read: with the vector functions of pgvector, which sqlglot's Postgres does not know all of. A distance
is ``COSINE_DISTANCE``, ``L2_DISTANCE`` or ``INNER_PRODUCT`` of a vector column and a ``VECTOR(n)``; they are read as sqlglot's own distance expressions, so that
each engine's dialect can write its own function for them, and written again as pgvector's."""
from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.postgres import PostgresGenerator
from sqlglot.parsers.postgres import PostgresParser


class PgVector(Postgres):
    class Parser(PostgresParser):
        FUNCTIONS = {**PostgresParser.FUNCTIONS,
                     "L2_DISTANCE": exp.EuclideanDistance.from_arg_list,
                     "INNER_PRODUCT": exp.DotProduct.from_arg_list}

    class Generator(PostgresGenerator):
        TRANSFORMS = {**PostgresGenerator.TRANSFORMS,
                      exp.EuclideanDistance: lambda self, e: self.func("L2_DISTANCE", e.this, e.expression),
                      exp.DotProduct: lambda self, e: self.func("INNER_PRODUCT", e.this, e.expression)}
