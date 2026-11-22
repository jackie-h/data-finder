"""The extension of sqlglot's Spark generator that Spark and Databricks (which is Spark) share, so that the Postgres SQL we generate gives the same rows
there: where Spark 3.5 does what Postgres does not, the SQL is written as Spark needs. ``SparkFixes`` is mixed in before the generator of the dialect,
and ``spark_transforms`` gives the TRANSFORMS that it needs."""
from typing import Any, Callable

import sqlglot
from sqlglot import exp
from sqlglot.generator import Generator


def _unwrapped(node: exp.Expr) -> exp.Expr:
    while isinstance(node, exp.Paren):
        node = node.this
    return node


def _is_cast_to(node: exp.Expr, type_: Any) -> bool:
    return isinstance(node, exp.Cast) and node.to.this == type_


def _joined(aggregate: exp.GroupConcat) -> exp.Expr:
    """``STRING_AGG(x, sep ORDER BY k ...)`` as Spark 3.5 has it, which has no LISTAGG (4.0 has): the values are collected, sorted by the keys with
    a comparator, and joined. A null value is left out, as STRING_AGG leaves it, and a null key is ordered as the sort says."""
    values = aggregate.this
    keys: list[exp.Ordered] = []
    if isinstance(values, exp.Order):
        keys, values = list(values.expressions), values.this
    separator = aggregate.args.get("separator") or exp.Literal.string("")
    if not keys:
        return exp.func("ARRAY_JOIN", exp.func("COLLECT_LIST", values.copy()), separator.copy())
    fields = ", ".join(["__value AS v"] + [f"__key{i} AS k{i}" for i in range(len(keys))])
    comparisons = []
    for i, key in enumerate(keys):
        before, after = ("-1", "1") if not key.args.get("desc") else ("1", "-1")
        first, last = ("-1", "1") if key.args.get("nulls_first") else ("1", "-1")
        l, r = f"l.k{i}", f"r.k{i}"
        comparisons.append(f"NULLIF(CASE WHEN {l} IS NULL AND {r} IS NULL THEN 0 WHEN {l} IS NULL THEN {first} WHEN {r} IS NULL THEN {last} "
                           f"WHEN {l} < {r} THEN {before} WHEN {l} > {r} THEN {after} ELSE 0 END, 0)")
    template = sqlglot.parse_one(
        f"ARRAY_JOIN(TRANSFORM(ARRAY_SORT(COLLECT_LIST(CASE WHEN __value IS NOT NULL THEN STRUCT({fields}) END), "
        f"(l, r) -> COALESCE({', '.join(comparisons)}, 0)), s -> s.v), __separator)", dialect="spark")
    substitutes: dict[str, exp.Expr] = {"__value": values, "__separator": separator, **{f"__key{i}": key.this for i, key in enumerate(keys)}}
    for column in list(template.find_all(exp.Column)):
        if column.name in substitutes:
            column.replace(substitutes[column.name].copy())
    return template


def _ordered_by_what_it_selects(select: exp.Select) -> exp.Select:
    """A SELECT DISTINCT ordered by the columns it selects as the names it selects them under: Spark does not find a column of the table in the
    ORDER BY once it is aliased."""
    order = select.args.get("order")
    if not (select.args.get("distinct") and order):
        return select
    shown = {each.this.sql(): each.args["alias"] for each in select.expressions if isinstance(each, exp.Alias)}
    if not any(sort.this.sql() in shown for sort in order.expressions):
        return select
    select = select.copy()
    for sort in select.args["order"].expressions:
        alias = shown.get(sort.this.sql())
        if alias is not None:
            sort.set("this", exp.column(alias.copy()))
    return select


class SparkFixes(Generator):
    """Mixed in before the generator of Spark or Databricks."""

    def extract_sql(self, expression: exp.Extract) -> str:
        part = expression.this.name.upper()
        difference = _unwrapped(expression.expression)
        if part == "ISODOW":  # Spark spells the ISO day of the week DOW_ISO
            changed = expression.copy()
            changed.set("this", exp.var("DOW_ISO"))
            return super().extract_sql(changed)
        if part == "EPOCH" and isinstance(difference, exp.Sub):  # the seconds between two timestamps, which Spark gives as an interval
            return self.sql(exp.Paren(this=exp.Sub(this=exp.func("UNIX_SECONDS", difference.this.copy()),
                                                   expression=exp.func("UNIX_SECONDS", difference.expression.copy()))))
        return super().extract_sql(expression)

    @staticmethod
    def _dot(a: str, b: str) -> str:
        """The inner product of two arrays, as a sum of the products (Spark 3.5 has no vector functions)."""
        return f"AGGREGATE(ZIP_WITH({a}, {b}, (x, y) -> CAST(x AS DOUBLE) * y), CAST(0 AS DOUBLE), (acc, v) -> acc + v)"

    def dotproduct_sql(self, expression: exp.DotProduct) -> str:
        return self._dot(self.sql(expression.this), self.sql(expression.expression))

    def euclideandistance_sql(self, expression: exp.EuclideanDistance) -> str:
        a, b = self.sql(expression.this), self.sql(expression.expression)
        return f"SQRT(AGGREGATE(ZIP_WITH({a}, {b}, (x, y) -> POWER(CAST(x AS DOUBLE) - y, 2)), CAST(0 AS DOUBLE), (acc, v) -> acc + v))"

    def cosinedistance_sql(self, expression: exp.CosineDistance) -> str:
        a, b = self.sql(expression.this), self.sql(expression.expression)
        return f"(1 - {self._dot(a, b)} / (SQRT({self._dot(a, a)}) * SQRT({self._dot(b, b)})))"

    def datatype_sql(self, expression: exp.DataType) -> str:
        if expression.is_type(exp.DType.VECTOR):  # a ``VECTOR(n)`` is an array of floats
            return "ARRAY<FLOAT>"
        return super().datatype_sql(expression)

    def _with_a_timestamp(self, expression: Any) -> Any:
        """date +/- interval is a timestamp in Postgres and a date in Spark: the date is made a timestamp first."""
        if isinstance(expression.expression, exp.Interval) and not _is_cast_to(expression.this, exp.DataType.Type.TIMESTAMP):
            expression = expression.copy()
            expression.this.replace(exp.cast(expression.this.copy(), exp.DataType.Type.TIMESTAMP))
        return expression

    def add_sql(self, expression: exp.Add) -> str:
        return super().add_sql(self._with_a_timestamp(expression))

    def sub_sql(self, expression: exp.Sub) -> str:
        if _is_cast_to(expression.this, exp.DataType.Type.DATE) and _is_cast_to(expression.expression, exp.DataType.Type.DATE):
            # date - date is an interval in Spark and the number of days in Postgres
            return self.sql(exp.func("DATEDIFF", expression.this.copy(), expression.expression.copy()))
        return super().sub_sql(self._with_a_timestamp(expression))


def spark_transforms(base: dict) -> dict:
    """The TRANSFORMS of a Spark generator (``base``) with a percentile left as ``PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)``, which Spark has (from
    3.3) and which is exact (sqlglot writes it as ``PERCENTILE_APPROX(x, p)``, an approximation that has no order), the ordered join of the values, and
    the order of a SELECT DISTINCT."""
    group_concat: Callable = base[exp.GroupConcat]
    select: Callable = base[exp.Select]

    def joined(self: Generator, expression: exp.GroupConcat) -> str:
        if isinstance(expression.parent, exp.Window):
            return group_concat(self, expression)
        return self.sql(_joined(expression))

    return {**base,
            exp.WithinGroup: lambda self, e: Generator.withingroup_sql(self, e),
            exp.GroupConcat: joined,
            exp.Select: lambda self, e: select(self, _ordered_by_what_it_selects(e))}
