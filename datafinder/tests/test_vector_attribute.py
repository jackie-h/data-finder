"""What a vector query does that no finder spec shows: the errors it raises, the column type it is made from, and the SQL written for Postgres, the
engine with no live runner here (pgvector). The distances themselves, and the nearest rows, are specced in companies_specs (``cosine_distance_of_each_company``
and the ``nearest_*`` specs), which DuckDB and Spark run."""
import math

import pytest

from datafinder import sql_for_dialect, to_sql
from datafinder.typed_attributes import VectorAttribute, typed_attribute
from model.relational import DoubleFunctionOperation, NoOperation, Table

_OWNER = Table("companies", [])


def _embedding(db_type: str = "VECTOR(3)") -> VectorAttribute:
    return VectorAttribute("Embedding", "embedding", db_type, _OWNER)


def test_a_vector_column_is_a_vector_attribute_with_its_dimension():
    attribute = typed_attribute("embedding", "VECTOR(1536)", _OWNER)
    assert isinstance(attribute, VectorAttribute) and attribute.dimension() == 1536


def test_the_dimension_is_unknown_when_the_type_does_not_say():
    assert _embedding("VECTOR").dimension() is None


@pytest.mark.parametrize("distance", ["cosine_distance", "l2_distance", "inner_product"])
def test_a_distance_is_a_floating_point_number_of_the_query(distance):
    """So it can be rounded, compared, ordered by and aggregated as any number is."""
    assert isinstance(getattr(_embedding(), distance)([1, 0, 0]), DoubleFunctionOperation)


@pytest.mark.parametrize("vector", [[1, 2], [1, 2, 3, 4]])
def test_a_vector_of_another_length_is_an_error(vector):
    with pytest.raises(ValueError, match="3 numbers"):
        _embedding().cosine_distance(vector)


@pytest.mark.parametrize("vector", [[], [math.nan, 0, 0], [math.inf, 0, 0]])
def test_an_empty_or_non_finite_vector_is_an_error(vector):
    with pytest.raises(ValueError):
        _embedding().l2_distance(vector)


def test_a_vector_of_a_column_of_unknown_length_is_not_checked():
    assert _embedding("VECTOR").cosine_distance([1, 2]) is not None


def test_nearest_is_of_a_distance():
    from datafinder.runner import FinderResult
    with pytest.raises(ValueError, match="cosine_distance"):
        FinderResult.nearest(object(), _embedding(), 3)  # pyright: ignore[reportArgumentType]


def test_nearest_needs_at_least_one_row():
    from datafinder.runner import FinderResult
    with pytest.raises(ValueError, match="k >= 1"):
        FinderResult.nearest(object(), _embedding().cosine_distance([1, 0, 0]), 0)  # pyright: ignore[reportArgumentType]


def test_the_generated_sql_is_pgvectors_and_postgres_writes_it_as_it_is():
    """The SQL we generate is Postgres with pgvector: ``COSINE_DISTANCE``, ``L2_DISTANCE`` and ``INNER_PRODUCT`` of the column and a ``VECTOR(n)``."""
    query = to_sql(None, None, [_embedding().cosine_distance([1, 0.5, 0]), _embedding().l2_distance([1, 0.5, 0]), _embedding().inner_product([1, 0.5, 0])],
                   _OWNER, NoOperation(), None, None, None)
    vector = "CAST(ARRAY[1.0, 0.5, 0.0] AS VECTOR(3))"
    assert f"COSINE_DISTANCE(t0.embedding, {vector})" in query and f"L2_DISTANCE(t0.embedding, {vector})" in query \
        and f"INNER_PRODUCT(t0.embedding, {vector})" in query
    postgres = sql_for_dialect(query, "postgres")
    assert f"L2_DISTANCE(t0.embedding, {vector})" in postgres and f"INNER_PRODUCT(t0.embedding, {vector})" in postgres
    assert "EUCLIDEAN_DISTANCE" not in postgres and "DOT_PRODUCT" not in postgres  # not sqlglot's own names for them
