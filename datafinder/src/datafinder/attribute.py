from typing import Any

from model.relational import Column, Operation, ComparisonOperation, ComparisonOperator, RelationalOperationElement, \
    ColumnWithJoin, SortOperation, SortDirection, AggregateOperation, AggregateOperator, IsNullOperation, IsNotNullOperation, \
    WindowFunction, WindowFunctionOperation, WindowSpecification


class Attribute(RelationalOperationElement):
    """Base wrapper for a table column exposed through the finder API."""

    __display_name: str
    __column: Column
    __owner: str
    __parent: Any
    __embedded: Any

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None,
                 embedded: 'Attribute | None' = None):
        """Create a typed attribute for a named column.

        ``embedded``, when set, is a fully-built alternate Attribute pointing at a flat column
        that satisfies this same property without the join ``parent`` depends on. The SQL
        generator decides at query-build time whether to use it (see sql_generator elision).
        """
        super().__init__()
        self.__display_name = display_name
        self.__column = Column(column_name, column_db_type, owner)
        self.__owner = owner
        self.__parent = parent
        self.__embedded = embedded

    def column(self) -> Column:
        """Return the underlying column metadata."""
        return self.__column

    def owner(self) -> str:
        """Return the owning table or relation name."""
        return self.__owner

    def parent(self) -> Any:
        """Return the parent join, if this attribute is traversal-based."""
        return self.__parent

    def embedded(self) -> 'Attribute | None':
        """Return the join-less alternate for this attribute, if one is mapped."""
        return self.__embedded

    def _column_with_join(self) -> ColumnWithJoin:
        """Return this attribute as a ColumnWithJoin, propagating its embedded alternate."""
        embedded_column_with_join = self.__embedded._column_with_join() if self.__embedded is not None else None
        return ColumnWithJoin(self.__column, self.__parent, embedded=embedded_column_with_join)

    def display_name(self) -> str:
        """Return the human-readable display name."""
        return self.__display_name

    def _normalize_window_parts(self, parts):
        if parts is None:
            return []
        if isinstance(parts, (list, tuple)):
            return list(parts)
        return [parts]

    def _window_spec(self, partition_by=None, order_by=None):
        return WindowSpecification(self._normalize_window_parts(partition_by), self._normalize_window_parts(order_by))

    def rank(self, method: str = 'min', ascending: bool = True, pct: bool = False,
             partition_by=None, order_by=None):
        """Assign a rank to each row within the window, following pandas ``Series.rank`` conventions.

        ``method`` controls tie-breaking:

        - ``'min'``   — tied rows share the lowest rank; the next rank skips.
        - ``'dense'`` — like ``'min'`` but ranks are contiguous (no gaps).
        - ``'first'`` — rows are numbered by order of appearance; no ties.

        When ``pct=True`` the result is a relative rank between 0 and 1:

        - default (``method!='max'``) — cumulative proportion using ``PERCENT_RANK`` semantics.
        - ``method='max'``            — cumulative proportion using ``CUME_DIST`` semantics.

        ``ascending`` is accepted for API symmetry with pandas but does not affect ranking;
        control sort direction through ``order_by`` instead.
        """
        if pct:
            if method == 'max':
                func, name = WindowFunction.CUME_DIST, 'Cume Dist'
            else:
                func, name = WindowFunction.PERCENT_RANK, 'Percent Rank'
        elif method == 'first':
            func, name = WindowFunction.ROW_NUMBER, 'Row Number'
        elif method == 'dense':
            func, name = WindowFunction.DENSE_RANK, 'Dense Rank'
        else:
            func, name = WindowFunction.RANK, 'Rank'
        return WindowFunctionOperation(None, func, name, window=self._window_spec(partition_by, order_by))

    def qcut(self, buckets: int, partition_by=None, order_by=None):
        """Divide rows into ``buckets`` equal-sized groups and return each row's bucket number (1-based).

        Named after ``pandas.qcut``, which splits a series into quantile-based bins.
        """
        return WindowFunctionOperation(None, WindowFunction.NTILE, 'Quantile',
                                       second_arg=buckets, window=self._window_spec(partition_by, order_by))

    def shift(self, periods: int = 1, fill_value=None, partition_by=None, order_by=None):
        """Return the column value offset by ``periods`` rows within the window, following pandas ``Series.shift``.

        - Positive ``periods`` look *back*: returns the value from ``periods`` rows before the current row.
        - Negative ``periods`` look *ahead*: returns the value from ``abs(periods)`` rows after the current row.
        - ``fill_value`` is returned when the offset row does not exist (at the start or end of the partition).
        """
        extra_args = [] if fill_value is None else [fill_value]
        func = WindowFunction.LAG if periods >= 0 else WindowFunction.LEAD
        label = 'Lag' if periods >= 0 else 'Lead'
        return WindowFunctionOperation(
            self._column_with_join(),
            func,
            label + ' ' + self.__display_name,
            second_arg=abs(periods),
            extra_args=extra_args,
            window=self._window_spec(partition_by, order_by),
        )

    def first(self, partition_by=None, order_by=None):
        """Return the first value of this column in the window frame."""
        return WindowFunctionOperation(
            self._column_with_join(),
            WindowFunction.FIRST_VALUE,
            'First ' + self.__display_name,
            window=self._window_spec(partition_by, order_by),
        )

    def last(self, partition_by=None, order_by=None):
        """Return the last value of this column in the window frame."""
        return WindowFunctionOperation(
            self._column_with_join(),
            WindowFunction.LAST_VALUE,
            'Last ' + self.__display_name,
            window=self._window_spec(partition_by, order_by),
        )

    def count(self) -> AggregateOperation:
        """Return a count aggregation for this Attribute."""
        return AggregateOperation(self._column_with_join(), AggregateOperator.COUNT, self.__display_name + ' Count')

    def ascending(self) -> SortOperation:
        """Return an ascending sort operation for this Attribute."""
        return SortOperation(self._column_with_join(), SortDirection.ASC)

    def descending(self) -> SortOperation:
        """Return a descending sort operation for this Attribute."""
        return SortOperation(self._column_with_join(), SortDirection.DESC)

    def is_none(self) -> Operation:
        """Return a filter that matches rows where this column is NULL. Equivalent to ``is None`` in Python."""
        return IsNullOperation(self._column_with_join())

    def is_not_none(self) -> Operation:
        """Return a filter that matches rows where this column is not NULL. Equivalent to ``is not None`` in Python."""
        return IsNotNullOperation(self._column_with_join())
