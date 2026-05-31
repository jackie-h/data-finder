from typing import Any

from model.relational import Column, Operation, ComparisonOperation, ComparisonOperator, RelationalOperationElement, \
    ColumnWithJoin, SortOperation, SortDirection, AggregateOperation, AggregateOperator, IsNullOperation, IsNotNullOperation, \
    WindowFunction, WindowFunctionOperation, WindowSpecification


class Attribute:
    """Base wrapper for a table column exposed through the finder API."""

    __display_name: str
    __column: Column
    __owner: str
    __parent: Any

    def __init__(self, display_name: str, column_name: str, column_db_type: str, owner:str, parent=None):
        """Create a typed attribute for a named column."""
        self.__display_name = display_name
        self.__column = Column(column_name, column_db_type, owner)
        self.__owner = owner
        self.__parent = parent

    def column(self) -> Column:
        """Return the underlying column metadata."""
        return self.__column

    def owner(self) -> str:
        """Return the owning table or relation name."""
        return self.__owner

    def parent(self) -> Any:
        """Return the parent join, if this attribute is traversal-based."""
        return self.__parent

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

    def ntile(self, buckets: int, partition_by=None, order_by=None):
        return WindowFunctionOperation(None, WindowFunction.NTILE, 'Ntile',
                                       second_arg=buckets, window=self._window_spec(partition_by, order_by))

    def shift(self, periods: int = 1, fill_value=None, partition_by=None, order_by=None):
        extra_args = [] if fill_value is None else [fill_value]
        func = WindowFunction.LAG if periods >= 0 else WindowFunction.LEAD
        label = 'Lag' if periods >= 0 else 'Lead'
        return WindowFunctionOperation(
            ColumnWithJoin(self.__column, self.__parent),
            func,
            label + ' ' + self.__display_name,
            second_arg=abs(periods),
            extra_args=extra_args,
            window=self._window_spec(partition_by, order_by),
        )

    def first_value(self, partition_by=None, order_by=None):
        return WindowFunctionOperation(
            ColumnWithJoin(self.__column, self.__parent),
            WindowFunction.FIRST_VALUE,
            'First Value ' + self.__display_name,
            window=self._window_spec(partition_by, order_by),
        )

    def last_value(self, partition_by=None, order_by=None):
        return WindowFunctionOperation(
            ColumnWithJoin(self.__column, self.__parent),
            WindowFunction.LAST_VALUE,
            'Last Value ' + self.__display_name,
            window=self._window_spec(partition_by, order_by),
        )

    def count(self) -> AggregateOperation:
        """Return a count aggregation for this Attribute."""
        return AggregateOperation(ColumnWithJoin(self.__column, self.__parent), AggregateOperator.COUNT, self.__display_name + ' Count')

    def ascending(self) -> SortOperation:
        """Return an ascending sort operation for this Attribute."""
        return SortOperation(ColumnWithJoin(self.__column, self.__parent), SortDirection.ASC)

    def descending(self) -> SortOperation:
        """Return a descending sort operation for this Attribute."""
        return SortOperation(ColumnWithJoin(self.__column, self.__parent), SortDirection.DESC)

    def is_none(self) -> Operation:
        """Return a filter that matches rows where this column is NULL. Equivalent to ``is None`` in Python."""
        return IsNullOperation(ColumnWithJoin(self.__column, self.__parent))

    def is_not_none(self) -> Operation:
        """Return a filter that matches rows where this column is not NULL. Equivalent to ``is not None`` in Python."""
        return IsNotNullOperation(ColumnWithJoin(self.__column, self.__parent))
