from typing import Any

from model.relational import Column, Operation, ComparisonOperation, ComparisonOperator, RelationalOperationElement, \
    ColumnWithJoin, SortOperation, SortDirection, AggregateOperation, AggregateOperator, WindowFunction, \
    WindowFunctionOperation, WindowSpecification


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

    def row_number(self, partition_by=None, order_by=None):
        return WindowFunctionOperation(None, WindowFunction.ROW_NUMBER, 'Row Number',
                                       window=self._window_spec(partition_by, order_by))

    def rank(self, partition_by=None, order_by=None):
        return WindowFunctionOperation(None, WindowFunction.RANK, 'Rank',
                                       window=self._window_spec(partition_by, order_by))

    def dense_rank(self, partition_by=None, order_by=None):
        return WindowFunctionOperation(None, WindowFunction.DENSE_RANK, 'Dense Rank',
                                       window=self._window_spec(partition_by, order_by))

    def ntile(self, buckets: int, partition_by=None, order_by=None):
        return WindowFunctionOperation(None, WindowFunction.NTILE, 'Ntile',
                                       second_arg=buckets, window=self._window_spec(partition_by, order_by))

    def cume_dist(self, partition_by=None, order_by=None):
        return WindowFunctionOperation(None, WindowFunction.CUME_DIST, 'Cume Dist',
                                       window=self._window_spec(partition_by, order_by))

    def percent_rank(self, partition_by=None, order_by=None):
        return WindowFunctionOperation(None, WindowFunction.PERCENT_RANK, 'Percent Rank',
                                       window=self._window_spec(partition_by, order_by))

    def lag(self, offset: int = 1, default=None, partition_by=None, order_by=None):
        extra_args = [] if default is None else [default]
        return WindowFunctionOperation(
            ColumnWithJoin(self.__column, self.__parent),
            WindowFunction.LAG,
            'Lag ' + self.__display_name,
            second_arg=offset,
            extra_args=extra_args,
            window=self._window_spec(partition_by, order_by),
        )

    def lead(self, offset: int = 1, default=None, partition_by=None, order_by=None):
        extra_args = [] if default is None else [default]
        return WindowFunctionOperation(
            ColumnWithJoin(self.__column, self.__parent),
            WindowFunction.LEAD,
            'Lead ' + self.__display_name,
            second_arg=offset,
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
