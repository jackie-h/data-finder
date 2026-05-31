class ExistsOperation:
    """Returned by RelatedFinder.exists(). Backend translates this to a join-presence check."""

    def __init__(self, node):
        self.node = node

    def and_op(self, other):
        from model.relational import LogicalOperation, LogicalOperator
        return LogicalOperation(self, LogicalOperator.AND, other)


class NotExistsOperation:
    """Returned by RelatedFinder.not_exists(). Backend translates this to a join-absence check."""

    def __init__(self, node):
        self.node = node

    def and_op(self, other):
        from model.relational import LogicalOperation, LogicalOperator
        return LogicalOperation(self, LogicalOperator.AND, other)


class RelatedFinder:
    """Base for generated related-finder classes that represent a traversable join.

    exists() / not_exists() return backend-agnostic operations. The relational SQL
    generator translates these to IS NOT NULL / IS NULL on the join's target column.
    """

    def __init__(self, node):
        self._node = node

    def exists(self) -> ExistsOperation:
        """Return a filter matching rows where the association has a match."""
        return ExistsOperation(self._node)

    def not_exists(self) -> NotExistsOperation:
        """Return a filter matching rows where the association has no match."""
        return NotExistsOperation(self._node)

    @staticmethod
    def _build_kwargs_filter(node, kwargs: dict, column_map: dict):
        """Build a filter operation from simple equality kwargs.

        ``column_map`` maps property IDs to ``(col_name, col_type, table)`` tuples.
        Multiple kwargs are combined with AND.  Raises ``ValueError`` for unknown keys.
        """
        import datetime
        import decimal
        from model.relational import (
            Column, ColumnWithJoin, ComparisonOperation, ComparisonOperator,
            LogicalOperation, LogicalOperator,
            StringConstantOperation, IntegerConstantOperation, FloatConstantOperation,
            BooleanConstantOperation, DateConstantOperation, DateTimeConstantOperation,
            DecimalConstantOperation,
        )
        ops = []
        for key, value in kwargs.items():
            if key not in column_map:
                raise ValueError(f"Unknown property '{key}' — valid properties are: {sorted(column_map)}")
            col_name, col_type, table = column_map[key]
            col = ColumnWithJoin(Column(col_name, col_type, table), node)
            if isinstance(value, bool):
                const = BooleanConstantOperation(value)
            elif isinstance(value, int):
                const = IntegerConstantOperation(value)
            elif isinstance(value, float):
                const = FloatConstantOperation(value)
            elif isinstance(value, decimal.Decimal):
                const = DecimalConstantOperation(value)
            elif isinstance(value, datetime.datetime):
                const = DateTimeConstantOperation(value)
            elif isinstance(value, datetime.date):
                const = DateConstantOperation(value)
            else:
                const = StringConstantOperation(str(value))
            ops.append(ComparisonOperation(col, ComparisonOperator.EQUAL, const))
        result = ops[0]
        for op in ops[1:]:
            result = LogicalOperation(result, LogicalOperator.AND, op)
        return result
