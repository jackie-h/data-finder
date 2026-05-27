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
