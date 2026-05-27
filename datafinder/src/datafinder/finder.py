from model.relational import JoinTreeNodeOperation, IsNullOperation, IsNotNullOperation, ColumnWithJoin, Operation


class RelatedFinder:
    """Base for generated related-finder classes that represent a traversable join.

    Provides exists() / not_exists() to filter the primary object based on
    whether the joined association has a matching row.  Because joins are always
    LEFT OUTER, a missing match leaves the join-key column NULL, so the check
    is simply IS NOT NULL / IS NULL on the join's right-hand (target) column.
    """

    def __init__(self, node: JoinTreeNodeOperation):
        self._node = node

    def exists(self) -> Operation:
        """Return a filter matching rows where the association has a match.

        Equivalent to checking that the joined row is not NULL.
        Example: ``tf.account().exists()`` → trades that have a matching account.
        """
        col = self._node.join.right
        return IsNotNullOperation(ColumnWithJoin(col, self._node))

    def not_exists(self) -> Operation:
        """Return a filter matching rows where the association has no match.

        Equivalent to checking that the joined row is NULL.
        Example: ``tf.account().not_exists()`` → trades with no matching account.
        """
        col = self._node.join.right
        return IsNullOperation(ColumnWithJoin(col, self._node))
