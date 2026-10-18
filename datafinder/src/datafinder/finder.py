from model.relational import ExistsOperation, NotExistsOperation, JoinTreeNodeOperation


def display_prefix(relation_name: str, parent_join: JoinTreeNodeOperation | None) -> str:
    """The display-name prefix for a column reached through a join: the relation names from the root, so
    ``account().branch().city()`` is "Account Branch City" and not just "Branch City"."""
    names = [relation_name]
    node = parent_join
    while node is not None:
        names.append(node.join.name)
        node = node.parent
    return ' '.join(name.title() for name in reversed(names))


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

