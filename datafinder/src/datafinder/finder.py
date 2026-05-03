from datafinder import JoinOperation, Attribute


class RelatedFinder:
    """Represents a traversable relationship from one finder to another."""

    def __init__(self, source: Attribute, target: Attribute):
        """Create a join between the source and target attributes."""
        self.__join = JoinOperation(source, target)
