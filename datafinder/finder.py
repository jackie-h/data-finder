from datafinder import JoinOperation, Attribute


class RelatedFinder:
    def __init__(self, source: Attribute, target: Attribute):
        self.__join = JoinOperation(source, target)

