from model.m3 import Property, Class
from model.mapping import ClassMapping, PropertyMapping, MilestonePropertyMapping


class GraphQLEndpoint:
    def __init__(self, url: str):
        self.url = url


class GraphQLQuery:
    def __init__(self, name: str, endpoint: GraphQLEndpoint):
        self.name = name
        self.endpoint = endpoint


class GraphQLField:
    def __init__(self, name: str):
        self.name = name


class GraphQLPropertyMapping(PropertyMapping):
    def __init__(self, property: Property, field: GraphQLField):
        super().__init__(property, field)


class GraphQLClassMapping(ClassMapping):
    def __init__(self, clazz: Class, property_mappings: list[GraphQLPropertyMapping],
                 query: GraphQLQuery, milestone_mapping: MilestonePropertyMapping = None):
        super().__init__(clazz, property_mappings, milestone_mapping)
        self.query = query
