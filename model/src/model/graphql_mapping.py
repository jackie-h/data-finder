from model.m3 import Property, Class
from model.mapping import ClassMapping, PropertyMapping, MilestonePropertyMapping


class GraphQLEndpoint:
    def __init__(self, url: str):
        self.url = url


class GraphQLProcessingMilestone:
    """Point-in-time query via a single processing-datetime argument (e.g. asOf).
    Analogous to ProcessingTemporalColumns in the relational world."""
    def __init__(self, argument_name: str = "asOf"):
        self.argument_name = argument_name


class GraphQLBusinessDateMilestone:
    """Business-date query via a single date argument (e.g. businessDate).
    Analogous to SingleBusinessDateColumn."""
    def __init__(self, argument_name: str = "businessDate"):
        self.argument_name = argument_name


class GraphQLBiTemporalMilestone:
    """Both temporal dimensions passed as separate arguments.
    Analogous to BusinessDateAndProcessingTemporalColumns / BiTemporalColumns."""
    def __init__(self, business_date_argument: str = "businessDate",
                 processing_argument: str = "asOf"):
        self.business_date_argument = business_date_argument
        self.processing_argument = processing_argument


GraphQLMilestone = GraphQLProcessingMilestone | GraphQLBusinessDateMilestone | GraphQLBiTemporalMilestone


class GraphQLQuery:
    def __init__(self, name: str, endpoint: GraphQLEndpoint,
                 milestone: GraphQLMilestone = None):
        self.name = name
        self.endpoint = endpoint
        self.milestone = milestone


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
