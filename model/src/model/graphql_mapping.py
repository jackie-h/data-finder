from model.m3 import Property, Class
from model.mapping import ClassMapping, PropertyMapping, MilestonePropertyMapping


class GraphQLFilterConvention:
    """Describes which GraphQL argument names the endpoint uses for server-side push-down."""
    def __init__(self, filter_arg: str, sort_arg: str, limit_arg: str):
        self.filter_arg = filter_arg  # e.g. "where", "filter"
        self.sort_arg = sort_arg      # e.g. "order_by", "orderBy"
        self.limit_arg = limit_arg    # e.g. "limit", "take", "first"


class GraphQLEndpoint:
    def __init__(self, url: str, filter_convention: 'GraphQLFilterConvention | None' = None):
        self.url = url
        self.filter_convention = filter_convention


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
                 milestone: GraphQLMilestone | None = None):
        self.name = name
        self.endpoint = endpoint
        self.milestone = milestone


class GraphQLField:
    def __init__(self, name: str):
        self.name = name


class GraphQLPropertyMapping(PropertyMapping):
    def __init__(self, property: Property, field: GraphQLField):
        super().__init__(property, field)


class GraphQLAssociationMapping(GraphQLPropertyMapping):
    """A navigation property resolved via a GraphQL nested field rather than a primitive field."""
    def __init__(self, property: Property, field: GraphQLField, association_name: str):
        super().__init__(property, field)
        self.association_name = association_name


class GraphQLClassMapping(ClassMapping):
    def __init__(self, clazz: Class, property_mappings: list[GraphQLPropertyMapping],
                 query: GraphQLQuery, milestone_mapping: MilestonePropertyMapping | None = None):
        super().__init__(clazz, property_mappings, milestone_mapping)  # type: ignore[arg-type]
        self.query = query
