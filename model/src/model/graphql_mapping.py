import json
import re
from dataclasses import dataclass, field

from model.m3 import Property, Class
from model.mapping import ClassMapping, PropertyMapping, MilestonePropertyMapping


class GraphQLFilterConvention:
    """Describes which GraphQL argument names the endpoint uses for server-side push-down."""
    def __init__(self, filter_arg: str, sort_arg: str, limit_arg: str):
        self.filter_arg = filter_arg  # e.g. "where", "filter"
        self.sort_arg = sort_arg      # e.g. "order_by", "orderBy"
        self.limit_arg = limit_arg    # e.g. "limit", "take", "first"


class GraphQLEndpoint:
    def __init__(self, url: str, filter_convention: GraphQLFilterConvention | None = None):
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
        super().__init__(clazz, property_mappings, milestone_mapping)
        self.query = query


# ---------------------------------------------------------------------------
# Field paths
#
# A column's name is the path of GraphQL fields from the root query to the field, for example
# `manager.projects[]{"code": "ALPHA"}.name`. A segment ending in `[]` is a to-many relation, and a
# trailing JSON object holds the equality filters the relation was navigated with.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PathSegment:
    name: str
    many: bool = False
    filters: dict = field(default_factory=dict, hash=False, compare=True)


_SEGMENT_NAME = re.compile(r'[^.\[{]+')


def format_path_segment(name: str, many: bool = False, filters: dict | None = None) -> str:
    return name + ("[]" if many else "") + (json.dumps(filters) if filters else "")


def parse_field_path(path: str) -> list[PathSegment]:
    segments: list[PathSegment] = []
    i = 0
    while i < len(path):
        m = _SEGMENT_NAME.match(path, i)
        if m is None:
            raise ValueError(f"Invalid field path {path!r} at {i}")
        name, i = m.group(), m.end()
        many = path.startswith("[]", i)
        if many:
            i += 2
        filters: dict = {}
        if i < len(path) and path[i] == "{":
            filters, i = json.JSONDecoder().raw_decode(path, i)
        segments.append(PathSegment(name, many, filters))
        if i < len(path):
            if path[i] != ".":
                raise ValueError(f"Invalid field path {path!r} at {i}")
            i += 1
    return segments


def plain_field_path(path: str) -> str:
    """The path of field names alone: the key a field has in a flattened row."""
    return ".".join(seg.name for seg in parse_field_path(path))


def association_segment(name: str, many: bool, kwargs: dict, fields: dict[str, str]) -> str:
    """The path segment for navigating an association with equality filters on the target's properties.

    kwargs are keyed by the target's Python property names; fields maps those names to GraphQL field names.
    """
    unknown = sorted(set(kwargs) - set(fields))
    if unknown:
        raise TypeError(f"Unknown filter {', '.join(unknown)} for {name}: expected one of {', '.join(sorted(fields))}")
    return format_path_segment(name, many, {fields[key]: value for key, value in kwargs.items()})
