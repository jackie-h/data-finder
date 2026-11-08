import datetime
import json
import re
from dataclasses import dataclass, field
from typing import Any

from model.m3 import Property, Class
from model.relational import NamedRelation
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


class GraphQLQuery(NamedRelation):
    """A named query of a GraphQL endpoint: the relation its fields are read from."""

    def __init__(self, name: str, endpoint: GraphQLEndpoint,
                 milestone: GraphQLMilestone | None = None):
        super().__init__(name, [])
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
    # the arguments the relation takes besides its filters: the dates it is read at, as {"asOf": "2021-06-01T00:00:00"}
    args: dict = field(default_factory=dict, hash=False, compare=True)


_SEGMENT_NAME = re.compile(r'[^.\[{]+')


# A segment's JSON holds its filters by field name and its arguments under a name that starts with "@"
_ARG_PREFIX = "@"


def format_path_segment(name: str, many: bool = False, filters: dict | None = None, args: dict | None = None) -> str:
    content = {**(filters or {}), **{_ARG_PREFIX + key: value for key, value in (args or {}).items()}}
    return name + ("[]" if many else "") + (json.dumps(content) if content else "")


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
        content: dict = {}
        if i < len(path) and path[i] == "{":
            content, i = json.JSONDecoder().raw_decode(path, i)
        filters = {key: value for key, value in content.items() if not key.startswith(_ARG_PREFIX)}
        args = {key[len(_ARG_PREFIX):]: value for key, value in content.items() if key.startswith(_ARG_PREFIX)}
        segments.append(PathSegment(name, many, filters, args))
        if i < len(path):
            if path[i] != ".":
                raise ValueError(f"Invalid field path {path!r} at {i}")
            i += 1
    return segments


def plain_field_path(path: str) -> str:
    """The path of field names alone: the key a field has in a flattened row."""
    return ".".join(seg.name for seg in parse_field_path(path))


def _argument_value(value: Any) -> str:
    """A date or datetime as the text a query sends: a plain date for a processing time is midnight on that date."""
    if isinstance(value, str):
        return value
    return value.isoformat()


def temporal_arguments(name: str, milestone: Any, business_date: Any = None, processing_time: Any = None) -> dict:
    """The arguments that read the relation ``name`` at a date, for the milestone of the query it reaches.

    Raises TypeError for a date the query cannot take: a query with no milestone takes none, and one with only a
    processing time takes no business date."""
    args: dict = {}
    if business_date is not None:
        if isinstance(milestone, GraphQLBiTemporalMilestone):
            args[milestone.business_date_argument] = _argument_value(business_date)
        elif isinstance(milestone, GraphQLBusinessDateMilestone):
            args[milestone.argument_name] = _argument_value(business_date)
        else:
            raise TypeError(f"{name} has no business date to be read at")
    if processing_time is not None:
        if isinstance(processing_time, datetime.date) and not isinstance(processing_time, datetime.datetime):
            processing_time = datetime.datetime.combine(processing_time, datetime.time.min)
        if isinstance(milestone, GraphQLBiTemporalMilestone):
            args[milestone.processing_argument] = _argument_value(processing_time)
        elif isinstance(milestone, GraphQLProcessingMilestone):
            args[milestone.argument_name] = _argument_value(processing_time)
        else:
            raise TypeError(f"{name} has no processing time to be read at")
    return args


def association_segment(name: str, many: bool, kwargs: dict, fields: dict[str, str], milestone: Any = None,
                        business_date: Any = None, processing_time: Any = None) -> str:
    """The path segment for navigating an association with equality filters on the target's properties, read at the
    dates given if the target query has a milestone.

    kwargs are keyed by the target's Python property names; fields maps those names to GraphQL field names.
    """
    unknown = sorted(set(kwargs) - set(fields))
    if unknown:
        raise TypeError(f"Unknown filter {', '.join(unknown)} for {name}: expected one of {', '.join(sorted(fields))}")
    args = temporal_arguments(name, milestone, business_date, processing_time)
    return format_path_segment(name, many, {fields[key]: value for key, value in kwargs.items()}, args)
