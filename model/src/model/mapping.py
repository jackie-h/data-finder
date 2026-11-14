from typing import Any, Generic, Sequence, TypeVar

from model.m3 import Class, Property


class PropertyMapping:
    def __init__(self, property: Property, target: Any, source_set_id: str | None = None, target_set_id: str | None = None):
        self.property = property
        self.target = target
        # For a navigation property between classes that are unions of sets, as Legend's ``sourceSetImplementationId`` and ``targetSetImplementationId``: the
        # ids of the set the mapping starts from and the one it reaches. ``x[y0, x0]`` is a mapping of ``x`` from the set ``x0`` to the set ``y0``, and a property
        # joined between several pairs of sets has a mapping for each pair. None for a class that is not a union of sets.
        self.source_set_id = source_set_id
        self.target_set_id = target_set_id

class MilestonePropertyMapping:
    def __init(self):
        pass

class ProcessingDateMilestonesPropertyMapping(MilestonePropertyMapping):
    def __init__(self, _in: PropertyMapping, _out: PropertyMapping, processing_infinite_datetime: str | None = None):
        self._in = _in
        self._out = _out
        self.processing_infinite_datetime = processing_infinite_datetime

class SingleProcessingDatetimeMilestonePropertyMapping(MilestonePropertyMapping):
    def __init__(self, _datetime: PropertyMapping):
        self._datetime = _datetime


class SingleBusinessDateMilestonePropertyMapping(MilestonePropertyMapping):
    def __init__(self, _date: PropertyMapping):
        self._date = _date


class BusinessDateAndProcessingMilestonePropertyMapping(SingleBusinessDateMilestonePropertyMapping,
                                                        ProcessingDateMilestonesPropertyMapping):
    def __init__(self, _date: PropertyMapping, _in: PropertyMapping, _out: PropertyMapping,
                 processing_infinite_datetime: str | None = None):
        SingleBusinessDateMilestonePropertyMapping.__init__(self, _date)
        ProcessingDateMilestonesPropertyMapping.__init__(self, _in, _out, processing_infinite_datetime)


class BusinessDateRangeMilestonePropertyMapping(MilestonePropertyMapping):
    def __init__(self, _date_from: PropertyMapping, _date_to: PropertyMapping,
                 business_infinite_datetime: str | None = None):
        self._date_from = _date_from
        self._date_to = _date_to
        self.business_infinite_datetime = business_infinite_datetime


class BiTemporalMilestonePropertyMapping(BusinessDateRangeMilestonePropertyMapping,
                                         ProcessingDateMilestonesPropertyMapping):
    def __init__(self, _date_from: PropertyMapping, _date_to: PropertyMapping,
                 _in: PropertyMapping, _out: PropertyMapping, processing_infinite_datetime: str | None = None,
                 business_infinite_datetime: str | None = None):
        BusinessDateRangeMilestonePropertyMapping.__init__(self, _date_from, _date_to, business_infinite_datetime)
        ProcessingDateMilestonesPropertyMapping.__init__(self, _in, _out, processing_infinite_datetime)

class ClassMapping:
    """How a class is mapped, as Legend's ``SetImplementation``: it has an ``id``, which names it where a class is mapped in several ways (the sets of a union),
    and the mappings of the properties of the class."""
    def __init__(self, clazz: Class, property_mappings: Sequence[PropertyMapping], milestone_mapping: MilestonePropertyMapping | None = None,
                 id: str | None = None):
        self.clazz = clazz
        self.property_mappings = property_mappings
        self.milestone_mapping = milestone_mapping
        self.id = id


S = TypeVar("S", bound="ClassMapping")


class UnionMapping(ClassMapping, Generic[S]):
    """A class that is the union of several set implementations, as Legend's ``OperationSetImplementation`` with its union operation: it holds the sets it is the
    union of, each a class mapping with an ``id`` of its own. It has no ``id`` of its own, and its property mappings are those of its sets, on the union of them."""
    def __init__(self, clazz: Class, sets: Sequence[S], property_mappings: Sequence[PropertyMapping],
                 milestone_mapping: MilestonePropertyMapping | None = None, id: str | None = None):
        super().__init__(clazz, property_mappings, milestone_mapping, id)
        self.sets: list[S] = list(sets)

class Mapping:
    def __init__(self, name: str, mappings: list[ClassMapping]):
        seen = {}
        for cm in mappings:
            cls_name = cm.clazz.name
            if cls_name in seen:
                raise ValueError(f"Class '{cls_name}' is mapped more than once in mapping '{name}'")
            seen[cls_name] = True
        self.name = name
        self.mappings = mappings