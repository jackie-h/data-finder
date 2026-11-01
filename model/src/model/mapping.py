from typing import Any, Sequence

from model.m3 import Class, Property


class PropertyMapping:
    def __init__(self, property: Property, target: Any):
        self.property = property
        self.target = target

class MilestonePropertyMapping:
    def __init(self):
        pass

class ProcessingDateMilestonesPropertyMapping(MilestonePropertyMapping):
    def __init__(self, _in: PropertyMapping, _out: PropertyMapping, infinite_datetime: str | None = None):
        self._in = _in
        self._out = _out
        self.infinite_datetime = infinite_datetime

class SingleProcessingDatetimeMilestonePropertyMapping(MilestonePropertyMapping):
    def __init__(self, _datetime: PropertyMapping):
        self._datetime = _datetime


class SingleBusinessDateMilestonePropertyMapping(MilestonePropertyMapping):
    def __init__(self, _date: PropertyMapping):
        self._date = _date


class BusinessDateAndProcessingMilestonePropertyMapping(SingleBusinessDateMilestonePropertyMapping,
                                                        ProcessingDateMilestonesPropertyMapping):
    def __init__(self, _date: PropertyMapping, _in: PropertyMapping, _out: PropertyMapping,
                 infinite_datetime: str | None = None):
        SingleBusinessDateMilestonePropertyMapping.__init__(self, _date)
        ProcessingDateMilestonesPropertyMapping.__init__(self, _in, _out, infinite_datetime)


class BusinessDateRangeMilestonePropertyMapping(MilestonePropertyMapping):
    def __init__(self, _date_from: PropertyMapping, _date_to: PropertyMapping, infinite_datetime: str | None = None):
        self._date_from = _date_from
        self._date_to = _date_to
        self.infinite_datetime = infinite_datetime


class BiTemporalMilestonePropertyMapping(BusinessDateRangeMilestonePropertyMapping,
                                         ProcessingDateMilestonesPropertyMapping):
    def __init__(self, _date_from: PropertyMapping, _date_to: PropertyMapping,
                 _in: PropertyMapping, _out: PropertyMapping, infinite_datetime: str | None = None):
        BusinessDateRangeMilestonePropertyMapping.__init__(self, _date_from, _date_to, infinite_datetime)
        ProcessingDateMilestonesPropertyMapping.__init__(self, _in, _out, infinite_datetime)

class ClassMapping:
    def __init__(self, clazz: Class, property_mappings: Sequence[PropertyMapping], milestone_mapping: MilestonePropertyMapping | None = None):
        self.clazz = clazz
        self.property_mappings = property_mappings
        self.milestone_mapping = milestone_mapping

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