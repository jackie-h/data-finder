from typing import Any

from model.m3 import Class, Property


class PropertyMapping:
    def __init__(self, property: Property, target: Any):
        self.property = property
        self.target = target

class MilestonePropertyMapping:
    def __init(self):
        pass

class ProcessingDateMilestonesPropertyMapping(MilestonePropertyMapping):
    def __init__(self, _in: PropertyMapping, _out: PropertyMapping):
        self._in = _in
        self._out = _out

class SingleBusinessDateMilestonePropertyMapping(MilestonePropertyMapping):
    def __init__(self, _date: PropertyMapping):
        self._date = _date


class BusinessDateAndProcessingMilestonePropertyMapping(SingleBusinessDateMilestonePropertyMapping,
                                                        ProcessingDateMilestonesPropertyMapping):
    def __init__(self, _date: PropertyMapping, _in: PropertyMapping, _out: PropertyMapping):
        SingleBusinessDateMilestonePropertyMapping.__init__(self, _date)
        ProcessingDateMilestonesPropertyMapping.__init__(self, _in, _out)


class BiTemporalMilestonePropertyMapping(ProcessingDateMilestonesPropertyMapping):
    def __init__(self, _date_from: PropertyMapping, _date_to: PropertyMapping,
                 _in: PropertyMapping, _out: PropertyMapping):
        super().__init__(_in, _out)
        self._date_from = _date_from
        self._date_to = _date_to

class ClassMapping:
    def __init__(self, clazz: Class, property_mappings: list[PropertyMapping], milestone_mapping: MilestonePropertyMapping = None):
        self.clazz = clazz
        self.property_mappings = property_mappings
        self.milestone_mapping = milestone_mapping

class Mapping:
    def __init__(self, name: str, mappings: list[ClassMapping]):
        self.name = name
        self.mappings = mappings