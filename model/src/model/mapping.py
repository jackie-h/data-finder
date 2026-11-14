from typing import Any, Generic, Sequence, TypeVar

from model.m3 import Class, Property


class PropertyMapping:
    def __init__(self, property: Property, target: Any, source_set_id: str | None = None, target_set_id: str | None = None):
        self.property = property
        self.target = target
        # For a navigation property (one whose type is a class): the ids of the set the mapping starts from and the set it reaches. A class that is a union of sets
        # has its sets named, and a navigation from or to it says which; a property joined between several pairs of sets has a mapping for each pair. Mandatory
        # where the class it starts from or the class it reaches is a union of sets (a Mapping checks), and None where it is not.
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
    """How a class is mapped: the mappings of its properties. It has an ``id``, which names it where a class is mapped in several ways (the sets of a union)."""
    def __init__(self, clazz: Class, property_mappings: Sequence[PropertyMapping], milestone_mapping: MilestonePropertyMapping | None = None,
                 id: str | None = None):
        self.clazz = clazz
        self.property_mappings = property_mappings
        self.milestone_mapping = milestone_mapping
        self.id = id


S = TypeVar("S", bound="ClassMapping")


class UnionMapping(ClassMapping, Generic[S]):
    """A class that is the union of several class mappings, its sets: it holds them, each with an ``id`` of its own that is not another's. It has no ``id`` of
    its own, and its property mappings are those of its sets, on the union of them."""
    def __init__(self, clazz: Class, sets: Sequence[S], property_mappings: Sequence[PropertyMapping],
                 milestone_mapping: MilestonePropertyMapping | None = None, id: str | None = None):
        super().__init__(clazz, property_mappings, milestone_mapping, id)
        if len(sets) < 2:
            raise ValueError(f"{clazz.name}: a union of sets has at least two sets, not {len(sets)}")
        if any(not each.id for each in sets):
            raise ValueError(f"every set of {clazz.name} needs an id")
        ids = [each.id for each in sets]
        for set_id in ids:
            if ids.count(set_id) > 1:
                raise ValueError(f"the sets of {clazz.name} have the id '{set_id}' twice")
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
        _check_set_ids(mappings)


def _set_ids(mapping: ClassMapping | None) -> list[str]:
    return [each.id for each in mapping.sets if each.id] if isinstance(mapping, UnionMapping) else []


def _check_set_ids(mappings: Sequence[ClassMapping]) -> None:
    """A navigation from or to a class that is a union of sets says which set it starts from and which it reaches, and one that is not a union says neither:
    otherwise a join to the union is a join to every set, which is rarely meant, and what it joins is not written anywhere."""
    by_class = {cm.clazz.name: cm for cm in mappings}
    for cm in mappings:
        for pm in cm.property_mappings:
            owner = f"the property {pm.property.id} of {cm.clazz.name}"
            if not isinstance(pm.property.type, Class):
                if pm.source_set_id or pm.target_set_id:
                    raise ValueError(f"{owner} is not a navigation: it has no set ids")
                continue
            target = by_class.get(pm.property.type.name)
            for role, mapped, set_id, name in (("source", cm, pm.source_set_id, cm.clazz.name), ("target", target, pm.target_set_id, pm.property.type.name)):
                ids = _set_ids(mapped)
                if ids and set_id is None:
                    raise ValueError(f"{owner} {'starts from' if role == 'source' else 'reaches'} a union of sets: it needs a {role} set id, one of {', '.join(ids)}")
                if ids and set_id not in ids:
                    raise ValueError(f"'{set_id}' is not a set of {name} ({', '.join(ids)})")
                if not ids and set_id is not None and (role == "source" or mapped is not None):
                    raise ValueError(f"{name} is not a union of sets: {owner.split(' of ')[0]} has no {role} set id")
