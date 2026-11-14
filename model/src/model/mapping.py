from typing import Any, Generic, Sequence, TypeVar

from model.m3 import Class, Property


class PropertyMapping:
    def __init__(self, property: Property, target: Any, source_set_id: str, target_set_id: str | None = None):
        self.property = property
        self.target = target
        # The id of the class mapping, or set, the property mapping belongs to: where it starts from. Every property mapping has one, and a Mapping checks that it is
        # the id of the class mapping it is in. For a navigation from a class that is a union of sets it is the id of the set it starts from, and a property joined
        # between several pairs of sets has a mapping for each pair.
        self.source_set_id = source_set_id
        # For a navigation property (one whose type is a class): the id of the set it reaches, when the class it reaches is a union of sets; None where it is not,
        # and for a property that is not a navigation.
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
    """How a class is mapped: the mappings of its properties. It has an ``id``, which names it where a class is mapped in several ways (the sets of a union), and
    which the property mappings in it say is where they start from. It is the name of the class, unless it is given another; no two class mappings or sets of a Mapping
    have the same."""
    def __init__(self, clazz: Class, property_mappings: Sequence[PropertyMapping], milestone_mapping: MilestonePropertyMapping | None = None,
                 id: str | None = None):
        self.clazz = clazz
        self.property_mappings = property_mappings
        self.milestone_mapping = milestone_mapping
        self.id: str = id or clazz.name


S = TypeVar("S", bound="ClassMapping")


class UnionMapping(ClassMapping, Generic[S]):
    """A class that is the union of several class mappings, its sets: it holds them, each with an ``id`` of its own that is not another's. Its own ``id`` is the
    name of the class unless it is given another, and its property mappings are those of its sets, on the union of them."""
    def __init__(self, clazz: Class, sets: Sequence[S], property_mappings: Sequence[PropertyMapping],
                 milestone_mapping: MilestonePropertyMapping | None = None, id: str | None = None):
        super().__init__(clazz, property_mappings, milestone_mapping, id)
        if len(sets) < 2:
            raise ValueError(f"{clazz.name}: a union of sets has at least two sets, not {len(sets)}")
        if any(each.id == each.clazz.name for each in sets):
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
    return [each.id for each in mapping.sets] if isinstance(mapping, UnionMapping) else []


def _check_set_ids(mappings: Sequence[ClassMapping]) -> None:
    """The ids of the class mappings and the sets are all different; a property mapping starts from the class mapping it is in (a navigation from a class that is
    a union of sets, from one of its sets), and the one that reaches a class that is a union of sets says which of its sets it reaches, as one that does not says
    nothing: otherwise a join to the union is a join to every set, which is rarely meant, and what it joins is not written anywhere."""
    ids: list[str] = []
    for cm in mappings:
        ids += [cm.id, *_set_ids(cm)]
    for set_id in ids:
        if ids.count(set_id) > 1:
            raise ValueError(f"the id '{set_id}' is the id of more than one class mapping")
    by_class = {cm.clazz.name: cm for cm in mappings}
    for cm in mappings:
        for sets_own in ([cm] + (list(cm.sets) if isinstance(cm, UnionMapping) else [])):
            for pm in sets_own.property_mappings:
                navigation = isinstance(pm.property.type, Class)
                from_a_set = isinstance(cm, UnionMapping) and sets_own is cm and navigation
                owner = f"the property {pm.property.id} of {cm.clazz.name}"
                if from_a_set:
                    if pm.source_set_id not in _set_ids(cm):
                        raise ValueError(f"'{pm.source_set_id}' is not a set of {cm.clazz.name} ({', '.join(_set_ids(cm))}): {owner} starts from one of them")
                elif pm.source_set_id != sets_own.id:
                    raise ValueError(f"{owner} has the source set id '{pm.source_set_id}', but the class mapping it belongs to is '{sets_own.id}'")
                if not navigation:
                    if pm.target_set_id:
                        raise ValueError(f"{owner} is not a navigation: it has no target set id")
                    continue
                target = by_class.get(pm.property.type.name)  # type: ignore[union-attr]
                target_ids = _set_ids(target)
                if target_ids and pm.target_set_id is None:
                    raise ValueError(f"{owner} reaches a union of sets: it needs a target set id, one of {', '.join(target_ids)}")
                if target_ids and pm.target_set_id not in target_ids:
                    raise ValueError(f"'{pm.target_set_id}' is not a set of {pm.property.type.name} ({', '.join(target_ids)})")  # type: ignore[union-attr]
                if not target_ids and pm.target_set_id is not None and target is not None:
                    raise ValueError(f"{pm.property.type.name} is not a union of sets: {owner.split(' of ')[0]} has no target set id")  # type: ignore[union-attr]
