from abc import ABC, abstractmethod
from typing import Any, Generic, Sequence, TypeVar

from model.m3 import Class, Property


class PropertyMapping:
    """How a property that holds a value is mapped (a column, say). A property typed by a class is mapped by a NavigationPropertyMapping."""
    def __init__(self, property: Property, target: Any, source_set_id: str):
        self.property = property
        self.target = target
        # The id of the class mapping, or set, the property mapping belongs to: where it starts from. Every property mapping has one, and a Mapping checks that it is
        # the id of the class mapping it is in. For a navigation from a class that is a union of sets it is the id of the set it starts from, and a property joined
        # between several pairs of sets has a mapping for each pair.
        self.source_set_id = source_set_id


class NavigationPropertyMapping(PropertyMapping):
    """How a property typed by a class is mapped: where it starts from, and the class mapping it reaches. The ``target_set_id`` is the id of that class mapping, which
    is the name of its class, or of the set of it when it is a union of sets, or the embedded mapping that holds it in the row. A Mapping checks that it is the id
    of a class mapping that there is."""
    def __init__(self, property: Property, target: Any, source_set_id: str, target_set_id: str):
        super().__init__(property, target, source_set_id)
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

class InstanceSetMapping(ABC):
    """A class mapped by the mappings of its properties, with an ``id``: what a class mapping and a class mapped in the row of another have in common. It is
    abstract: a class is mapped as one or the other. The ``id`` names it, and is the id that the property mappings in it say they start from."""
    @abstractmethod
    def __init__(self, clazz: Class, property_mappings: Sequence[PropertyMapping], id: str):
        self.clazz = clazz
        self.property_mappings = property_mappings
        self.id: str = id


class ClassMapping(InstanceSetMapping):
    """How a class is mapped at the top: the mappings of its properties, and its milestoning. Its ``id`` names it where a class is mapped in several ways (the
    sets of a union); it is the name of the class, unless it is given another, and no two class mappings or sets of a Mapping have the same."""
    def __init__(self, clazz: Class, property_mappings: Sequence[PropertyMapping], milestone_mapping: MilestonePropertyMapping | None = None,
                 id: str | None = None):
        super().__init__(clazz, property_mappings, id or clazz.name)
        self.milestone_mapping = milestone_mapping


class EmbeddedSetMapping(InstanceSetMapping):
    """A partial class mapping embedded inline in the owning (root) table.

    Represents a chained property (e.g. account.name) satisfied by a flat column on the
    root table, avoiding the join that would normally be required to reach it. Nested
    property_mappings entries may themselves target a Column (leaf) or a further nested
    EmbeddedSetMapping (e.g. account.branch.city) — all such columns live on the root table —
    or a Join, for a property of the embedded class that reaches another table (e.g.
    classification.system), whose first column is a column of the root table.

    Its ``id`` is that of what holds it and the path to it, ``Trade_account``, and the property mappings in it start from it.
    """
    def __init__(self, clazz: Class, property_mappings: Sequence[PropertyMapping], id: str):
        super().__init__(clazz, property_mappings, id)
        for pm in property_mappings:
            if pm.source_set_id != id:
                raise ValueError(f"the property {pm.property.id} of {clazz.name} embedded as {id} has the source set id '{pm.source_set_id}', not '{id}'")


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
    """The ids of the class mappings and the sets are all different. A property mapping starts from the class mapping it is in (a navigation from a class that is
    a union of sets, from one of its sets), and a navigation reaches the class mapping of the class it is typed by (or one of its sets, if that class is a union, or
    the embedded mapping that holds it in the row, which is the value of the property): otherwise a join to a union is a join to every set, which is rarely meant, and
    what it joins is not written anywhere."""
    ids: list[str] = []
    for cm in mappings:
        ids += [cm.id, *_set_ids(cm)]
    for set_id in ids:
        if ids.count(set_id) > 1:
            raise ValueError(f"the id '{set_id}' is the id of more than one class mapping")
    by_class = {cm.clazz.name: cm for cm in mappings}
    for cm in mappings:
        for holder in ([cm] + (list(cm.sets) if isinstance(cm, UnionMapping) else [])):
            _check_property_mappings(holder, cm, by_class)


def _check_property_mappings(holder: InstanceSetMapping, owner: ClassMapping, by_class: dict[str, ClassMapping]) -> None:
    """The property mappings of ``holder``, which is ``owner``, or one of its sets, or an embedded mapping in one of them."""
    for pm in holder.property_mappings:
        where = f"the property {pm.property.id} of {holder.clazz.name}"
        typed_by = pm.property.type
        from_a_set = isinstance(owner, UnionMapping) and holder is owner and isinstance(typed_by, Class)
        if from_a_set:
            if pm.source_set_id not in _set_ids(owner):
                raise ValueError(f"'{pm.source_set_id}' is not a set of {owner.clazz.name} ({', '.join(_set_ids(owner))}): {where} starts from one of them")
        elif pm.source_set_id != holder.id:
            raise ValueError(f"{where} has the source set id '{pm.source_set_id}', but the class mapping it belongs to is '{holder.id}'")
        if not isinstance(typed_by, Class):
            if isinstance(pm, NavigationPropertyMapping):
                raise ValueError(f"{where} is not typed by a class: it is not mapped by a NavigationPropertyMapping")
            continue
        if not isinstance(pm, NavigationPropertyMapping):
            raise ValueError(f"{where} is typed by a class: it is mapped by a NavigationPropertyMapping")
        if isinstance(pm.target, EmbeddedSetMapping):  # mapped in the row: it reaches the embedded mapping, not the class's own
            if pm.target_set_id != pm.target.id:
                raise ValueError(f"{where} reaches '{pm.target_set_id}', but it is mapped by the embedded class mapping '{pm.target.id}'")
            _check_property_mappings(pm.target, owner, by_class)
            continue
        target = by_class.get(typed_by.name)
        target_ids = _set_ids(target)
        if target_ids:
            if pm.target_set_id not in target_ids:
                raise ValueError(f"{where} reaches '{pm.target_set_id}', which is not a set of {typed_by.name} ({', '.join(target_ids)})")
        elif pm.target_set_id != (target.id if target is not None else typed_by.name):
            raise ValueError(f"{where} reaches '{pm.target_set_id}', but " + (
                f"the class mapping of {typed_by.name} is '{target.id}'" if target is not None else
                f"{typed_by.name} has no class mapping, so it can only be '{typed_by.name}'"))
