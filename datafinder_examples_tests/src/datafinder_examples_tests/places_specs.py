"""
Test specifications for a navigation with a filter of its own (places_mapping.md), as Legend's ``x.locations->filter(f | ...)``:
the condition is in the ``ON`` of the left outer join of the navigation, so a row it rejects leaves the joined columns NULL and
drops no row of the query. A condition in the query is a ``WHERE`` and drops the row.

    geo.people — id, name; geo.regions — id, name; geo.locations — id, place, visits, person_id, region_id

Ann (1) has been to Hoboken (3 times), New York (5) and Boston (2), Bob (2) to Boston (4) and Cy (3) to New York (1). Boston and
Hoboken are in the East region (1), and Cy's New York in the West (2).
"""

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

PLACES_MAPPING = "places_mapping.md"
_HOBOKEN_OR_NEW_YORK = ["Hoboken", "New York"]


def _filter_keeps_every_person(f):
    locations = f.locations().filter(lambda each: each.place().is_in(_HOBOKEN_OR_NEW_YORK))
    return f.find_all([f.name(), locations.place()]).order_by(f.name().ascending(), locations.place().ascending())


def _filter_in_the_query_drops_the_person(f):
    return f.find_all([f.name(), f.locations().place()], f.locations().place().is_in(_HOBOKEN_OR_NEW_YORK)) \
            .order_by(f.name().ascending(), f.locations().place().ascending())


def _one_filtered_navigation_read_twice(f):
    locations = f.locations().filter(lambda each: each.place().is_in(_HOBOKEN_OR_NEW_YORK))
    return f.find_all([f.name(), locations.place(), locations.visits()]) \
            .order_by(f.name().ascending(), locations.place().ascending())


def _two_filters_on_one_navigation(f):
    in_new_york = f.locations().filter(lambda each: each.place().eq("New York"))
    in_new_york_or_hoboken = f.locations().filter(lambda each: each.place().is_in(["New York", "Hoboken"]))
    return f.find_all([f.name(), in_new_york.place(), in_new_york_or_hoboken.visits()]) \
            .order_by(f.name().ascending(), in_new_york_or_hoboken.visits().ascending())


def _equal_filters_written_twice(f):
    # as Legend's two columns of x.locations->filter(f | $f.place in ['Hoboken','New York']), each with the filter written out
    return f.find_all([
        f.name(),
        f.locations().filter(lambda each: each.place().is_in(["Hoboken", "New York"])).place(),
        f.locations().filter(lambda each: each.place().is_in(["Hoboken", "New York"])).visits(),
    ]).order_by(f.name().ascending(), f.locations().filter(lambda each: each.place().is_in(["Hoboken", "New York"])).place().ascending())


def _same_values_in_another_order(f):
    listed = f.locations().filter(lambda each: each.place().is_in(["Hoboken", "New York"]))
    reversed_ = f.locations().filter(lambda each: each.place().is_in(["New York", "Hoboken"]))
    return f.find_all([f.name(), listed.place(), reversed_.visits()]) \
            .order_by(f.name().ascending(), listed.place().ascending(), reversed_.visits().ascending())


def _same_condition_with_another_value(f):
    in_new_york = f.locations().filter(lambda each: each.place().eq("New York"))
    in_boston = f.locations().filter(lambda each: each.place().eq("Boston"))
    return f.find_all([f.name(), in_new_york.place(), in_boston.visits()]).order_by(f.name().ascending())


def _filter_of_a_filter(f):
    frequent = f.locations().filter(lambda each: each.place().is_in(_HOBOKEN_OR_NEW_YORK)).filter(lambda each: each.visits() > 3)
    return f.find_all([f.name(), frequent.place(), frequent.visits()]).order_by(f.name().ascending())


def _filter_below_a_navigation(f):
    locations = f.locations()
    east = locations.region().filter(lambda each: each.name().eq("East"))
    return f.find_all([f.name(), locations.place(), east.name()]) \
            .order_by(f.name().ascending(), locations.place().ascending())


def _filter_reading_through_another_navigation(f):
    in_the_east = f.locations().filter(lambda each: each.region().name().eq("East"))
    return f.find_all([f.name(), in_the_east.place()]).order_by(f.name().ascending(), in_the_east.place().ascending())


def _filter_reading_through_another_navigation_or_a_column(f):
    busy_or_in_the_west = f.locations().filter(lambda each: each.region().name().eq("West").or_(each.visits() > 3))
    return f.find_all([f.name(), busy_or_in_the_west.place()]).order_by(f.name().ascending())


PLACES_PERSON_FINDER_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file=PLACES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_filter_on_a_navigation_keeps_every_person",
            # Bob has been only to Boston, so he has no location that holds, and is there with none
            query=_filter_keeps_every_person,
        ),
        TestExpectation(
            name="the_same_condition_in_the_query_drops_the_person_with_no_location",
            query=_filter_in_the_query_drops_the_person,
        ),
        TestExpectation(
            name="a_filtered_navigation_that_two_columns_read_is_one_join",
            query=_one_filtered_navigation_read_twice,
        ),
        TestExpectation(
            name="two_filters_on_one_navigation_are_two_joins",
            # the first join holds only New York, the second Hoboken as well, so Ann's New York is beside each of her two
            query=_two_filters_on_one_navigation,
        ),
        TestExpectation(
            name="filters_with_equal_conditions_are_one_join_when_each_is_written_out",
            query=_equal_filters_written_twice,
        ),
        TestExpectation(
            name="filters_with_different_conditions_are_different_joins_even_for_the_same_values",
            # the second filter lists the values the other way round: a condition that is written differently is not the same
            # condition, so it is a join of its own, and each of Ann's two rows is beside each of her two
            query=_same_values_in_another_order,
        ),
        TestExpectation(
            name="the_same_condition_with_another_value_is_a_join_of_its_own",
            query=_same_condition_with_another_value,
        ),
        TestExpectation(
            name="a_filter_of_a_filtered_navigation_holds_both_conditions",
            # of Hoboken and New York, only Ann's New York has been visited more than 3 times
            query=_filter_of_a_filter,
        ),
        TestExpectation(
            name="a_filter_on_a_navigation_below_another",
            # every location is read; only the region of Cy's New York is not East
            query=_filter_below_a_navigation,
        ),
        TestExpectation(
            name="a_filter_whose_condition_reads_through_another_navigation",
            # the region of a location is joined for the condition, which holds in the join of the locations: Cy's only
            # location is in the West, so he is there with none
            query=_filter_reading_through_another_navigation,
        ),
        TestExpectation(
            name="a_filter_of_a_column_or_a_column_of_another_navigation",
            # in the West (Cy's New York) or visited more than 3 times (Ann's New York, Bob's Boston)
            query=_filter_reading_through_another_navigation_or_a_column,
        ),
    ],
)

PLACES_LOCATION_FINDER_SPECS = FinderSpec(
    finder_name="LocationFinder",
    mapping_file=PLACES_MAPPING,
    expectations=[
        TestExpectation(
            name="a_filter_on_a_navigation_to_one_leaves_the_others_without_it",
            query=lambda f: f.find_all([f.id_(), f.person().filter(lambda each: each.name().eq("Ann")).name()])
                             .order_by(f.id_().ascending()),
        ),
    ],
)
