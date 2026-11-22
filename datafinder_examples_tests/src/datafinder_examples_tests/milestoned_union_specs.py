"""
Test specifications for a milestoned step read through a union, using milestoned_union_mapping.md.

Order, Product and ProductClassification are each the union of two sets that read the same table, so every row is there twice, and every
set of a class joins every set of the class it reaches. Product and ProductClassification are milestoned by a business snapshot date; Order
is not. A query reads Order, then its product at a date, then the classification of that product at the same date.

    OrderTable                                          — id INT, prodFk INT: order 1 has product 1, order 2 has product 2
    ProductTableWithBusinessSnapshotMilestoning         — id INT, name VARCHAR, type VARCHAR, snapshotDate DATE:
                                                          product 1 on 2015-08-15, product 2 on 2015-08-26 (ProductName1) and 2015-10-16,
                                                          product 3 (OPTION) on 2015-08-26
    ProductClassificationTableWithBusinessSnapshotMilestoning — type VARCHAR, type_description VARCHAR, snapshotDate DATE:
                                                          STOCK has a description on 2015-08-15 (V1), 08-26 (V2), 09-15 (V3) and 10-17 (V4)

On 2015-08-26 only order 2 has a product (product 2, STOCK), and the one classification it has is STOCK DESC-V2. Each of its rows is
found for each of the 2 sets of Order and, through it, for each of the 2 sets of Product, so 4 rows; the classification is found in each
of its 2 sets.

The first milestoned step, the product, is read once for the filter and the columns. A milestoned step reached through it, the
classification, is joined once for the filter and once for the columns, as Legend joins it: the filter is for one row of the table and the
column is read from another, so a row is found for each pair, 2 x 2 for the sets of the classification.
"""
import numpy as np

from datafinder_examples_tests.spec import FinderSpec, TestExpectation

_DATE = "2015-08-26"
_STOCK_DESCRIPTION = "STOCK DESC-V2"

# 2 sets of Order x 2 sets of Product x 2 sets of ProductClassification
_PAIRS_OF_SETS = 8

def _over_the_product(f, columns_and_condition):
    """A query over the product as of the date, a step that the filter and the columns both use, as Legend's ``$o.product(%date)`` is
    written twice for one step. A step made by another call is another step, with joins of its own."""
    columns, condition = columns_and_condition(f, f.product(business_date=_DATE))
    return f.find_all(columns, condition) if condition is not None else f.find_all(columns)


MILESTONED_UNION_ORDER_FINDER_SPECS = FinderSpec(
    finder_name="OrderFinder",
    mapping_file="milestoned_union_mapping.md",
    expectations=[
        TestExpectation(
            name="a_union_finds_each_row_for_each_of_its_sets",
            query=lambda f: f.find_all([f.id_()]).order_by(f.id_().ascending()),
            expected_columns=["Id"],
            expected_result=np.array([[1], [1], [2], [2]], dtype=object),
        ),
        TestExpectation(
            name="a_milestoned_step_is_found_for_each_pair_of_sets_and_is_missing_where_the_date_has_no_row",
            query=lambda f: _over_the_product(f, lambda f, p: ([f.id_(), p.name()], None)).order_by(f.id_().ascending()),
            expected_columns=["Id", "Product Name"],
            # order 1's product is dated 2015-08-15, so on the 26th it has none: its 2 rows keep a missing name; order 2's is found
            # for each of its 2 sets and, through each, each of the 2 sets of Product
            expected_result=np.array([[1, None]] * 2 + [[2, "ProductName1"]] * 4, dtype=object),
        ),
        TestExpectation(
            name="the_first_milestoned_step_is_one_join_for_the_filter_and_the_columns",
            query=lambda f: _over_the_product(f, lambda f, p: ([p.id_()], p.name().eq("ProductName1"))),
            expected_columns=["Product Id"],
            expected_result=np.array([[2]] * 4, dtype=object),
        ),
        TestExpectation(
            name="a_milestoned_step_that_is_only_filtered_is_one_join",
            query=lambda f: _over_the_product(
                f, lambda f, p: ([f.id_()], p.classification().description().eq(_STOCK_DESCRIPTION))),
            expected_columns=["Id"],
            expected_result=np.array([[2]] * _PAIRS_OF_SETS, dtype=object),
        ),
        TestExpectation(
            name="a_milestoned_step_that_is_only_read_is_one_join",
            query=lambda f: _over_the_product(f, lambda f, p: ([f.id_(), p.classification().type_()], None))
            .order_by(f.id_().ascending()),
            expected_columns=["Id", "Product Classification Type"],
            # order 1 has no product that day, so its 2 rows have no classification; order 2 has the 8 pairs of sets
            expected_result=np.array([[1, None]] * 2 + [[2, "STOCK"]] * _PAIRS_OF_SETS, dtype=object),
        ),
        TestExpectation(
            name="a_milestoned_step_reached_through_another_is_two_joins_when_it_is_filtered_and_read",
            query=lambda f: _over_the_product(
                f, lambda f, p: ([p.classification().type_()], p.classification().description().eq(_STOCK_DESCRIPTION))),
            expected_columns=["Product Classification Type"],
            # the 8 pairs of sets, each found for each of the 2 sets of the classification that the column is read from
            expected_result=np.array([["STOCK"]] * (_PAIRS_OF_SETS * 2), dtype=object),
        ),
        TestExpectation(
            name="every_column_that_is_read_shares_the_join_of_the_step_that_is_not_the_filter",
            query=lambda f: _over_the_product(
                f, lambda f, p: ([p.classification().type_(), p.classification().description()],
                                 p.classification().description().eq(_STOCK_DESCRIPTION))),
            expected_columns=["Product Classification Type", "Product Classification Description"],
            expected_result=np.array([["STOCK", _STOCK_DESCRIPTION]] * (_PAIRS_OF_SETS * 2), dtype=object),
        ),
        TestExpectation(
            name="a_step_made_by_another_call_is_another_step_with_joins_of_its_own",
            query=lambda f: f.find_all([f.product(business_date=_DATE).id_()],
                                       f.product(business_date=_DATE).name().eq("ProductName1")),
            expected_columns=["Product Id"],
            # the filter's product and the column's product are not the same step, each joined from the 2 sets of Order, so 2 x 2 x 2
            expected_result=np.array([[2]] * 8, dtype=object),
        ),
    ],
)
