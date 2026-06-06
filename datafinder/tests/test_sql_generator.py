import datetime

from datafinder import StringAttribute, to_sql
from datafinder import IntegerAttribute
from datafinder.sql_generator import SQLQueryGenerator
from datafinder.typed_attributes import DoubleAttribute, DateAttribute, DateTimeAttribute
from model.relational import Table, Column, NoOperation, CountAllOperation, JoinOperation, JoinTreeNodeOperation


def _make_table_and_attr():
    col = Column("NAME", "VARCHAR")
    table = Table("accounts", [col])
    attr = StringAttribute("Name", "NAME", "VARCHAR", "accounts")
    return table, attr


def _make_multi_col_table():
    name_col = Column("NAME", "VARCHAR")
    id_col = Column("ID", "INT")
    table = Table("accounts", [name_col, id_col])
    name_attr = StringAttribute("Name", "NAME", "VARCHAR", "accounts")
    id_attr = IntegerAttribute("Id", "ID", "INT", "accounts")
    return table, name_attr, id_attr


class TestNoOperationFilter:

    def test_build_filter_returns_empty_string(self):
        gen = SQLQueryGenerator()
        assert gen.build_filter(NoOperation()) == ""

    def test_no_filter_produces_no_where_clause(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, NoOperation())
        assert "WHERE" not in sql

    def test_no_filter_still_produces_select_and_from(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, NoOperation())
        assert "SELECT" in sql
        assert "FROM" in sql
        assert "accounts" in sql

    def test_with_filter_produces_where_clause(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, attr.eq("Acme"))
        assert "WHERE" in sql
        assert "'Acme'" in sql


class TestStringOperations:

    def test_ne_produces_not_equal(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, attr.ne("Acme"))
        assert "<>" in sql
        assert "'Acme'" in sql

    def test_contains_produces_like_with_wildcards(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, attr.contains("corp"))
        assert "LIKE" in sql
        assert "'%corp%'" in sql

    def test_starts_with_produces_like_with_trailing_wildcard(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, attr.starts_with("Acme"))
        assert "LIKE" in sql
        assert "'Acme%'" in sql

    def test_ends_with_produces_like_with_leading_wildcard(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, attr.ends_with("Inc"))
        assert "LIKE" in sql
        assert "'%Inc'" in sql

    def test_ne_no_like_in_sql(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, attr.ne("Acme"))
        assert "LIKE" not in sql

    def test_contains_has_where_clause(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, attr.contains("corp"))
        assert "WHERE" in sql

    def test_starts_with_has_where_clause(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, attr.starts_with("Acme"))
        assert "WHERE" in sql

    def test_ends_with_has_where_clause(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, attr.ends_with("Inc"))
        assert "WHERE" in sql


class TestOrderBy:

    def test_no_order_by_produces_no_order_clause(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, NoOperation())
        assert "ORDER BY" not in sql

    def test_asc_produces_order_by_clause(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, NoOperation(), [attr.ascending()])
        assert "ORDER BY" in sql
        assert "ASC" in sql

    def test_desc_produces_order_by_clause(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, NoOperation(), [attr.descending()])
        assert "ORDER BY" in sql
        assert "DESC" in sql

    def test_asc_does_not_produce_desc(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, NoOperation(), [attr.ascending()])
        assert "DESC" not in sql

    def test_order_by_references_correct_column(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, NoOperation(), [attr.ascending()])
        assert "NAME" in sql.split("ORDER BY")[1]

    def test_multi_column_order_by(self):
        table, name_attr, id_attr = _make_multi_col_table()
        sql = to_sql(None, None, [name_attr, id_attr], table, NoOperation(),
                                          [name_attr.ascending(), id_attr.descending()])
        order_clause = sql.split("ORDER BY")[1]
        assert "ASC" in order_clause
        assert "DESC" in order_clause

    def test_order_by_with_filter(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, attr.eq("Acme"), [attr.ascending()])
        assert "WHERE" in sql
        assert "ORDER BY" in sql
        assert sql.index("WHERE") < sql.index("ORDER BY")

    def test_finder_result_order_by_chaining(self):
        from datafinder import FinderResult, convert_inputs_and_select
        table, attr = _make_table_and_attr()
        result = convert_inputs_and_select(None, None, [attr], table, NoOperation())
        assert isinstance(result, FinderResult)
        chained = result.order_by(attr.ascending())
        assert chained is result
        assert len(result._order_by) == 1


class TestCountAllOperation:

    def test_count_all_produces_count_star(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [CountAllOperation("accounts")], table, NoOperation())
        assert "COUNT(*)" in sql

    def test_count_all_has_count_alias(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [CountAllOperation("accounts")], table, NoOperation())
        assert '"Count"' in sql

    def test_count_all_includes_from_clause(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [CountAllOperation("accounts")], table, NoOperation())
        assert "FROM" in sql
        assert "accounts" in sql

    def test_count_all_with_filter(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [CountAllOperation("accounts")], table, attr.eq("Acme"))
        assert "COUNT(*)" in sql
        assert "WHERE" in sql

    def test_count_all_alongside_column(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr, CountAllOperation("accounts")], table, NoOperation())
        assert "COUNT(*)" in sql
        assert "NAME" in sql


class TestAttributeCount:

    def test_attribute_count_produces_count_column(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.count()], table, NoOperation())
        assert "COUNT(" in sql
        assert "NAME" in sql

    def test_attribute_count_does_not_produce_star(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.count()], table, NoOperation())
        assert "COUNT(*)" not in sql

    def test_attribute_count_with_filter(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.count()], table, attr.eq("Acme"))
        assert "COUNT(" in sql
        assert "WHERE" in sql

    def test_integer_attribute_count(self):
        table, name_attr, id_attr = _make_multi_col_table()
        sql = to_sql(None, None, [id_attr.count()], table, NoOperation())
        assert "COUNT(" in sql
        assert "ID" in sql


class TestWindowFunctions:

    def test_rank_method_first_produces_row_number(self):
        table, name_attr, id_attr = _make_multi_col_table()
        sql = to_sql(None, None, [id_attr.rank(method='first', order_by=[id_attr.ascending()])], table, NoOperation())
        assert "ROW_NUMBER()" in sql
        assert "OVER (" in sql
        assert "ORDER BY" in sql

    def test_rank_method_min_with_partition_and_order(self):
        table, name_attr, id_attr = _make_multi_col_table()
        sql = to_sql(
            None, None,
            [id_attr.rank(method='min', partition_by=[name_attr], order_by=[id_attr.descending()])],
            table, NoOperation(),
        )
        assert "RANK()" in sql
        assert "PARTITION BY" in sql
        assert "NAME" in sql
        assert "DESC" in sql

    def test_rank_method_dense(self):
        table, name_attr, id_attr = _make_multi_col_table()
        sql = to_sql(None, None, [id_attr.rank(method='dense', order_by=[id_attr.ascending()])], table, NoOperation())
        assert "DENSE_RANK()" in sql

    def test_rank_pct_true_produces_percent_rank(self):
        table, name_attr, id_attr = _make_multi_col_table()
        sql = to_sql(None, None, [id_attr.rank(pct=True, order_by=[id_attr.ascending()])], table, NoOperation())
        assert "PERCENT_RANK()" in sql

    def test_rank_pct_true_method_max_produces_cume_dist(self):
        table, name_attr, id_attr = _make_multi_col_table()
        sql = to_sql(None, None, [id_attr.rank(pct=True, method='max', order_by=[id_attr.ascending()])], table, NoOperation())
        assert "CUME_DIST()" in sql

    def test_shift_positive_periods_produces_lag(self):
        table, name_attr, id_attr = _make_multi_col_table()
        sql = to_sql(None, None, [id_attr.shift(1, order_by=[id_attr.ascending()])], table, NoOperation())
        assert "LAG(" in sql

    def test_shift_negative_periods_produces_lead(self):
        table, name_attr, id_attr = _make_multi_col_table()
        sql = to_sql(None, None, [id_attr.shift(-1, order_by=[id_attr.ascending()])], table, NoOperation())
        assert "LEAD(" in sql

    def test_shift_default_is_lag_by_one(self):
        table, name_attr, id_attr = _make_multi_col_table()
        sql = to_sql(None, None, [id_attr.shift(order_by=[id_attr.ascending()])], table, NoOperation())
        assert "LAG(t0.ID, 1)" in sql

    def test_sum_over_partition_and_order_by(self):
        table, name_attr, id_attr = _make_multi_col_table()
        sql = to_sql(
            None, None,
            [id_attr.sum().over(partition_by=[name_attr], order_by=[id_attr.ascending()])],
            table, NoOperation(),
        )
        assert "SUM(" in sql
        assert "OVER (" in sql
        assert "PARTITION BY" in sql
        assert "ORDER BY" in sql


class TestGroupBy:

    def test_no_group_by_produces_no_group_clause(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, NoOperation())
        assert "GROUP BY" not in sql

    def test_group_by_single_column(self):
        table, name_attr, id_attr = _make_multi_col_table()
        sql = to_sql(None, None, [name_attr], table, NoOperation(), group_by=[name_attr])
        assert "GROUP BY" in sql
        assert "NAME" in sql.split("GROUP BY")[1]

    def test_group_by_multiple_columns(self):
        table, name_attr, id_attr = _make_multi_col_table()
        sql = to_sql(None, None, [name_attr, id_attr], table, NoOperation(),
                                          group_by=[name_attr, id_attr])
        group_clause = sql.split("GROUP BY")[1]
        assert "NAME" in group_clause
        assert "ID" in group_clause

    def test_group_by_with_filter(self):
        table, name_attr, id_attr = _make_multi_col_table()
        sql = to_sql(None, None, [name_attr], table, name_attr.eq("Acme"),
                                          group_by=[name_attr])
        assert "WHERE" in sql
        assert "GROUP BY" in sql
        assert sql.index("WHERE") < sql.index("GROUP BY")

    def test_group_by_before_order_by(self):
        table, name_attr, id_attr = _make_multi_col_table()
        sql = to_sql(None, None, [name_attr], table, NoOperation(),
                                          order_by=[name_attr.ascending()], group_by=[name_attr])
        assert "GROUP BY" in sql
        assert "ORDER BY" in sql
        assert sql.index("GROUP BY") < sql.index("ORDER BY")

    def test_finder_result_group_by_chaining(self):
        from datafinder import FinderResult, convert_inputs_and_select
        table, name_attr, id_attr = _make_multi_col_table()
        result = convert_inputs_and_select(None, None, [name_attr], table, NoOperation())
        assert isinstance(result, FinderResult)
        chained = result.group_by(name_attr)
        assert chained is result
        assert len(result._group_by) == 1


class TestLimit:

    def test_no_limit_produces_no_limit_clause(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, NoOperation())
        assert "LIMIT" not in sql

    def test_limit_produces_limit_clause(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, NoOperation(), limit=10)
        assert "LIMIT 10" in sql

    def test_limit_after_order_by(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, NoOperation(),
                                          order_by=[attr.ascending()], limit=5)
        assert "ORDER BY" in sql
        assert "LIMIT 5" in sql
        assert sql.index("ORDER BY") < sql.index("LIMIT")

    def test_limit_after_where(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr], table, attr.eq("Acme"), limit=3)
        assert "WHERE" in sql
        assert "LIMIT 3" in sql
        assert sql.index("WHERE") < sql.index("LIMIT")

    def test_finder_result_limit_chaining(self):
        from datafinder import FinderResult, convert_inputs_and_select
        table, attr = _make_table_and_attr()
        result = convert_inputs_and_select(None, None, [attr], table, NoOperation())
        assert isinstance(result, FinderResult)
        chained = result.limit(10)
        assert chained is result
        assert result._limit == 10


def _make_double_attr():
    col = Column("PRICE", "DOUBLE")
    table = Table("trades", [col])
    attr = DoubleAttribute("Price", "PRICE", "DOUBLE", "trades")
    return table, attr


class TestNumericScalarFunctions:

    def test_abs_produces_abs_function(self):
        table, attr = _make_double_attr()
        sql = to_sql(None, None, [attr.abs()], table, NoOperation())
        assert "ABS(" in sql
        assert "PRICE" in sql

    def test_abs_display_name(self):
        table, attr = _make_double_attr()
        sql = to_sql(None, None, [attr.abs()], table, NoOperation())
        assert '"Abs Price"' in sql

    def test_ceil_produces_ceiling_function(self):
        table, attr = _make_double_attr()
        sql = to_sql(None, None, [attr.ceil()], table, NoOperation())
        assert "CEILING(" in sql

    def test_floor_produces_floor_function(self):
        table, attr = _make_double_attr()
        sql = to_sql(None, None, [attr.floor()], table, NoOperation())
        assert "FLOOR(" in sql

    def test_sqrt_produces_sqrt_function(self):
        table, attr = _make_double_attr()
        sql = to_sql(None, None, [attr.sqrt()], table, NoOperation())
        assert "SQRT(" in sql

    def test_mod_produces_mod_with_second_arg(self):
        table, attr = _make_double_attr()
        sql = to_sql(None, None, [attr.mod(3)], table, NoOperation())
        assert "MOD(" in sql
        assert ", 3" in sql

    def test_mod_operator_produces_mod(self):
        table, attr = _make_double_attr()
        sql = to_sql(None, None, [attr % 3], table, NoOperation())
        assert "MOD(" in sql
        assert ", 3" in sql

    def test_power_produces_power_with_second_arg(self):
        table, attr = _make_double_attr()
        sql = to_sql(None, None, [attr.power(2)], table, NoOperation())
        assert "POWER(" in sql
        assert ", 2" in sql

    def test_pow_operator_produces_power(self):
        table, attr = _make_double_attr()
        sql = to_sql(None, None, [attr ** 2], table, NoOperation())
        assert "POWER(" in sql
        assert ", 2" in sql

    def test_round_without_decimals(self):
        table, attr = _make_double_attr()
        sql = to_sql(None, None, [attr.round()], table, NoOperation())
        assert "ROUND(" in sql
        assert ", " not in sql.split("ROUND(")[1].split(")")[0]

    def test_round_with_decimals(self):
        table, attr = _make_double_attr()
        sql = to_sql(None, None, [attr.round(2)], table, NoOperation())
        assert "ROUND(" in sql
        assert ", 2" in sql

    def test_scalar_function_with_filter(self):
        table, attr = _make_double_attr()
        _, str_attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.abs()], table, NoOperation())
        assert "FROM" in sql
        assert "trades" in sql


def _make_date_attr():
    col = Column("TRADE_DATE", "DATE")
    table = Table("trades", [col])
    attr = DateAttribute("Trade Date", "TRADE_DATE", "DATE", "trades")
    return table, attr


def _make_datetime_attr():
    col = Column("CREATED_AT", "TIMESTAMP")
    table = Table("events", [col])
    attr = DateTimeAttribute("Created At", "CREATED_AT", "TIMESTAMP", "events")
    return table, attr


class TestDateExtract:

    def test_year_produces_extract_year(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr.year()], table, NoOperation())
        assert "EXTRACT(YEAR FROM" in sql

    def test_month_produces_extract_month(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr.month()], table, NoOperation())
        assert "EXTRACT(MONTH FROM" in sql

    def test_day_produces_extract_day(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr.day()], table, NoOperation())
        assert "EXTRACT(DAY FROM" in sql

    def test_quarter_produces_extract_quarter(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr.quarter()], table, NoOperation())
        assert "EXTRACT(QUARTER FROM" in sql

    def test_week_produces_extract_week(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr.week()], table, NoOperation())
        assert "EXTRACT(WEEK FROM" in sql

    def test_day_of_week_produces_extract_dow(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr.day_of_week()], table, NoOperation())
        assert "EXTRACT(DOW FROM" in sql

    def test_hour_on_datetime(self):
        table, attr = _make_datetime_attr()
        sql = to_sql(None, None, [attr.hour()], table, NoOperation())
        assert "EXTRACT(HOUR FROM" in sql

    def test_minute_on_datetime(self):
        table, attr = _make_datetime_attr()
        sql = to_sql(None, None, [attr.minute()], table, NoOperation())
        assert "EXTRACT(MINUTE FROM" in sql

    def test_second_on_datetime(self):
        table, attr = _make_datetime_attr()
        sql = to_sql(None, None, [attr.second()], table, NoOperation())
        assert "EXTRACT(SECOND FROM" in sql

    def test_year_display_name(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr.year()], table, NoOperation())
        assert '"Year Trade Date"' in sql


class TestDateArithmetic:

    def test_add_days_produces_interval(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr.add_days(7)], table, NoOperation())
        assert "+ INTERVAL 7 DAY" in sql

    def test_subtract_days_produces_minus_interval(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr.subtract_days(3)], table, NoOperation())
        assert "- INTERVAL 3 DAY" in sql

    def test_add_months_produces_month_interval(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr.add_months(2)], table, NoOperation())
        assert "+ INTERVAL 2 MONTH" in sql

    def test_add_years_produces_year_interval(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr.add_years(1)], table, NoOperation())
        assert "+ INTERVAL 1 YEAR" in sql

    def test_add_hours_on_datetime(self):
        table, attr = _make_datetime_attr()
        sql = to_sql(None, None, [attr.add_hours(6)], table, NoOperation())
        assert "+ INTERVAL 6 HOUR" in sql

    def test_timedelta_days_operator_on_date(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr + datetime.timedelta(days=7)], table, NoOperation())
        assert "+ INTERVAL 7 DAY" in sql

    def test_timedelta_subtraction_operator_on_date(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr - datetime.timedelta(days=3)], table, NoOperation())
        assert "- INTERVAL 3 DAY" in sql

    def test_timedelta_days_operator_on_datetime(self):
        table, attr = _make_datetime_attr()
        sql = to_sql(None, None, [attr + datetime.timedelta(days=2)], table, NoOperation())
        assert "+ INTERVAL 2 DAY" in sql

    def test_timedelta_hours_operator_on_datetime(self):
        table, attr = _make_datetime_attr()
        sql = to_sql(None, None, [attr + datetime.timedelta(hours=6)], table, NoOperation())
        assert "+ INTERVAL 6 HOUR" in sql

    def test_timedelta_minutes_operator_on_datetime(self):
        table, attr = _make_datetime_attr()
        sql = to_sql(None, None, [attr + datetime.timedelta(minutes=30)], table, NoOperation())
        assert "+ INTERVAL 30 MINUTE" in sql

    def test_timedelta_seconds_operator_on_datetime(self):
        table, attr = _make_datetime_attr()
        sql = to_sql(None, None, [attr + datetime.timedelta(seconds=45)], table, NoOperation())
        assert "+ INTERVAL 45 SECOND" in sql


class TestDateDiff:

    def test_diff_days_produces_date_diff(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr.diff_days(datetime.date(2024, 1, 1))], table, NoOperation())
        assert "DATE_DIFF('day'," in sql

    def test_diff_months_uses_month_part(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr.diff_months(datetime.date(2024, 1, 1))], table, NoOperation())
        assert "DATE_DIFF('month'," in sql

    def test_diff_days_includes_date_literal(self):
        table, attr = _make_date_attr()
        sql = to_sql(None, None, [attr.diff_days(datetime.date(2024, 6, 15))], table, NoOperation())
        assert "2024-06-15" in sql

    def test_diff_datetime_formats_as_timestamp(self):
        table, attr = _make_datetime_attr()
        sql = to_sql(None, None, [attr.diff_hours(datetime.datetime(2024, 1, 1, 12, 0, 0))], table, NoOperation())
        assert "DATE_DIFF('hour'," in sql
        assert "2024-01-01 12:00:00" in sql


class TestStringScalarFunctions:

    def test_upper_produces_upper_function(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.upper()], table, NoOperation())
        assert "UPPER(" in sql

    def test_lower_produces_lower_function(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.lower()], table, NoOperation())
        assert "LOWER(" in sql

    def test_strip_produces_trim_function(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.strip()], table, NoOperation())
        assert "TRIM(" in sql

    def test_lstrip_produces_ltrim_function(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.lstrip()], table, NoOperation())
        assert "LTRIM(" in sql

    def test_rstrip_produces_rtrim_function(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.rstrip()], table, NoOperation())
        assert "RTRIM(" in sql

    def test_length_produces_length_function(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.length()], table, NoOperation())
        assert "LENGTH(" in sql

    def test_reverse_produces_reverse_function(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.reverse()], table, NoOperation())
        assert "REVERSE(" in sql

    def test_left_produces_left_with_n(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.left(3)], table, NoOperation())
        assert "LEFT(" in sql
        assert ", 3" in sql

    def test_right_produces_right_with_n(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.right(5)], table, NoOperation())
        assert "RIGHT(" in sql
        assert ", 5" in sql

    def test_repeat_produces_repeat_with_n(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.repeat(2)], table, NoOperation())
        assert "REPEAT(" in sql
        assert ", 2" in sql

    def test_replace_produces_replace_with_quoted_args(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.replace("Corp", "LLC")], table, NoOperation())
        assert "REPLACE(" in sql
        assert "'Corp'" in sql
        assert "'LLC'" in sql

    def test_substring_without_length(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.substring(2)], table, NoOperation())
        assert "SUBSTRING(" in sql
        assert ", 3" in sql

    def test_substring_with_length(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.substring(2, 4)], table, NoOperation())
        assert "SUBSTRING(" in sql
        assert ", 3, 4" in sql

    def test_upper_display_name(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr.upper()], table, NoOperation())
        assert '"Upper Name"' in sql


class TestStringSlice:

    def test_slice_stop_only_produces_left(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr[:4]], table, NoOperation())
        assert "LEFT(" in sql
        assert ", 4" in sql

    def test_slice_negative_start_produces_right(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr[-4:]], table, NoOperation())
        assert "RIGHT(" in sql
        assert ", 4" in sql

    def test_slice_reverse_produces_reverse(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr[::-1]], table, NoOperation())
        assert "REVERSE(" in sql

    def test_slice_start_only_produces_substring(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr[2:]], table, NoOperation())
        assert "SUBSTRING(" in sql
        assert ", 3" in sql

    def test_slice_start_and_stop_produces_substring(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr[2:6]], table, NoOperation())
        assert "SUBSTRING(" in sql
        assert ", 3, 4" in sql

    def test_mul_produces_repeat(self):
        table, attr = _make_table_and_attr()
        sql = to_sql(None, None, [attr * 3], table, NoOperation())
        assert "REPEAT(" in sql
        assert ", 3" in sql


class TestNullEndMilestoning:

    def _make_milestoned_table(self, infinite_datetime=None):
        from model.milestoning import ProcessingTemporalColumns, MilestonedTable
        start_col = Column("in_z", "TIMESTAMP")
        end_col = Column("out_z", "TIMESTAMP")
        mc = ProcessingTemporalColumns(start_col, end_col, infinite_datetime=infinite_datetime)
        table = MilestonedTable("trades", [], mc)
        return table

    def test_finite_infinite_datetime_produces_no_is_null(self):
        table = self._make_milestoned_table(infinite_datetime="9999-12-31 23:59:59")
        dt = datetime.datetime(2024, 6, 1, 12, 0, 0)
        from datafinder.sql_generator import build_milestoning_filter_operation
        op = build_milestoning_filter_operation(None, dt, table)
        gen = SQLQueryGenerator()
        sql = gen.build_filter(op)
        assert "IS NULL" not in sql
        assert "out_z" in sql

    def test_null_infinite_datetime_produces_is_null_clause(self):
        table = self._make_milestoned_table(infinite_datetime=None)
        dt = datetime.datetime(2024, 6, 1, 12, 0, 0)
        from datafinder.sql_generator import build_milestoning_filter_operation
        op = build_milestoning_filter_operation(None, dt, table)
        gen = SQLQueryGenerator()
        sql = gen.build_filter(op)
        assert "IS NULL" in sql
        assert "out_z" in sql

    def test_null_infinite_datetime_uses_or_between_gt_and_is_null(self):
        table = self._make_milestoned_table(infinite_datetime=None)
        dt = datetime.datetime(2024, 6, 1, 12, 0, 0)
        from datafinder.sql_generator import build_milestoning_filter_operation
        op = build_milestoning_filter_operation(None, dt, table)
        gen = SQLQueryGenerator()
        sql = gen.build_filter(op)
        assert " OR " in sql
        gt_pos = sql.index("out_z >")
        or_pos = sql.index(" OR ")
        null_pos = sql.index("IS NULL")
        assert gt_pos < or_pos < null_pos

    def test_to_sql_with_null_end_contains_is_null(self):
        table = self._make_milestoned_table(infinite_datetime=None)
        from datafinder.typed_attributes import DateTimeAttribute as DTA
        attr = DTA("start_at", "in_z", "TIMESTAMP", "trades")
        dt = datetime.datetime(2024, 6, 1, 12, 0, 0)
        sql = to_sql(None, dt, [attr], table, NoOperation())
        assert "IS NULL" in sql


class TestMilestoningOnJoinedTable:
    """Verify that milestoning is applied as a JOIN ON condition, not a WHERE clause,
    when the milestoned table is reached via a join rather than being the root table."""

    def _make_account_to_trade_reverse_join(self):
        from model.milestoning import ProcessingTemporalColumns, MilestonedTable

        # Non-milestoned root: Account
        account_id_col = Column("ID", "INT", "account_master")
        account_table = Table("account_master", [account_id_col])

        # Milestoned join target: Trade
        in_z = Column("in_z", "TIMESTAMP", "trades")
        out_z = Column("out_z", "TIMESTAMP", "trades")
        sym_col = Column("sym", "VARCHAR", "trades")
        fk_col = Column("account_id", "INT", "trades")
        mc = ProcessingTemporalColumns(in_z, out_z)
        trade_table = MilestonedTable("trades", [sym_col, fk_col, in_z, out_z], mc)

        # Reverse join: account_master.ID → trades.account_id
        join = JoinOperation("Trade", trade_table, account_id_col, fk_col)
        node = JoinTreeNodeOperation(join)
        sym_attr = StringAttribute("Trade Symbol", "sym", "VARCHAR", "trades", node)
        return account_table, sym_attr

    def test_milestoning_appears_in_join_on_clause_not_where(self):
        """When the joined table is milestoned and processing_datetime is provided,
        the milestoning filter must be in the JOIN ON clause, not the WHERE clause."""
        account_table, sym_attr = self._make_account_to_trade_reverse_join()
        dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
        sql = to_sql(None, dt, [sym_attr], account_table, NoOperation())

        assert "LEFT OUTER JOIN" in sql
        join_part = sql[sql.index("LEFT OUTER JOIN"):]
        assert "in_z" in join_part
        assert "out_z" in join_part
        assert "WHERE" not in sql

    def test_no_milestoning_on_join_when_no_processing_datetime(self):
        """When processing_datetime is None, no milestoning filter is added to the join."""
        account_table, sym_attr = self._make_account_to_trade_reverse_join()
        sql = to_sql(None, None, [sym_attr], account_table, NoOperation())

        assert "LEFT OUTER JOIN" in sql
        assert "in_z" not in sql
        assert "out_z" not in sql
        assert "WHERE" not in sql

    def test_milestoning_join_filter_uses_correct_processing_datetime(self):
        """The processing_datetime value is embedded in the JOIN ON milestoning filter."""
        account_table, sym_attr = self._make_account_to_trade_reverse_join()
        dt = datetime.datetime(2023, 6, 15, 9, 0, 0)
        sql = to_sql(None, dt, [sym_attr], account_table, NoOperation())

        assert "'2023-06-15 09:00:00'" in sql

    def test_root_milestoning_goes_to_where_joined_milestoning_goes_to_on(self):
        """When BOTH root and joined tables are milestoned, root milestoning is in WHERE
        and join milestoning is in the ON clause."""
        from model.milestoning import ProcessingTemporalColumns, MilestonedTable

        # Milestoned root: Trade
        in_z_root = Column("in_z", "TIMESTAMP", "trades")
        out_z_root = Column("out_z", "TIMESTAMP", "trades")
        fk_col = Column("account_id", "INT", "trades")
        sym_col = Column("sym", "VARCHAR", "trades")
        mc_root = ProcessingTemporalColumns(in_z_root, out_z_root)
        trade_table = MilestonedTable("trades", [sym_col, fk_col, in_z_root, out_z_root], mc_root)
        sym_attr = StringAttribute("Symbol", "sym", "VARCHAR", "trades")

        # Milestoned join target: Price
        in_z_price = Column("in_z", "TIMESTAMP", "price")
        out_z_price = Column("out_z", "TIMESTAMP", "price")
        price_col = Column("PRICE", "DOUBLE", "price")
        mc_price = ProcessingTemporalColumns(in_z_price, out_z_price)
        price_table = MilestonedTable("price", [price_col, in_z_price, out_z_price], mc_price)
        sym_fk = Column("sym", "VARCHAR", "trades")
        sym_pk = Column("SYM", "VARCHAR", "price")
        join = JoinOperation("Instrument", price_table, sym_fk, sym_pk)
        price_attr = StringAttribute("Price", "PRICE", "DOUBLE", "price", JoinTreeNodeOperation(join))

        dt = datetime.datetime(2023, 1, 1, 12, 0, 0)
        sql = to_sql(None, dt, [sym_attr, price_attr], trade_table, NoOperation())

        where_pos = sql.index("WHERE")
        join_pos = sql.index("LEFT OUTER JOIN")
        # Root milestoning lands in WHERE
        where_clause = sql[where_pos:]
        assert "in_z" in where_clause
        # Join milestoning lands before WHERE (in the ON clause)
        on_clause = sql[join_pos:where_pos]
        assert "in_z" in on_clause


class TestJoinOrdering:
    """Joins must appear in a deterministic, insertion-order sequence."""

    def test_two_joins_order_is_stable(self):
        """Selecting columns from two different joined tables must always produce
        the joins in the same order regardless of Python set iteration."""
        account_id_col = Column("account_id", "INT", "trades")
        sym_col = Column("sym", "VARCHAR", "trades")
        trade_table = Table("trades", [account_id_col, sym_col])

        acct_id = Column("ID", "INT", "accounts")
        acct_table = Table("accounts", [acct_id])
        acct_join = JoinOperation("account", acct_table, account_id_col, acct_id)
        acct_node = JoinTreeNodeOperation(acct_join)

        inst_sym = Column("SYM", "VARCHAR", "instruments")
        inst_table = Table("instruments", [inst_sym])
        inst_join = JoinOperation("instrument", inst_table, sym_col, inst_sym)
        inst_node = JoinTreeNodeOperation(inst_join)

        acct_name = StringAttribute("Account Id", "ID", "INT", "accounts", acct_node)
        inst_sym_attr = StringAttribute("Symbol", "SYM", "VARCHAR", "instruments", inst_node)

        sqls = [
            to_sql(None, None, [acct_name, inst_sym_attr], trade_table, NoOperation()),
            to_sql(None, None, [acct_name, inst_sym_attr], trade_table, NoOperation()),
            to_sql(None, None, [acct_name, inst_sym_attr], trade_table, NoOperation()),
        ]
        assert sqls[0] == sqls[1] == sqls[2]
        assert sqls[0].index("accounts") < sqls[0].index("instruments")


class TestMultiHopJoins:
    """When traversing A→B→C, the B join must be emitted before the C join
    even when no columns from B are selected."""

    def _make_two_hop_setup(self):
        # Root: org table
        org_id = Column("id", "INT", "org")
        org_table = Table("org", [org_id])

        # Intermediate: employees table
        emp_id = Column("id", "INT", "employees")
        emp_org_fk = Column("org_id", "INT", "employees")
        emp_mgr_fk = Column("manager_id", "INT", "employees")
        emp_name = Column("name", "VARCHAR", "employees")
        emp_table = Table("employees", [emp_id, emp_org_fk, emp_mgr_fk, emp_name])

        # Leaf: trades table (joined via employees.id → trades.employee_id)
        trade_sym = Column("sym", "VARCHAR", "trades")
        trade_emp_fk = Column("employee_id", "INT", "trades")
        trade_table = Table("trades", [trade_sym, trade_emp_fk])

        # Hop 1: org → employees
        emp_join = JoinOperation("employees", emp_table, org_id, emp_org_fk)
        emp_node = JoinTreeNodeOperation(emp_join)

        # Hop 2: employees → trades (parent = emp_node)
        trade_join = JoinOperation("trades", trade_table, emp_id, trade_emp_fk)
        trade_node = JoinTreeNodeOperation(trade_join, parent=emp_node)

        trade_sym_attr = StringAttribute("Symbol", "sym", "VARCHAR", "trades", trade_node)
        return org_table, emp_node, trade_node, trade_sym_attr

    def test_ancestor_join_emitted_when_no_intermediate_column_selected(self):
        """Selecting only a leaf-table column must still emit the intermediate join."""
        org_table, emp_node, trade_node, trade_sym_attr = self._make_two_hop_setup()
        sql = to_sql(None, None, [trade_sym_attr], org_table, NoOperation())

        assert sql.count("LEFT OUTER JOIN") == 2

    def test_ancestor_join_appears_before_leaf_join(self):
        """The intermediate (employees) join must precede the leaf (trades) join."""
        org_table, emp_node, trade_node, trade_sym_attr = self._make_two_hop_setup()
        sql = to_sql(None, None, [trade_sym_attr], org_table, NoOperation())

        emp_pos = sql.index("employees")
        trades_pos = sql.index("trades", emp_pos + 1)
        assert emp_pos < trades_pos

    def test_three_join_chain_correct_order(self):
        """A→B→C→D produces joins in B, C, D order."""
        a_id = Column("id", "INT", "a")
        a_table = Table("a", [a_id])

        b_id = Column("id", "INT", "b")
        b_fk = Column("a_id", "INT", "b")
        b_table = Table("b", [b_id, b_fk])

        c_id = Column("id", "INT", "c")
        c_fk = Column("b_id", "INT", "c")
        c_table = Table("c", [c_id, c_fk])

        d_val = Column("val", "VARCHAR", "d")
        d_fk = Column("c_id", "INT", "d")
        d_table = Table("d", [d_val, d_fk])

        b_join = JoinOperation("b", b_table, a_id, b_fk)
        b_node = JoinTreeNodeOperation(b_join)
        c_join = JoinOperation("c", c_table, b_id, c_fk)
        c_node = JoinTreeNodeOperation(c_join, parent=b_node)
        d_join = JoinOperation("d", d_table, c_id, d_fk)
        d_node = JoinTreeNodeOperation(d_join, parent=c_node)

        d_attr = StringAttribute("Val", "val", "VARCHAR", "d", d_node)
        sql = to_sql(None, None, [d_attr], a_table, NoOperation())

        assert sql.count("LEFT OUTER JOIN") == 3
        b_pos = sql.index(" b ")
        c_pos = sql.index(" c ")
        d_pos = sql.index(" d ")
        assert b_pos < c_pos < d_pos


class TestIsNoneIsNotNone:

    def _make_table_and_string_attr(self):
        col = Column("NOTES", "VARCHAR")
        table = Table("orders", [col])
        attr = StringAttribute("Notes", "NOTES", "VARCHAR", "orders")
        return table, attr

    def _make_table_and_int_attr(self):
        col = Column("QUANTITY", "INT")
        table = Table("orders", [col])
        attr = IntegerAttribute("Quantity", "QUANTITY", "INT", "orders")
        return table, attr

    def test_is_none_string_produces_is_null(self):
        table, attr = self._make_table_and_string_attr()
        sql = to_sql(None, None, [attr], table, attr.is_none())
        assert "IS NULL" in sql
        assert "IS NOT NULL" not in sql

    def test_is_not_none_string_produces_is_not_null(self):
        table, attr = self._make_table_and_string_attr()
        sql = to_sql(None, None, [attr], table, attr.is_not_none())
        assert "IS NOT NULL" in sql

    def test_is_none_integer_produces_is_null(self):
        table, attr = self._make_table_and_int_attr()
        sql = to_sql(None, None, [attr], table, attr.is_none())
        assert "IS NULL" in sql
        assert "IS NOT NULL" not in sql

    def test_is_not_none_integer_produces_is_not_null(self):
        table, attr = self._make_table_and_int_attr()
        sql = to_sql(None, None, [attr], table, attr.is_not_none())
        assert "IS NOT NULL" in sql

    def test_is_none_combined_with_other_filter(self):
        table, attr = self._make_table_and_string_attr()
        qty_col = Column("QUANTITY", "INT")
        table2 = Table("orders", [qty_col, Column("NOTES", "VARCHAR")])
        qty_attr = IntegerAttribute("Quantity", "QUANTITY", "INT", "orders")
        combined = attr.is_none().and_op(qty_attr > 0)
        sql = to_sql(None, None, [attr], table2, combined)
        assert "IS NULL" in sql
        assert ">" in sql

    def test_is_not_none_combined_with_other_filter(self):
        table, attr = self._make_table_and_string_attr()
        qty_attr = IntegerAttribute("Quantity", "QUANTITY", "INT", "orders")
        combined = attr.is_not_none().and_op(qty_attr > 0)
        qty_col = Column("QUANTITY", "INT")
        table2 = Table("orders", [qty_col, Column("NOTES", "VARCHAR")])
        sql = to_sql(None, None, [attr], table2, combined)
        assert "IS NOT NULL" in sql
        assert ">" in sql


class TestExistsNotExists:

    def _make_trade_with_account_join(self):
        from datafinder.finder import RelatedFinder
        trade_table = Table("trading.trades", [Column("sym", "VARCHAR", "trading.trades"),
                                               Column("account_id", "INT", "trading.trades")])
        account_table = Table("ref_data.account_master", [])
        account_join = JoinOperation(
            "Account", account_table,
            Column("account_id", "INT", "trading.trades"),
            Column("ID", "INT", "ref_data.account_master"),
        )
        account_node = JoinTreeNodeOperation(account_join)
        account_finder = RelatedFinder(account_node)
        sym_attr = StringAttribute("Symbol", "sym", "VARCHAR", "trading.trades")
        return trade_table, sym_attr, account_finder, account_node

    def test_exists_generates_is_not_null_on_join_key(self):
        trade_table, sym_attr, account_finder, _ = self._make_trade_with_account_join()
        sql = to_sql(None, None, [sym_attr], trade_table, account_finder.exists())
        assert "IS NOT NULL" in sql

    def test_not_exists_generates_is_null_on_join_key(self):
        trade_table, sym_attr, account_finder, _ = self._make_trade_with_account_join()
        sql = to_sql(None, None, [sym_attr], trade_table, account_finder.not_exists())
        assert "IS NULL" in sql
        assert "IS NOT NULL" not in sql

    def test_exists_references_join_target_column(self):
        trade_table, sym_attr, account_finder, _ = self._make_trade_with_account_join()
        sql = to_sql(None, None, [sym_attr], trade_table, account_finder.exists())
        assert "ID" in sql

    def test_exists_includes_left_outer_join(self):
        trade_table, sym_attr, account_finder, _ = self._make_trade_with_account_join()
        sql = to_sql(None, None, [sym_attr], trade_table, account_finder.exists())
        assert "LEFT OUTER JOIN" in sql

    def test_not_exists_can_be_combined_with_other_filter(self):
        trade_table, sym_attr, account_finder, _ = self._make_trade_with_account_join()
        combined = account_finder.not_exists().and_op(sym_attr.eq("AAPL"))
        sql = to_sql(None, None, [sym_attr], trade_table, combined)
        assert "IS NULL" in sql
        assert "AAPL" in sql


class TestFindForDateRange:

    def _make_bitemporal_table(self):
        from model.milestoning import BiTemporalColumns, MilestonedTable
        biz_from = Column("biz_from", "DATE")
        biz_to = Column("biz_to", "DATE")
        in_z = Column("in_z", "TIMESTAMP")
        out_z = Column("out_z", "TIMESTAMP")
        mc = BiTemporalColumns(biz_from, biz_to, in_z, out_z, infinite_datetime="9999-12-31 23:59:59")
        return MilestonedTable("trades", [], mc)

    def _make_single_biz_date_table(self):
        from model.milestoning import SingleBusinessDateColumn, MilestonedTable
        biz_date = Column("biz_date", "DATE")
        mc = SingleBusinessDateColumn(biz_date)
        return MilestonedTable("positions", [], mc)

    def _make_biz_date_and_processing_table(self):
        from model.milestoning import BusinessDateAndProcessingTemporalColumns, MilestonedTable
        biz_date = Column("biz_date", "DATE")
        in_z = Column("in_z", "TIMESTAMP")
        out_z = Column("out_z", "TIMESTAMP")
        mc = BusinessDateAndProcessingTemporalColumns(biz_date, in_z, out_z, infinite_datetime="9999-12-31 23:59:59")
        return MilestonedTable("positions", [], mc)

    def test_bitemporal_range_uses_overlap_semantics(self):
        """business_date_from_col <= range_end AND business_date_to_col > range_start"""
        from datafinder.sql_generator import build_milestoning_filter_operation_for_date_range
        table = self._make_bitemporal_table()
        date_from = datetime.date(2024, 1, 1)
        date_to = datetime.date(2024, 12, 31)
        op = build_milestoning_filter_operation_for_date_range(date_from, date_to, None, table)
        gen = SQLQueryGenerator()
        sql = gen.build_filter(op)
        assert "biz_from" in sql
        assert "biz_to" in sql
        assert "'2024-12-31'" in sql
        assert "'2024-01-01'" in sql

    def test_bitemporal_range_overlap_order(self):
        """biz_from <= range_end comes before biz_to > range_start"""
        from datafinder.sql_generator import build_milestoning_filter_operation_for_date_range
        table = self._make_bitemporal_table()
        op = build_milestoning_filter_operation_for_date_range(
            datetime.date(2024, 1, 1), datetime.date(2024, 12, 31), None, table)
        gen = SQLQueryGenerator()
        sql = gen.build_filter(op)
        from_pos = sql.index("biz_from")
        to_pos = sql.index("biz_to")
        assert from_pos < to_pos

    def test_bitemporal_range_includes_processing_filter(self):
        table = self._make_bitemporal_table()
        pdt = datetime.datetime(2025, 6, 1, 0, 0, 0)
        from datafinder.sql_generator import build_milestoning_filter_operation_for_date_range
        op = build_milestoning_filter_operation_for_date_range(
            datetime.date(2024, 1, 1), datetime.date(2024, 12, 31), pdt, table)
        gen = SQLQueryGenerator()
        sql = gen.build_filter(op)
        assert "in_z" in sql
        assert "out_z" in sql

    def test_single_biz_date_range_uses_gte_lte(self):
        """business_date >= range_start AND business_date <= range_end"""
        from datafinder.sql_generator import build_milestoning_filter_operation_for_date_range
        table = self._make_single_biz_date_table()
        op = build_milestoning_filter_operation_for_date_range(
            datetime.date(2024, 1, 1), datetime.date(2024, 12, 31), None, table)
        gen = SQLQueryGenerator()
        sql = gen.build_filter(op)
        assert ">=" in sql
        assert "<=" in sql
        assert "'2024-01-01'" in sql
        assert "'2024-12-31'" in sql

    def test_biz_date_and_processing_range_filters_both(self):
        table = self._make_biz_date_and_processing_table()
        pdt = datetime.datetime(2025, 6, 1, 0, 0, 0)
        from datafinder.sql_generator import build_milestoning_filter_operation_for_date_range
        op = build_milestoning_filter_operation_for_date_range(
            datetime.date(2024, 1, 1), datetime.date(2024, 12, 31), pdt, table)
        gen = SQLQueryGenerator()
        sql = gen.build_filter(op)
        assert "biz_date" in sql
        assert "in_z" in sql

    def test_to_sql_with_business_date_to_generates_range_sql(self):
        table = self._make_bitemporal_table()
        attr = DateAttribute("Biz From", "biz_from", "DATE", "trades")
        pdt = datetime.datetime(2025, 6, 1, 0, 0, 0)
        sql = to_sql(
            datetime.date(2024, 1, 1), pdt, [attr], table, NoOperation(),
            business_date_to=datetime.date(2024, 12, 31),
        )
        assert "'2024-12-31'" in sql
        assert "'2024-01-01'" in sql
        assert "in_z" in sql

    def test_to_sql_without_business_date_to_unchanged(self):
        """Passing business_date_to=None must not change existing point-in-time behaviour."""
        table = self._make_bitemporal_table()
        attr = DateAttribute("Biz From", "biz_from", "DATE", "trades")
        pdt = datetime.datetime(2025, 6, 1, 0, 0, 0)
        bd = datetime.date(2024, 6, 15)
        sql_point = to_sql(bd, pdt, [attr], table, NoOperation())
        sql_none = to_sql(bd, pdt, [attr], table, NoOperation(), business_date_to=None)
        assert sql_point == sql_none
