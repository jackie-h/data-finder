import datetime

from datafinder import StringAttribute, to_sql
from datafinder import IntegerAttribute
from datafinder.sql_generator import SQLQueryGenerator
from datafinder.typed_attributes import DoubleAttribute, DateAttribute, DateTimeAttribute
from model.relational import Table, Column, NoOperation, CountAllOperation


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
