from typing import Sequence
from model.relational import Column, SAME_INFINITY, Table, _SameInfinity


class MilestoningColumns:
    def __init__(self):
        pass

    def columns(self) -> list[Column]:
        raise NotImplementedError()

class ProcessingTemporalColumns(MilestoningColumns):
    def __init__(self, start_at_column: Column, end_at_column: Column, infinite_datetime: str | None = None):
        super().__init__()
        self.start_at_column = start_at_column
        self.end_at_column = end_at_column
        self.infinite_datetime = infinite_datetime

    def columns(self) -> list[Column]:
        return [self.start_at_column, self.end_at_column]

class SingleBusinessDateColumn(MilestoningColumns):
    def __init__(self, business_date_column: Column):
        MilestoningColumns.__init__(self)
        self.business_date_column = business_date_column

    def columns(self) -> list[Column]:
        return [self.business_date_column]


class SingleProcessingDatetimeColumn(MilestoningColumns):
    """A processing snapshot: one datetime column holding the processing time a row was taken at, which a query's
    processing time must equal."""
    def __init__(self, processing_datetime_column: Column):
        MilestoningColumns.__init__(self)
        self.processing_datetime_column = processing_datetime_column

    def columns(self) -> list[Column]:
        return [self.processing_datetime_column]


class BusinessDateAndProcessingTemporalColumns(SingleBusinessDateColumn, ProcessingTemporalColumns):
    def __init__(self, business_date_column: Column, start_at_column: Column, end_at_column: Column,
                 infinite_datetime: str | None = None):
        SingleBusinessDateColumn.__init__(self, business_date_column)
        ProcessingTemporalColumns.__init__(self, start_at_column, end_at_column, infinite_datetime)

    def columns(self) -> list[Column]:
        return SingleBusinessDateColumn.columns(self) + ProcessingTemporalColumns.columns(self)


class BusinessDateRangeColumns(MilestoningColumns):
    """A business date range: a row applies from ``business_date_from`` up to, but not including, ``business_date_to``."""
    def __init__(self, business_date_from_column: Column, business_date_to_column: Column,
                 infinite_datetime: str | None = None):
        MilestoningColumns.__init__(self)
        self.business_date_from_column = business_date_from_column
        self.business_date_to_column = business_date_to_column
        self.infinite_datetime = infinite_datetime
        self.business_infinite_datetime = infinite_datetime  # a bitemporal table can end its business axis another way

    def columns(self) -> list[Column]:
        return [self.business_date_from_column, self.business_date_to_column]


class BiTemporalColumns(BusinessDateRangeColumns, ProcessingTemporalColumns):
    def __init__(self, business_date_from_column: Column, business_date_to_column: Column,
                 start_at_column: Column, end_at_column: Column, infinite_datetime: str | None = None,
                 business_infinite_datetime: 'str | None | _SameInfinity' = SAME_INFINITY):
        """``infinite_datetime`` is what the end of a row that has not ended holds on the processing axis, and
        ``business_infinite_datetime`` on the business axis, the same unless it is given. A Legend table can end one
        axis in a datetime and the other in a date, or one in an infinity and the other in NULL (``None``)."""
        BusinessDateRangeColumns.__init__(self, business_date_from_column, business_date_to_column, infinite_datetime)
        ProcessingTemporalColumns.__init__(self, start_at_column, end_at_column, infinite_datetime)
        if not isinstance(business_infinite_datetime, _SameInfinity):
            self.business_infinite_datetime = business_infinite_datetime

    def columns(self) -> list[Column]:
        return BusinessDateRangeColumns.columns(self) + ProcessingTemporalColumns.columns(self)

class MilestonedTable(Table):
    def __init__(self, name: str, columns: 'Sequence[Column | tuple[str, str]]', milestoning_columns: MilestoningColumns,
                 primary_key: 'Sequence[str | Column]' = ()):
        super().__init__(name, columns, primary_key=primary_key)
        self.milestoning_columns = milestoning_columns
        for col in milestoning_columns.columns():
            col.table = self
            col.owner = self
