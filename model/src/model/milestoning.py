import datetime
from dataclasses import dataclass
from typing import Sequence
from model.relational import Column, Operation, Relation, Table, TableAlias


@dataclass(frozen=True)
class AsOf:
    """The dates a query reads its rows as of, and what they add to it. A date is a point in time, or the start of a range when it has an
    end (``business_date_to``, ``processing_datetime_to``). ``filter`` is the predicate that the dates and the milestoning of the table
    add to what was asked of the rows; it is none for a table that is not milestoned. The joins a query needs read their tables as of the
    same dates, unless a step of a navigation was given others, so they are kept for working those out again."""
    business_date: datetime.date | None = None
    business_date_to: datetime.date | None = None
    processing_datetime: datetime.datetime | datetime.date | None = None
    processing_datetime_to: datetime.datetime | datetime.date | None = None
    filter: Operation | None = None



def milestoning_columns_of(relation: Relation | None) -> 'MilestoningColumns | None':
    """The columns that milestone a relation's rows: a milestoned table's, or those of a query that a class is mapped to with a scheme;
    none for a relation that is not milestoned."""
    if isinstance(relation, MilestonedTable):
        return relation.milestoning_columns
    if isinstance(relation, TableAlias):
        return relation.milestoning_columns
    return None


class MilestoningColumns:
    def __init__(self):
        pass

    def columns(self) -> list[Column]:
        raise NotImplementedError()

class ProcessingTemporalColumns(MilestoningColumns):
    """A processing range. ``processing_infinite_datetime`` is what its end holds in a row that has not ended, or ``None``
    when that end is NULL."""
    def __init__(self, start_at_column: Column, end_at_column: Column, processing_infinite_datetime: str | None = None):
        super().__init__()
        self.start_at_column = start_at_column
        self.end_at_column = end_at_column
        self.processing_infinite_datetime = processing_infinite_datetime

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
                 processing_infinite_datetime: str | None = None):
        SingleBusinessDateColumn.__init__(self, business_date_column)
        ProcessingTemporalColumns.__init__(self, start_at_column, end_at_column, processing_infinite_datetime)

    def columns(self) -> list[Column]:
        return SingleBusinessDateColumn.columns(self) + ProcessingTemporalColumns.columns(self)


class BusinessDateRangeColumns(MilestoningColumns):
    """A business date range: a row applies from ``business_date_from`` up to, but not including, ``business_date_to``.
    ``business_infinite_datetime`` is what the end holds in a row that has not ended, or ``None`` when that end is NULL."""
    def __init__(self, business_date_from_column: Column, business_date_to_column: Column,
                 business_infinite_datetime: str | None = None):
        MilestoningColumns.__init__(self)
        self.business_date_from_column = business_date_from_column
        self.business_date_to_column = business_date_to_column
        self.business_infinite_datetime = business_infinite_datetime

    def columns(self) -> list[Column]:
        return [self.business_date_from_column, self.business_date_to_column]


class BiTemporalColumns(BusinessDateRangeColumns, ProcessingTemporalColumns):
    def __init__(self, business_date_from_column: Column, business_date_to_column: Column,
                 start_at_column: Column, end_at_column: Column, processing_infinite_datetime: str | None = None,
                 business_infinite_datetime: str | None = None):
        """Each axis has its own infinity: a table can end one in a datetime and the other in a date, or one in
        an infinity and the other in NULL (``None``)."""
        BusinessDateRangeColumns.__init__(self, business_date_from_column, business_date_to_column,
                                          business_infinite_datetime)
        ProcessingTemporalColumns.__init__(self, start_at_column, end_at_column, processing_infinite_datetime)

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
