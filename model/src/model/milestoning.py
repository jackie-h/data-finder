from model.relational import Column, Table


class MilestoningColumns:
    def __init__(self):
        pass

    def columns(self) -> [Column]:
        pass

class ProcessingTemporalColumns(MilestoningColumns):
    def __init__(self, start_at_column: Column, end_at_column: Column):
        super().__init__()
        self.start_at_column = start_at_column
        self.end_at_column = end_at_column

    def columns(self) -> [Column]:
        return [self.start_at_column, self.end_at_column]

class SingleBusinessDateColumn(MilestoningColumns):
    def __init__(self, business_date_column: Column):
        super().__init__()
        self.business_date_column = business_date_column

    def columns(self) -> [Column]:
        return [self.business_date_column]

class MilestonedTable(Table):
    def __init__(self, name: str, columns: list[Column], milestoning_columns: MilestoningColumns):
        super().__init__(name, columns)
        self.milestoning_columns = milestoning_columns
        for col in milestoning_columns.columns():
            col.table = self