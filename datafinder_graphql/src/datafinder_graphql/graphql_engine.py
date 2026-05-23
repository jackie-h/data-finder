import datetime
import json
import urllib.request

import numpy as np
import pandas as pd

from datafinder import Attribute, Operation, DataFrame, QueryRunnerBase
from model.graphql_mapping import (
    GraphQLQuery,
    GraphQLProcessingMilestone,
    GraphQLBusinessDateMilestone,
    GraphQLBiTemporalMilestone,
)


class GraphQLOutput(DataFrame):

    def __init__(self, rows: list[dict], field_names: list[str]):
        self._rows = rows
        self._field_names = field_names

    def to_numpy(self) -> np.ndarray:
        if not self._rows:
            return np.array([], dtype='object')
        return np.array([[row[f] for f in self._field_names] for row in self._rows], dtype='object')

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame(self._rows, columns=self._field_names)


def _build_temporal_args(business_date: datetime.date,
                         processing_datetime: datetime.datetime,
                         milestone) -> list[str]:
    """Translate temporal parameters into GraphQL argument strings based on milestone config."""
    if milestone is None:
        return []
    args = []
    if isinstance(milestone, GraphQLBiTemporalMilestone):
        if business_date is not None:
            args.append(f'{milestone.business_date_argument}: "{business_date}"')
        if processing_datetime is not None:
            args.append(f'{milestone.processing_argument}: "{processing_datetime.isoformat()}"')
    elif isinstance(milestone, GraphQLBusinessDateMilestone):
        if business_date is not None:
            args.append(f'{milestone.argument_name}: "{business_date}"')
    elif isinstance(milestone, GraphQLProcessingMilestone):
        if processing_datetime is not None:
            args.append(f'{milestone.argument_name}: "{processing_datetime.isoformat()}"')
    return args


class GraphQLConnect(QueryRunnerBase):

    @staticmethod
    def select(business_date: datetime.date, processing_datetime: datetime.datetime,
               columns: list[Attribute], table: GraphQLQuery, op: Operation,
               order_by: list = None, group_by: list = None, limit: int = None) -> DataFrame:
        field_names = [col.column().name for col in columns]
        fields_str = " ".join(field_names)

        arg_parts = _build_temporal_args(business_date, processing_datetime,
                                         getattr(table, 'milestone', None))
        args = f"({', '.join(arg_parts)})" if arg_parts else ""

        query_str = f"{{ {table.name}{args} {{ {fields_str} }} }}"

        payload = json.dumps({"query": query_str}).encode()
        req = urllib.request.Request(
            table.endpoint.url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read())

        rows = result["data"][table.name]
        return GraphQLOutput(rows, field_names)
