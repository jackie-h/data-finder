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
        return np.array(
            [[_extract_value(row, f) for f in self._field_names] for row in self._rows],
            dtype='object',
        )

    def to_pandas(self) -> pd.DataFrame:
        if not self._rows:
            return pd.DataFrame(columns=self._field_names)
        return pd.DataFrame(
            [[_extract_value(row, f) for f in self._field_names] for row in self._rows],
            columns=self._field_names,
        )


def _extract_value(row: dict, field_name: str):
    """Extract a value from a row, supporting dot-separated nested paths."""
    if "." in field_name:
        parent, child = field_name.split(".", 1)
        return _extract_value(row.get(parent) or {}, child)
    return row.get(field_name)


def _build_fields_str(columns: list[Attribute]) -> str:
    """Build the GraphQL field selection string, grouping nested dot-path fields.

    e.g. columns with names ["symbol", "account.name", "account.id"] produce
    "symbol account { name id }"
    """
    nested: dict[str, list[str]] = {}
    flat: list[str] = []
    for col in columns:
        name = col.column().name
        if "." in name:
            parent, child = name.split(".", 1)
            nested.setdefault(parent, []).append(child)
        else:
            flat.append(name)
    parts = list(flat)
    for parent, children in nested.items():
        parts.append(f"{parent} {{ {' '.join(children)} }}")
    return " ".join(parts)


def _build_temporal_args(business_date: datetime.date,
                         processing_datetime: datetime.datetime,
                         milestone) -> list[str]:
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
               order_by: list = None, group_by: list = None, limit: int = None,
               timeout_ms: int = 60_000, business_date_to: datetime.date = None) -> DataFrame:
        field_names = [col.column().name for col in columns]
        fields_str = _build_fields_str(columns)

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
        try:
            with urllib.request.urlopen(req, timeout=timeout_ms / 1000) as response:
                result = json.loads(response.read())
        except TimeoutError:
            raise TimeoutError(f"Query exceeded {timeout_ms}ms timeout")

        rows = result["data"][table.name]
        return GraphQLOutput(rows, field_names)
