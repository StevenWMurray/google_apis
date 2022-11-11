from __future__ import annotations
import yaml
import re
from datetime import date
from typing import (
    TypedDict,
    Optional,
    TYPE_CHECKING,
    Callable,
    Any,
    TypeVar,
    ParamSpec,
    Literal,
)
from itertools import chain

from attrs import frozen, field, define, fields
from attrs import validators as validators

from uar_types import (
    DateRangeJson,
    SamplingLevel,
    ColumnType,
    FilterOperator,
    VersionedParser,
    FilterType,
    Expression,
)

if TYPE_CHECKING:
    from attrs import Attribute
    from collections.abc import Sized


P = ParamSpec("P")
T = TypeVar("T")


def camel_to_snake_case(instr: str) -> str:
    """Convert string from camelCase to snake_case"""

    def repl(x: re.Match) -> str:
        return f"_{x.group(1).lower()}"

    return re.sub(r"([A-Z])", repl, instr)


def snake_to_camel_case(instr: str) -> str:
    def repl(x: re.Match) -> str:
        return f"{x.group(1).upper()}"

    return re.sub(r"_([a-z])", repl, instr)


def len_between(min_len: int, max_len: int):
    def check(self, attribute: Attribute, value: Optional[Sized]) -> None:
        err_str = f"Length must be between {min_len} and {max_len}"
        if value is not None:
            if len(value) < min_len or len(value) > max_len:
                raise ValueError(err_str)

    return check


@frozen
class DateRange(VersionedParser):
    start_date: date
    end_date: date

    @classmethod
    def from_doc(cls, obj: DateRangeJson) -> DateRange:
        return cls(
            date.fromisoformat(obj["startDate"]), date.fromisoformat(obj["endDate"])
        )

    @property
    def to_request(self) -> dict[str, str]:
        """Produce concrete API request data for this query"""
        return {
            "startDate": self.start_date.isoformat(),
            "endDate": self.end_date.isoformat(),
        }


@frozen
class Segment(VersionedParser):
    ...


@frozen
class CohortGroup(VersionedParser):
    ...


@frozen
class Filter(VersionedParser):
    ftype: FilterType
    operator: FilterOperator
    column: str
    value: int | str | list[str]

    @classmethod
    def from_doc(cls, ftype: str, expr: str):
        parsed = Expression.parse_string(expr)[0]
        column = parsed[0][0]
        operator = FilterOperator.get("expr", parsed[1])
        if operator == FilterOperator.IN:
            value = list(parsed[2])
        else:
            value = parsed[2]

        ftype = ftype.upper()
        if ftype[-1] == "S":  # strip trailing plurals
            ftype = ftype[:-1]
        return Filter(FilterType[ftype], operator, column, value)

    @property
    def to_request(self) -> dict[str, Any]:
        def serialize_dimension():
            value = self.value if self.operator == FilterOperator.IN else [self.value]
            return {
                "dimensionName": f"ga:{self.column}",
                "not": self.operator.value.ua.negated,
                "operator": self.operator.value.ua.op,
                "expressions": value,
                "caseSensitive": True,
            }

        def serialize_metric():
            return {
                "metricName": f"ga:{self.column}",
                "not": self.operator.value.ua.negated,
                "operator": self.operator.value.ua.op,
                "comparisonValue": str(self.value),
            }

        if self.ftype == FilterType.DIMENSION:
            return serialize_dimension()
        elif self.ftype == FilterType.METRIC:
            return serialize_metric()
        return NotImplemented


@frozen
class Column(VersionedParser):
    ctype: ColumnType
    expression: str
    alias: Optional[str] = field(default=None)
    # to-do: add histogram bucket support

    @classmethod
    def from_doc(cls, ctype: str, src: str):
        ctype = ctype.upper()
        if ctype[-1] == "S":  # strip trailing plurals
            ctype = ctype[:-1]
        return Column(ColumnType[ctype], src)

    @property
    def to_request(self) -> dict[str, Any]:
        """Produce concrete API request data for this query"""
        if self.ctype == ColumnType.DIMENSION:
            return {"name": f"ga:{self.expression}"}
        elif self.ctype == ColumnType.METRIC:
            return {"expression": f"ga:{self.expression}"}
        return NotImplemented


@frozen
class UARequestKey(VersionedParser):
    view_id: int
    date_ranges: tuple[DateRange, ...] = field(validator=len_between(1, 2))
    sampling: SamplingLevel = field(default=SamplingLevel.LARGE)
    segments: Optional[tuple[Segment, ...]] = field(
        default=None, validator=len_between(1, 4)
    )
    cohort: Optional[CohortGroup] = field(default=None)

    @classmethod
    def from_doc(cls, obj: dict[str, Any]):
        view_id = int(obj["scope"]["viewId"])
        date_ranges = tuple(map(lambda x: DateRange.from_doc(x), obj["dateRanges"]))

        if "queryOptions" in obj and "sampling" in obj["queryOptions"]:
            sampling = SamplingLevel[obj["queryOptions"]["sampling"]]
            return cls(view_id, date_ranges, sampling)
        else:
            return cls(view_id, date_ranges)

    @property
    def to_request(self) -> dict[str, Any]:
        """Produce concrete API request data for this query"""
        return {
            "viewId": str(self.view_id),
            "dateRanges": list(map(lambda x: x.to_request, self.date_ranges)),
            "samplingLevel": self.sampling.name,
        }


@frozen
class UAQueryOptions(VersionedParser):
    """Collection of paging & summary directives to UA reporting API"""

    DEFAULT_PAGE_SIZE = 10000

    page_size: int = field(default=DEFAULT_PAGE_SIZE)
    page_token: Optional[str] = field(default=None)
    include_empty_rows: bool = field(default=False)
    include_totals: bool = field(default=False)
    include_value_ranges: bool = field(default=False)
    key_list = [
        "pageSize",
        "pageToken",
        "includeEmptyRows",
        "includeTotals",
        "includeValueRanges",
    ]

    @classmethod
    def from_doc(cls, obj: dict[str, Any]):
        if "queryOptions" not in obj:
            return cls()

        opts = dict(
            filter(
                lambda kv: kv[1] is not None,
                map(
                    lambda opt: (
                        camel_to_snake_case(opt),
                        obj["queryOptions"].get(opt),
                    ),
                    cls.key_list,
                ),
            )
        )
        return cls(**opts)

    @property
    def to_request(self):
        """Produce concrete API request data for this query"""
        opts = {
            "pageSize": self.page_size,
            "includeEmptyRows": self.include_empty_rows,
            "hideTotals": not self.include_totals,
            "hideValueRanges": not self.include_value_ranges,
        }
        pt = {"pageToken": self.page_token} if self.page_token is not None else {}
        return opts | pt


@frozen
class UARequest(VersionedParser):
    key: UARequestKey
    columns: list[Column]
    filters: Optional[list[Filter]] = field(default=None)
    query_options: UAQueryOptions = field(default=UAQueryOptions())
    dimensions: list[Column] = field(validator=len_between(1, 7))
    metrics: list[Column] = field(validator=len_between(1, 10))
    version_key: tuple[int, ...] = field(default=(0, 1), kw_only=True)

    @dimensions.default
    def _dimension_factory(self):
        return list(filter(lambda col: col.ctype == ColumnType.DIMENSION, self.columns))

    @metrics.default
    def _metric_factory(self):
        return list(filter(lambda col: col.ctype == ColumnType.METRIC, self.columns))

    @classmethod
    def from_doc(cls, obj: dict[str, Any]):
        def map_columns(ctype: str) -> map[Column]:
            def build(col_def: str) -> Column:
                return Column.from_doc(ctype, col_def)

            return map(build, obj["columns"][ctype])

        def map_filters(ftype: str) -> map[Filter]:
            def build(expression: str):
                return Filter.from_doc(ftype, expression)

            return map(build, obj["filters"][ftype])

        def flatlist(fn: Callable[[str], map[T]], key: str) -> list[T]:
            """Iterate over nested arrays & flatten"""
            return list(chain.from_iterable(map(fn, obj[key])))

        def opt_flatlist(fn: Callable[[str], map[T]], key: str) -> Optional[list[T]]:
            """Call flatlist iff the key is on the object"""
            if key in obj:
                return flatlist(fn, key)
            return None

        return cls(
            UARequestKey.from_doc(obj),
            flatlist(map_columns, "columns"),
            filters=opt_flatlist(map_filters, "filters"),
            query_options=UAQueryOptions.from_doc(obj),
        )

    @property
    def to_request(self) -> dict[str, Any]:
        """Produce concrete API request data for this query"""

        def build_filter_clause(
            filter_type: FilterType,
        ) -> tuple[str, Optional[list[dict]]]:
            if self.filters is None:
                filters = []
            else:
                filters = list(
                    map(
                        lambda _filter: _filter.to_request,
                        filter(
                            lambda _filter: _filter.ftype == filter_type, self.filters
                        ),
                    )
                )
            key = f"{filter_type.name.lower()}FilterClauses"
            value = [{"operator": "AND", "filters": filters}]
            return (key, value) if len(filters) > 0 else (key, None)

        cols = {
            "dimensions": list(map(lambda col: col.to_request, self.dimensions)),
            "metrics": list(map(lambda col: col.to_request, self.metrics)),
        }
        filters = dict(
            filter(
                lambda x: x[1] is not None,
                map(lambda x: build_filter_clause(x), FilterType),
            )
        )
        return self.key.to_request | cols | filters | self.query_options.to_request


@define
class UARequestBatch:
    """1 batch of up to 5 requests to be sent to the API"""

    key: UARequestKey
    requests: list[UARequest] = field(init=False)


if __name__ == "__main__":
    with open("ga-reporting/1.yaml") as f:
        data = yaml.load(f, Loader=yaml.SafeLoader)
