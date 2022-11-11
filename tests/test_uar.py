import yaml
import json
from unittest import TestCase, main
from datetime import date
from typing import Any, Sequence

from uar_types import AliasedEnum, AliasedValue, UAFilterLiteral
from uar import (
    UARequest,
    UARequestKey,
    DateRange,
    SamplingLevel,
    Column,
    ColumnType,
    UAQueryOptions,
    camel_to_snake_case,
    snake_to_camel_case,
    Filter,
    FilterOperator,
    FilterType,
)


def get_fields(field_list: Sequence[str], obj: dict[str, Any]) -> dict[str, Any]:
    _kl = filter(lambda field: field in obj, field_list)
    return {key: obj[key] for key in _kl}


class TestUtilityFunctions(TestCase):
    def testCamelToSnakeCaseConversion(self):
        self.assertEqual("", camel_to_snake_case(""))
        self.assertEqual("page_size", camel_to_snake_case("pageSize"))
        self.assertEqual(
            "include_value_ranges", camel_to_snake_case("includeValueRanges")
        )

    def testSnakeToCamelCaseConversion(self):
        self.assertEqual("", snake_to_camel_case(""))
        self.assertEqual("pageSize", snake_to_camel_case("page_size"))
        self.assertEqual(
            "includeValueRanges", snake_to_camel_case("include_value_ranges")
        )

    def testCallingEnumDotNameReturnsFirstDeclaredNameWithSameValue(self):
        self.assertEqual("DEFAULT", SamplingLevel.DEFAULT.name)
        self.assertEqual("DEFAULT", SamplingLevel.MEDIUM.name)

    def testGetMethodOnAliasedEnum(self):
        class TestEnum(AliasedEnum):
            EQ = AliasedValue("==", UAFilterLiteral("EXACT", False))
            NEQ = AliasedValue("!=", UAFilterLiteral("EXACT", True))
            CONTAINS = AliasedValue("*=", UAFilterLiteral("PARTIAL", False))

        self.assertEqual(TestEnum.EQ, TestEnum.get("expr", "=="))
        self.assertEqual(TestEnum.CONTAINS, TestEnum.get("expr", "*="))
        self.assertEqual(
            TestEnum.NEQ, TestEnum.get("ua", UAFilterLiteral("EXACT", True))
        )


class TestBasicDocumentParse(TestCase):
    doc_1 = yaml.load(
        """
        scope:
            viewId: 16619750
        dateRanges:
            - startDate: '2022-02-01'
              endDate: '2022-02-28'
            - startDate: '2022-01-01'
              endDate: '2022-01-31'
        columns:
            dimensions:
                - 'medium'
                - 'source'
            metrics:
                - 'sessions'
        queryOptions:
            sampling: "MEDIUM"
            pageSize: 50
            includeEmptyRows: FALSE
            includeTotals: TRUE
            includeValueRanges: TRUE
        """,
        Loader=yaml.SafeLoader,
    )

    parsed_doc1: dict[str, Any] = {
        "key": UARequestKey(
            16619750,
            (
                DateRange(date(2022, 2, 1), date(2022, 2, 28)),
                DateRange(date(2022, 1, 1), date(2022, 1, 31)),
            ),
            SamplingLevel.DEFAULT,
        ),
        "columns": [
            Column(ColumnType.DIMENSION, "medium"),
            Column(ColumnType.DIMENSION, "source"),
            Column(ColumnType.METRIC, "sessions"),
        ],
        "queryOptions": UAQueryOptions(
            page_size=50,
            include_totals=True,
            include_empty_rows=False,
            include_value_ranges=True,
        ),
    }

    parsed_1 = UARequest(
        parsed_doc1["key"],
        parsed_doc1["columns"],
        query_options=parsed_doc1["queryOptions"],
    )

    output = {
        "viewId": "16619750",
        "dateRanges": [
            {"startDate": "2022-02-01", "endDate": "2022-02-28"},
            {"startDate": "2022-01-01", "endDate": "2022-01-31"},
        ],
        "dimensions": [{"name": "ga:medium"}, {"name": "ga:source"}],
        "metrics": [{"expression": "ga:sessions"}],
        "samplingLevel": "DEFAULT",
        "pageSize": 50,
        "includeEmptyRows": False,
        "hideTotals": False,
        "hideValueRanges": False,
    }

    output_parts: dict[str, Any] = {
        "key": {
            "viewId": "16619750",
            "dateRanges": [
                {"startDate": "2022-02-01", "endDate": "2022-02-28"},
                {"startDate": "2022-01-01", "endDate": "2022-01-31"},
            ],
            "samplingLevel": "DEFAULT",
        },
        "columns": {
            "dimensions": [{"name": "ga:medium"}, {"name": "ga:source"}],
            "metrics": [{"expression": "ga:sessions"}],
        },
        "queryOptions": {
            "pageSize": 50,
            "includeEmptyRows": False,
            "hideTotals": False,
            "hideValueRanges": False,
        },
    }

    def testParseDocKey(self) -> None:
        self.assertEqual(self.parsed_doc1["key"], UARequestKey.from_doc(self.doc_1))

    def testParseDocDimensionsMetrics(self) -> None:
        self.assertEqual(
            self.parsed_1.dimensions,
            [
                Column(ColumnType.DIMENSION, "medium"),
                Column(ColumnType.DIMENSION, "source"),
            ],
        )
        self.assertEqual(self.parsed_1.metrics, [Column(ColumnType.METRIC, "sessions")])

    def testParseDocQuery(self) -> None:
        self.assertEqual(
            self.parsed_doc1["queryOptions"], UAQueryOptions.from_doc(self.doc_1)
        )

    def testParseDocFull(self) -> None:
        self.assertEqual(self.parsed_doc1["key"], UARequest.from_doc(self.doc_1).key)
        self.assertEqual(
            self.parsed_doc1["columns"], UARequest.from_doc(self.doc_1).columns
        )
        self.assertEqual(
            self.parsed_doc1["queryOptions"],
            UARequest.from_doc(self.doc_1).query_options,
        )

    def testOutputDocKey(self) -> None:
        self.assertEqual(self.output_parts["key"], self.parsed_doc1["key"].to_request)

    def testOutputDocQueryOpts(self) -> None:
        self.assertEqual(
            self.output_parts["queryOptions"],
            self.parsed_doc1["queryOptions"].to_request,
        )

    def testOutputDocFull(self) -> None:
        def get_key(request: dict[str, Any]) -> dict[str, Any]:
            _kl = ["viewId", "dateRanges", "segments", "cohortGroup", "samplingLevel"]
            return get_fields(_kl, request)

        def get_opts(request: dict[str, Any]) -> dict[str, Any]:
            _kl = [
                "pageSize",
                "pageToken",
                "includeEmptyRows",
                "hideTotals",
                "hideValueRanges",
            ]
            return get_fields(_kl, request)

        def get_columns(request: dict[str, Any]) -> dict[str, Any]:
            _kl = ["dimensions", "metrics"]
            return get_fields(_kl, request)

        query = self.parsed_1
        query_key = get_key(query.to_request)
        query_opts = get_opts(query.to_request)
        query_cols = get_columns(query.to_request)
        self.assertEqual(query_key, self.output_parts["key"])
        self.assertEqual(query_opts, self.output_parts["queryOptions"])
        self.assertEqual(query_cols, self.output_parts["columns"])
        self.assertEqual(query.to_request, self.output)


class TestDocumentWithNoQueryOptions(TestCase):
    doc = yaml.load(
        """
        scope:
            viewId: 16619750
        dateRanges:
            - startDate: '2022-02-01'
              endDate: '2022-02-28'
        columns:
            dimensions:
                - 'date'
            metrics:
                - 'sessions'
        """,
        Loader=yaml.SafeLoader,
    )

    query = UARequest(
        key=UARequestKey(
            16619750,
            (DateRange(date(2022, 2, 1), date(2022, 2, 28)),),
            SamplingLevel.LARGE,
        ),
        columns=[
            Column(ColumnType.DIMENSION, "date"),
            Column(ColumnType.METRIC, "sessions"),
        ],
        query_options=UAQueryOptions(
            page_size=10000,
            include_totals=False,
            include_empty_rows=False,
            include_value_ranges=False,
        ),
    )

    request = {
        "viewId": "16619750",
        "dateRanges": [{"startDate": "2022-02-01", "endDate": "2022-02-28"}],
        "dimensions": [{"name": "ga:date"}],
        "metrics": [{"expression": "ga:sessions"}],
        "samplingLevel": "LARGE",
        "pageSize": 10000,
        "includeEmptyRows": False,
        "hideTotals": True,
        "hideValueRanges": True,
    }

    def testDocParsesWithNoQueryOpts(self):
        self.assertEqual(self.query, UARequest.from_doc(self.doc))

    def testDocSerializesWithNoQueryOpts(self):
        self.assertEqual(self.request, self.query.to_request)


class TestDocumentWithFiltersParse(TestCase):
    doc = yaml.load(
        """
        scope:
            viewId: 16619750
        dateRanges:
            - startDate: '2022-02-01'
              endDate: '2022-02-28'
        columns:
            dimensions:
                - 'date'
            metrics:
                - 'sessions'
        filters:
            dimensions:
                - "source == 'google'"
                - "medium IN ('cpc', 'ppc')"
            metrics:
                - "sessions >= 100"
        """,
        Loader=yaml.SafeLoader,
    )

    parsed_filters = [
        Filter(FilterType.DIMENSION, FilterOperator.EQ, "source", "google"),
        Filter(FilterType.DIMENSION, FilterOperator.IN, "medium", ["cpc", "ppc"]),
        Filter(FilterType.METRIC, FilterOperator.GTE, "sessions", 100),
    ]

    request_filters = [
        {
            "dimensionName": "ga:source",
            "not": False,
            "operator": "EXACT",
            "expressions": ["google"],
            "caseSensitive": True,
        },
        {
            "dimensionName": "ga:medium",
            "not": False,
            "operator": "IN_LIST",
            "expressions": ["cpc", "ppc"],
            "caseSensitive": True,
        },
        {
            "metricName": "ga:sessions",
            "not": True,
            "operator": "LESS_THAN",
            "comparisonValue": "100",
        },
    ]

    request = {
        "viewId": "16619750",
        "dateRanges": [{"startDate": "2022-02-01", "endDate": "2022-02-28"}],
        "samplingLevel": "LARGE",
        "dimensions": [{"name": "ga:date"}],
        "metrics": [{"expression": "ga:sessions"}],
        "dimensionFilterClauses": [{"operator": "AND", "filters": request_filters[:2]}],
        "metricFilterClauses": [{"operator": "AND", "filters": request_filters[2:]}],
        "samplingLevel": "LARGE",
        "pageSize": 10000,
        "includeEmptyRows": False,
        "hideTotals": True,
        "hideValueRanges": True,
    }

    query = UARequest(
        key=UARequestKey(16619750, (DateRange(date(2022, 2, 1), date(2022, 2, 28)),)),
        columns=[
            Column(ColumnType.DIMENSION, "date"),
            Column(ColumnType.METRIC, "sessions"),
        ],
        filters=parsed_filters,
    )

    def testParseFilterExpression(self) -> None:
        self.assertEqual(
            self.parsed_filters[0], Filter.from_doc("dimensions", "source == 'google'")
        )
        self.assertEqual(
            self.parsed_filters[1],
            Filter.from_doc("dimensions", "medium IN ('cpc', 'ppc')"),
        )
        self.assertEqual(
            self.parsed_filters[2], Filter.from_doc("metrics", "sessions >= 100")
        )

    def testParseDocWithFilters(self) -> None:
        self.assertEqual(self.query, UARequest.from_doc(self.doc))

    def testSerializeQueryWithFilters(self) -> None:
        self.assertEqual(self.request_filters[0], self.parsed_filters[0].to_request)
        self.assertEqual(self.request_filters[1], self.parsed_filters[1].to_request)
        self.assertEqual(self.request_filters[2], self.parsed_filters[2].to_request)

    def testSerializeDocWithFilters(self) -> None:
        self.assertEqual(self.request, self.query.to_request)


if __name__ == "__main__":
    main()
