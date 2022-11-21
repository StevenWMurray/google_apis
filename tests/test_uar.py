import yaml
import json
from unittest import TestCase, main
from datetime import date
from typing import Any, Sequence

from uar_types import AliasedEnum, AliasedValue, UAFilterLiteral, KeyRequestPair
from uar import (
    UARequest,
    UARequestKey,
    UARequestBatch,
    DateRange,
    SamplingLevel,
    Column,
    ColumnType,
    UAQueryOptions,
    camel_to_snake_case,
    snake_to_camel_case,
    chunk,
    Filter,
    FilterOperator,
    FilterType,
)


def get_fields(field_list: Sequence[str], obj: dict[str, Any]) -> dict[str, Any]:
    _kl = filter(lambda field: field in obj, field_list)
    return {key: obj[key] for key in _kl}


def requests_from_pairs(krpairs: Sequence[KeyRequestPair]) -> list[dict[str, Any]]:
    return list(map(lambda x: x.request, krpairs))


class TestUtilityFunctions(TestCase):
    def testCamelToSnakeCaseConversion(self) -> None:
        self.assertEqual("", camel_to_snake_case(""))
        self.assertEqual("page_size", camel_to_snake_case("pageSize"))
        self.assertEqual(
            "include_value_ranges", camel_to_snake_case("includeValueRanges")
        )

    def testSnakeToCamelCaseConversion(self) -> None:
        self.assertEqual("", snake_to_camel_case(""))
        self.assertEqual("pageSize", snake_to_camel_case("page_size"))
        self.assertEqual(
            "includeValueRanges", snake_to_camel_case("include_value_ranges")
        )

    def testCallingEnumDotNameReturnsFirstDeclaredNameWithSameValue(self) -> None:
        self.assertEqual("DEFAULT", SamplingLevel.DEFAULT.name)
        self.assertEqual("DEFAULT", SamplingLevel.MEDIUM.name)

    def testGetMethodOnAliasedEnum(self) -> None:
        class TestEnum(AliasedEnum):
            EQ = AliasedValue("==", UAFilterLiteral("EXACT", False))
            NEQ = AliasedValue("!=", UAFilterLiteral("EXACT", True))
            CONTAINS = AliasedValue("*=", UAFilterLiteral("PARTIAL", False))

        self.assertEqual(TestEnum.EQ, TestEnum.get("expr", "=="))
        self.assertEqual(TestEnum.CONTAINS, TestEnum.get("expr", "*="))
        self.assertEqual(
            TestEnum.NEQ, TestEnum.get("ua", UAFilterLiteral("EXACT", True))
        )

    def testChunk(self) -> None:
        result = [[1, 2], [3]]
        self.assertEqual(chunk([1, 2, 3], 2), [[1, 2], [3]])
        self.assertEqual(chunk("foobar", 4), [["f", "o", "o", "b"], ["a", "r"]])


class TestDateRangeUtilities(TestCase):
    dtr_1 = DateRange(date(2021, 9, 12), date(2021, 9, 12))
    dtr_2 = DateRange(date(2022, 2, 1), date(2022, 2, 28))
    dtr_3 = DateRange(date(2020, 1, 1), date(2020, 3, 31))  # leap year

    def testDateRangeLen(self) -> None:
        self.assertEqual(len(self.dtr_1), 1)
        self.assertEqual(len(self.dtr_2), 28)
        self.assertEqual(len(self.dtr_3), 91)

    def testDateRangeContains(self) -> None:
        self.assertIn(date(2021, 9, 12), self.dtr_1)
        self.assertNotIn(date(2022, 3, 1), self.dtr_2)
        self.assertIn(date(2020, 2, 29), self.dtr_3)

    def testDateRangeContainsStr(self) -> None:
        self.assertIn("2021-09-12", self.dtr_1)
        self.assertNotIn("2022-03-01", self.dtr_2)
        # wrong date format
        self.assertNotIn("February 29, 2020", self.dtr_3)

    def testDateRangeGetItem(self) -> None:
        self.assertEqual(self.dtr_1[0], date(2021, 9, 12))
        self.assertEqual(self.dtr_2[-1], date(2022, 2, 28))
        self.assertEqual(self.dtr_3[-10], date(2020, 3, 22))

    def testDateRangeGetSlice(self) -> None:
        self.assertEqual(self.dtr_1[:], self.dtr_1)
        self.assertEqual(
            self.dtr_3[31:51], DateRange(date(2020, 2, 1), date(2020, 2, 20))
        )
        self.assertEqual(
            self.dtr_2[10:-5], DateRange(date(2022, 2, 11), date(2022, 2, 23))
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

    def testDocParsesWithNoQueryOpts(self) -> None:
        self.assertEqual(self.query, UARequest.from_doc(self.doc))

    def testDocSerializesWithNoQueryOpts(self) -> None:
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


class TestBuildUARequestBatch(TestCase):
    doc_1 = yaml.load(
        """
        scope:
            viewId: 16619750
        dateRanges:
            - startDate: '2022-01-01'
              endDate: '2022-03-31'
        columns:
            dimensions:
                - 'date'
            metrics:
                - 'sessions'
        """,
        Loader=yaml.SafeLoader,
    )

    doc_2 = yaml.load(
        """
        scope:
            viewId: 16619750
        dateRanges:
            - startDate: '2022-01-01'
              endDate: '2022-03-31'
        columns:
            dimensions:
                - 'campaign'
            metrics:
                - 'pageviews'
        """,
        Loader=yaml.SafeLoader,
    )

    doc_3 = yaml.load(
        """
        scope:
            viewId: 16619750
        dateRanges:
            - startDate: '2022-04-01'
              endDate: '2022-06-30'
        columns:
            dimensions:
                - 'date'
            metrics:
                - 'sessions'
        """,
        Loader=yaml.SafeLoader,
    )

    def setUp(self) -> None:
        self.batch1 = [
            UARequest(
                key=UARequestKey(
                    16619750, (DateRange(date(2022, 1, 1), date(2022, 3, 31)),)
                ),
                columns=[
                    Column(ColumnType.DIMENSION, "date"),
                    Column(ColumnType.METRIC, "sessions"),
                ],
            ),
            UARequest(
                key=UARequestKey(
                    16619750, (DateRange(date(2022, 1, 1), date(2022, 3, 31)),)
                ),
                columns=[
                    Column(ColumnType.DIMENSION, "campaign"),
                    Column(ColumnType.METRIC, "pageviews"),
                ],
            ),
        ]
        self.batch2 = [
            UARequest(
                key=UARequestKey(
                    16619750, (DateRange(date(2022, 4, 1), date(2022, 6, 30)),)
                ),
                columns=[
                    Column(ColumnType.DIMENSION, "date"),
                    Column(ColumnType.METRIC, "sessions"),
                ],
            ),
        ]
        key1 = self.batch1[0].key
        key2 = self.batch2[0].key
        self.batches = UARequestBatch({key1: self.batch1, key2: self.batch2})

    def tearDown(self) -> None:
        UARequestBatch.MAXSIZE = 5

    def testCreateBatchFromQueryYaml(self) -> None:
        return self.assertEqual(
            self.batches, UARequestBatch.from_doc([self.doc_1, self.doc_2, self.doc_3])
        )

    def testBuildRequestFromBatchNoSplittingRequired(self) -> None:
        request1 = {
            "reportRequests": [self.batch1[0].to_request, self.batch1[1].to_request]
        }
        request2 = {"reportRequests": [self.batch2[0].to_request]}
        result = requests_from_pairs(self.batches.to_request)
        self.assertIn(request1, result)
        self.assertIn(request2, result)

    def testBuildRequestFromBatchWithSplitting(self) -> None:
        UARequestBatch.MAXSIZE = 1
        request1 = {"reportRequests": [self.batch1[0].to_request]}
        request2 = {"reportRequests": [self.batch1[1].to_request]}
        request3 = {"reportRequests": [self.batch2[0].to_request]}
        result = requests_from_pairs(self.batches.to_request)
        self.assertIn(request1, result)
        self.assertIn(request2, result)
        self.assertIn(request3, result)


if __name__ == "__main__":
    main()
