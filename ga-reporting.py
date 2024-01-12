#!/usr/bin/env python

"""Invoke GA reporting API, passing each input line as a request body"""

import json
import yaml
import argparse
import sys
import math
import logging
import io
from warnings import warn
from typing import Iterator, NamedTuple, TYPE_CHECKING, Any
from datetime import date, timedelta
from copy import deepcopy
from collections.abc import Mapping
from itertools import chain

from google_auth import Services, send_request

import uar


class DateRange(NamedTuple):
    start_date: str
    end_date: str


service = Services.from_auth_context("GoogleAds").analytics_service


def init_parsers(parser: argparse.ArgumentParser) -> None:
    """Spawns a parser to read the command line inputs

    The only mandatory argument is the report request body. Everything else is
    optional.

    For now, the "use new query format" is opt-in, to give time to migrate
    existing tools that call the CLI bindings to the new query format. This
    will eventually become the default option.
    """
    parser.add_argument(
        "body",
        nargs="?",
        default=sys.stdin,
        help="File containing the API request(s)",
        type=argparse.FileType("r", encoding="UTF-8"),
    )
    parser.add_argument(
        "--use-new-query-format",
        "-q",
        default=False,
        help="Declare whether to use the new universal query format, or parse input as a pre-built GA API request.",
        type=bool,
    )
    parser.add_argument(
        "--output-file",
        "-o",
        default=sys.stdout,
        help="Output file to write the API response to",
        type=argparse.FileType("w", encoding="UTF-8"),
    )
    parser.add_argument(
        "--debug",
        "-d",
        default=False,
        help="Produce additional debugging output",
        type=bool,
    )


def get_cli_opts() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create requests against the GA Reporting API"
    )

    init_parsers(parser)
    cmd_args = parser.parse_args()
    return cmd_args


def read_json_input(body: io.TextIOWrapper) -> Iterator[dict]:
    """Produce a stream of JSON report requests from CLI args

    This attempts to read the input file as a JSONL, or if that fails, then as a
    JSON file. Each found input is yielded one at a time.
    """
    lines = [line for line in body]
    try:
        yield from (json.loads(line) for line in lines)
    except json.JSONDecodeError:
        yield json.loads("\n".join(lines))


def has_sampling(response: dict) -> bool:
    """Check response for sampling"""
    for report in response["reports"]:
        if (
            "samplingSpaceSizes" in report["data"]
            or "samplesReadCounts" in report["data"]
        ):
            return True
    return False


def split_request(request_body: dict, response: dict):
    """Take each date range in the request body and shrink it

    This takes into account the number of samples read vs. the sampling space,
    and divides the date range into even intervals.
    """

    def calculate_samples():
        """Guess # of intervals required to fix sampled request"""
        num_sessions = int(response["reports"][0]["data"]["samplingSpaceSizes"][0])
        num_samples = int(response["reports"][0]["data"]["samplesReadCounts"][0])
        num_intervals = math.ceil(num_sessions / num_samples * 4 / 3)
        sampling = {
            "sessions": num_sessions,
            "samples": num_samples,
            "num_intervals": num_intervals,
        }
        logging.debug(f"ResponseSampling: {json.dumps(sampling)}")
        return sampling

    def generate_intervals(request_body: dict, num_intervals: int) -> list[list[dict]]:
        interval_groups = []
        for date_range in request_body["reportRequests"][0]["dateRanges"]:
            # Format: {startDate: str, endDate: str}
            start = date.fromisoformat(date_range["startDate"])
            end = date.fromisoformat(date_range["endDate"])
            temp = (end - start + timedelta(days=1)) / num_intervals
            interval_len = timedelta(days=(temp.days + (temp.seconds > 0)))

            intervals = []
            for i in range(0, num_intervals - 1):
                int_end = start + interval_len - timedelta(days=1)
                intervals.append(
                    {"startDate": start.isoformat(), "endDate": int_end.isoformat()}
                )
                start += interval_len
            intervals.append(
                {"startDate": start.isoformat(), "endDate": end.isoformat()}
            )
            interval_groups.append(intervals)
        return interval_groups

    num_intervals = calculate_samples()["num_intervals"]
    interval_groups = generate_intervals(request_body, num_intervals)
    new_date_ranges = zip(*interval_groups)
    for date_range_group in new_date_ranges:
        # copy required here to ensure that separate retry attempts don't
        # overwrite each other's date ranges
        rb = deepcopy(request_body)
        for request in rb["reportRequests"]:
            request["dateRanges"] = list(date_range_group)
        yield rb


def execute_api_queries(
    input_file: io.TextIOWrapper, output_file: io.TextIOWrapper, DEBUGGING: bool = False
):
    queue = read_json_input(input_file)
    try:
        while True:
            request_body = next(queue)
            request = service.reports().batchGet(body=request_body)
            response = send_request(request)

            if has_sampling(response):
                bad_req = deepcopy(request_body)
                logging.debug(
                    "SAMPLED RESPONSE: "
                    + str(request_body["reportRequests"][0]["dateRanges"])
                )
                if DEBUGGING:
                    print(
                        "ADD TO QUEUE"
                        + str(request_body["reportRequests"][0]["dateRanges"])
                    )
                    bad_resp = deepcopy(response)
                    bad_resp["request"] = bad_req
                    json.dump(bad_resp, output_file)
                queue = chain(queue, split_request(bad_req, deepcopy(response)))
            else:
                response["request"] = request_body
                json.dump(response, output_file)
                output_file.write("\n")
    except StopIteration:
        pass


def main_v1(
    input_file: io.TextIOWrapper, output_file: io.TextIOWrapper, DEBUGGING: bool = False
):
    """Main flow of control for new query version

    Does not yet support splitting based on sampled responses.
    """
    query = uar.UARequestBatch.from_doc(
        list(yaml.load_all(input_file, Loader=yaml.SafeLoader))
    )


def execute(query: uar.UARequestBatch) -> dict[str, Any]:
    for request in query.to_request:
        response = send_request(request)
        if has_sampling(response):
            warn_msg = f"Sampling detected on query with view ID {query}"
            # warn()
    return {}


if __name__ == "__main__":
    cmd_args = get_cli_opts()
    if cmd_args.debug:
        logging.basicConfig(level=logging.DEBUG)
    if cmd_args.use_new_query_format:
        pass
    else:
        execute_api_queries(cmd_args.body, cmd_args.output_file, cmd_args.debug)
