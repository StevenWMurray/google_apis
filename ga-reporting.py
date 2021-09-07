#!/usr/bin/env python

"""Invoke GA reporting API, passing each input line as a request body"""

import json
import argparse
import time
import random
import sys
import math
import itertools
from datetime import date, timedelta

from google_auth import Services, send_request

service=Services.from_auth_context("GoogleAds").analytics_service


def init_parsers(parser: argparse.ArgumentParser):
    """Spawns a parser to read the command line inputs

    The only mandatory argument is the report request body. Everything else is
    optional.
    """
    parser.add_argument(
        'body',
        nargs='?',
        default=sys.stdin,
        help='File containing the API request JSON',
        type=argparse.FileType('r', encoding='UTF-8'))
    parser.add_argument(
        '--output-file',
        '-o',
        default=sys.stdout,
        help='Output file to write the API response to',
        type=argparse.FileType('w', encoding='UTF-8'))


def read_input(parser):
    """Produce a stream of JSON report requests from CLI args

    This attempts to read the input file as a JSONL, or if that fails, then as a
    JSONL file. Each found input is yielded one at a time.
    """
    lines = [line for line in parser.body]
    try:
        yield from (json.loads(line) for line in lines)
    except json.JSONDecodeError:
        yield json.loads('\n'.join(lines))


def has_sampling(response: dict):
    """Check response for sampling"""
    for report in response["reports"]:
        if "samplingSpaceSizes" in report["data"]:
            return True
    return False

def split_request(request_body: dict, response: dict):
    """Take each date range in the request body and shrink it

    This takes into account the number of samples read vs. the sampling space,
    and divides the date range into even intervals.
    """
    num_sessions = int(response['reports'][0]['data']['samplingSpaceSizes'][0])
    num_samples = int(response['reports'][0]['data']['samplesReadCounts'][0])
    num_intervals = math.ceil(num_sessions / num_samples * 4/3)

    interval_groups = []
    for date_range in request_body['reportRequests'][0]['dateRanges']:
        start = date.fromisoformat(date_range['startDate'])
        end = date.fromisoformat(date_range['endDate'])
        temp = (end - start + timedelta(days=1)) / num_intervals
        interval_len = timedelta(days=(temp.days+(temp.seconds>0)))

        intervals = []
        for i in range(0, num_intervals-1):
            int_end = start + interval_len - timedelta(days=1)
            intervals.append({
                "startDate": start.isoformat(),
                "endDate": int_end.isoformat()
            })
            start += interval_len
        intervals.append({
            "startDate": start.isoformat(),
            "endDate": end.isoformat()
        })
        interval_groups.append(intervals)

    new_date_ranges = zip(*interval_groups)
    for date_range_group in new_date_ranges:
        for request in request_body['reportRequests']:
            request['dateRanges'] = list(date_range_group)
        yield request_body


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create requests against the GA Reporting API")

    init_parsers(parser)
    cmd_args = parser.parse_args()

    queue = read_input(cmd_args)
    try:
        while True:
            request_body = next(queue)
            request = service.reports().batchGet(body=request_body)
            response = send_request(request)
            if (has_sampling(response)):
                queue = itertools.chain(
                    queue, split_request(request_body, response))
            else:
                response['request'] = request_body
                json.dump(response, cmd_args.output_file)
                cmd_args.output_file.write('\n')
    except StopIteration:
        pass
