import json
import argparse
import time
import random
import sys
from datetime import date
from googleapiclient.errors import HttpError

from google_auth import Services

service=Services.from_auth_context("GoogleAds").analytics_service

def sendRequest(request):
    """Make API requests with exponential backoff"""
    retryable_errors = [
        'userRateLimitExceeded',
        'quotedExceeded',
        'internalServerError',
        'backendError'
    ]

    max_retries = 5
    for n in range(0, max_retries):
        try:
            return request.execute()

        except HttpError as error:
            if error.resp.reason in retryable_errors and n < max_retries:
                time.sleep((2 ** n) + random.random())
            else:
                raise error



def init_parsers(parser: argparse.ArgumentParser):
    """Spawns a parser to read the command line inputs

    The only mandatory argument is the report request body. Everything else is
    optional.

    The options provide a convenience utility for updating view IDs & date 
    ranges on a pre-existing report request body. If either is omitted, it is 
    assumed to be defined on the report request body.
    """
    date_group = parser.add_mutually_exclusive_group(required=False)
    date_group.add_argument(
        '--date',
        help='Pull data for a single date',
        type=date.fromisoformat)
    date_group.add_argument(
        '--dates',
        nargs=2,
        metavar=('START_DATE', 'END_DATE'),
        help='Pull data for the bounded date range START_DATE END_DATE',
        type=date.fromisoformat)

    compare_date_group = parser.add_mutually_exclusive_group(required=False)
    compare_date_group.add_argument(
        '--compare-date',
        help='Pull comparison data for a single date',
        type=date.fromisoformat)
    compare_date_group.add_argument(
        '--compare-dates',
        nargs=2,
        metavar=('START_DATE', 'END_DATE'),
        help='Pull comparison data for the bounded date range START_DATE END_DATE',
        type=date.fromisoformat)

    parser.add_argument(
        '--view-id',
        required=False,
        help='Set view ID to pull report data from',
    )
    parser.add_argument(
        'body',
        help='File containing the API request JSON',
        type=argparse.FileType('r', encoding='UTF-8')
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create requests against the GA Reporting API")

    init_parsers(parser)
    cmd_args = parser.parse_args()

    request_body = json.load(cmd_args.body)
    request = service.reports().batchGet(body=request_body)
    json.dump(sendRequest(request), sys.stdout)
