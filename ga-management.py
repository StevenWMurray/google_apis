#!/usr/bin/env python
"""Command line tool for making requests to the GA Management API"""

import json
import argparse
import time
import random
import sys
from dataclasses import dataclass
from googleapiclient.errors import HttpError
from google_auth import Services

entity_types = [
    {
        "name": "accounts",
        "help": "Operations on accounts",
        "endpoints": [
            {
                "name": "list",
                "help": "List account data",
                "args": [
                    {
                        "name": "--max-results"
                    }, {
                        "name": "--start-index"
                    }
                ]
            }
        ]
    }
]

service=Services.from_auth_context("GoogleAds").analytics_management_service


def add_entity_type_parser(parser, entity_type: dict) -> None:
    """Simplified interface to add entity type parsers to argparser"""
    entity_parser = parser.add_parser(entity_type['name'], help=entity_type['help'])
    endpoints = entity_type['endpoints']
    subparser = entity_parser.add_subparsers(
        description="Declare which endpoints to call",
        required=True)

    for endpoint in endpoints:
        add_endpoint_parser(subparser, endpoint)

def add_endpoint_parser(parser, endpoint: dict) -> None:
    """Simplified interface to add endpoint parsers to argparser"""
    parser.add_parser(endpoint['name'], help=endpoint['help'])


def makeRequest(request):
    """Make API requests with exponential backoff"""
    retryable_errors = [
        'userRateLimitExceeded',
        'quotedExceeded',
        'internalServerError',
        'backendError'
    ]

    for n in range(0, 5):
        try:
            return request.execute()

        except HttpError as error:
            if error.resp.reason in retryable_errors:
                time.sleep((2 ** n) + random.random())
            else:
                break
                
    print(json.dumps(error), file=sys.stderr)


def init_entity_type_parsers(parser: argparse.ArgumentParser) -> None:
    """Add an argument parser for each supported API entity type"""
    subparser = parser.add_subparsers(
        description="Declare which GA entity type to invoke",
        dest="endpoint",
        required=True)

    for entity_type in entity_types:
        add_entity_type_parser(subparser, entity_type)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create requests against the GA Management API")

    init_entity_type_parsers(parser)
    namespace = parser.parse_args(['accounts', 'list', '--help'])
    print(namespace)
