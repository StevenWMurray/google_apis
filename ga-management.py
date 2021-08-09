#!/usr/bin/env python
"""Command line tool for making requests to the GA Management API"""

import json
import argparse
import time
import random
import sys
from pathlib import Path

from googleapiclient.errors import HttpError
from google_auth import Services

service=Services.from_auth_context("GoogleAds").analytics_management_service
arg_data_path=Path("utilities/ga-management-args.json")

pgh = lambda parent, child: f"ID of the {parent} to retrieve the {child} from."
gh = lambda child: f"ID of the {child} object to retrieve"

entity_help = {
    "accounts": "Operations on accounts",
    "accountSummaries": "Get data on full account hierarchies",
    "webproperties": "Operations on website properties",
    "profiles": "Operations on profiles (views)",
    "webPropertyAdWordsLinks": "Operations on Google Ads links",
    "goals": "Operations on view goals"
}

type_map = {
    "string": str,
    "integer": int,
    "boolean": bool,
    "FILE": argparse.FileType('r')
}

def parse_arg_data(data_path: Path):
    """Serialize arg data JSON into code

    In addition to loading the json, this substitutes a couple values in the
    arg data with functions.
    """
    arg_data = json.loads(data_path.read_text())
    for api in arg_data:
        api['help'] = f"Invokes the GA {api['name']} API"
        for entity in api["entities"]:
            for endpoint in entity["endpoints"]:
                endpoint["library_func"] = getattr(
                    getattr(
                        getattr(service, api["name"])(),
                        entity["name"])(),
                    endpoint["name"])
                # lib_func = getattr(lib_func, entity["name"])()
                # endpoint["library_func"] = getattr(lib_func, endpoint["name"])
                for arg in endpoint["args"]:
                    arg["data"]["type"] = type_map[arg["data"]["type"]]
                    if arg["name"] == "body":
                        arg["data"]["nargs"] = "?"
                        arg["data"]["default"] = sys.stdout
    return arg_data


def add_api_selection_parser(parser, api_data: dict) -> None:
    api_parser = parser.add_parser(api_data['name'], help=api_data['help'])
    entities = api_data['entities']
    subparser = api_parser.add_subparsers(
        description="Declare which entity type to operate on",
        required=True)

    for entity_type in entities:
        add_entity_type_parser(subparser, entity_type)

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
    endpoint_parser = parser.add_parser(endpoint['name'], help=endpoint['help'])
    endpoint_parser.set_defaults(library_func=endpoint['library_func'])
    for arg in endpoint['args']:
        endpoint_parser.add_argument(arg['name'], **arg.get('data', {}))


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


def init_parsers(parser: argparse.ArgumentParser) -> None:
    """Add an argument parser for each supported API"""
    subparser = parser.add_subparsers(
        description="Declare which API to invoke",
        required=True)

    for entity_type in parse_arg_data(arg_data_path):
        add_api_selection_parser(subparser, entity_type)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create requests against the GA Management API")

    init_parsers(parser)
    cmd_args = parser.parse_args()
    library_func = cmd_args.library_func
    delattr(cmd_args, 'library_func')

    if hasattr(cmd_args, 'body'):
        cmd_args.body = json.load(cmd_args.body)
    request = library_func(**vars(cmd_args))
    json.dump(sendRequest(request), sys.stdout)
    print()
