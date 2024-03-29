#!/usr/bin/env python

"""Invoke Google Sheets API"""

import json
import argparse
import sys
from functools import reduce
from pathlib import Path
from typing import Literal, Optional

from google_auth import Services, send_request

service=Services.from_auth_context("GoogleAds").sheets_service
arg_data_path=Path("discovery/argfiles/google-sheets.json")
PathArgType = Optional[Literal["path", "parent"]]

get_invoke = lambda obj, key: getattr(obj, key)()

def parse_discovery_data(data_path: Path) -> dict:
    """Serialize discovery document data JSON into code

    In addition to loading the json, this substitutes a couple values in the
    arg data with functions.
    """
    arg_data=json.loads(data_path.read_text())
    for entity in arg_data:
        for endpoint in entity["endpoints"]:
            endpoint['library_path'] = entity['libraryPath']
            endpoint["library_func"] = lambda service, lib_path: getattr(
                reduce(get_invoke, lib_path, service),
                endpoint["name"])
            for arg in endpoint["args"]:
                arg=parse_arg_data(arg)
    return arg_data


def parse_arg_data(arg: dict) -> dict:
    """Transform certain fields of the argument data"""
    type_map = {
        "string": str,
        "integer": int,
        "boolean": bool,
        "FILE": argparse.FileType('r')
    }
    arg["data"]["type"] = type_map[arg["data"]["type"]]
    if arg["name"] == "body":
        arg["data"]["default"] = sys.stdin
    return arg


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
    endpoint_parser.set_defaults(
        library_func=endpoint['library_func'],
        library_path=endpoint['library_path'])
    for arg in endpoint['args']:
        endpoint_parser.add_argument(arg['name'], **arg.get('data', {}))


def init_parsers(parser: argparse.ArgumentParser) -> None:
    """Add an argument parser for each supported entity type"""
    parser.add_argument(
        '--auth-context',
        nargs='?',
        default='GoogleAds',
        help="Input which account the parser should authenticate against")
    subparser = parser.add_subparsers(
        description="Declare which entity type to operate on",
        required=True)

    for entity_type in parse_discovery_data(arg_data_path):
        add_entity_type_parser(subparser, entity_type)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create requests against the Google Sheets API")

    init_parsers(parser)
    cmd_args = parser.parse_args()
    context_key = cmd_args.auth_context
    service = Services.from_auth_context(context_key).sheets_service
    library_function = cmd_args.library_func(service, cmd_args.library_path)
    delattr(cmd_args, 'library_func')
    delattr(cmd_args, 'library_path')
    delattr(cmd_args, 'auth_context')

    if hasattr(cmd_args, 'body'):
        cmd_args.body = json.load(cmd_args.body)
    request = library_function(**vars(cmd_args))
    response = send_request(request)

    # The client library sometimes returns a bytes object instead of a
    # dictionary, so coerce it into a dict if needed
    # if hasattr(response, "decode"):
    #     response = json.loads(response)
    json.dump(response, sys.stdout)
    print()
