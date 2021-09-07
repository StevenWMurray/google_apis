#!/usr/bin/env python
"""Command line tool for making requests to the GA Management API"""

import json
import argparse
import time
import random
import sys
from functools import reduce
from pathlib import Path
from typing import Literal, Optional

from google_auth import Services, send_request

service=Services.from_auth_context("GoogleAds").tagmanager_service
arg_data_path=Path("discovery/argfiles/tagmanager.json")
PathArgType = Optional[Literal["path", "parent"]]

get_invoke = lambda obj, key: getattr(obj, key)()

class ApiPath:
    """API Path aware argument & client library handling"""
    def __init__(self, service: Services, lib_path: list[str], endpoint: str,
                 path_var: PathArgType) -> None:
        """Setup the correct client library object & path variable parser"""
        self.lib_path = lib_path
        self.path_var = path_var
        self.endpoint = endpoint
        self.create_library_function(service)
        self.prepare_path_parser()

    def create_library_function(self, service: Services) -> None:
        """Create library function handler"""
        get_invoke = lambda obj, key: getattr(obj, key)()
        self.library_function = getattr(
            reduce(get_invoke, self.lib_path, service), self.endpoint)

    def prepare_path_parser(self):
        """Build the "parse path" function

        Need to:
            - Decide whether to trim the libpath
            - Zip the libPath objects w/ parsed args
        """
        def parse_path_args(cli_args: argparse.Namespace) -> None:
            """Compile separate CLI arguments into a single path arg"""
            mangleName = lambda instr: instr[:-1] + "Id"
            path_args = [getattr(cli_args, mangleName(arg)) for arg in lib_path]
            for arg in lib_path:
                delattr(cli_args, mangleName(arg))
            path = "/".join("/".join(e) for e in zip(lib_path, path_args))
            setattr(cli_args, self.path_var, path)

        if self.path_var is None:
            self.parse_path_args = lambda x: None
            return
        if self.path_var == "path":
            lib_path = self.lib_path
        elif self.path_var == "parent":
            lib_path = self.lib_path[:-1]
        self.parse_path_args = parse_path_args

    def do_special_arg_handling(self, cli_args):
        """Subparser-specific handling of parsed CLI arguments

        Currently, only includes 'folders > move_entities_to_folder' commands.
        This splits the 3 "entityId" fields, input as comma-separated strings,
        into lists, as expected by the client library.
        """
        if self.endpoint == "move_entities_to_folder":
            for arg in ["tagId", "triggerId", "variableId"]:
                argval = getattr(cli_args, arg)
                if argval is not None:
                    setattr(cli_args, arg, argval.split(","))


type_map = {
    "string": str,
    "integer": int,
    "boolean": bool,
    "FILE": argparse.FileType('r')
}

def parse_arg_data(data_path: Path) -> dict:
    """Serialize arg data JSON into code

    In addition to loading the json, this substitutes a couple values in the
    arg data with functions.
    """
    arg_data = json.loads(data_path.read_text())
    for entity in arg_data:
        for endpoint in entity["endpoints"]:
            endpoint["path_handler"] = ApiPath(
                service,
                entity["libraryPath"],
                endpoint["name"],
                endpoint["pathVar"])
            for arg in endpoint["args"]:
                arg["data"]["type"] = type_map[arg["data"]["type"]]
                if arg["name"] == "body":
                    arg["data"]["nargs"] = "?"
                    arg["data"]["default"] = sys.stdin
    return arg_data


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
    endpoint_parser.set_defaults(path_handler=endpoint['path_handler'])
    for arg in endpoint['args']:
        endpoint_parser.add_argument(arg['name'], **arg.get('data', {}))


def init_parsers(parser: argparse.ArgumentParser) -> None:
    """Add an argument parser for each supported entity type"""
    subparser = parser.add_subparsers(
        description="Declare which entity type to operate on",
        required=True)

    for entity_type in parse_arg_data(arg_data_path):
        add_entity_type_parser(subparser, entity_type)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create requests against the GA Management API")

    init_parsers(parser)
    cmd_args = parser.parse_args()
    path_handler = cmd_args.path_handler
    path_handler.parse_path_args(cmd_args)
    path_handler.do_special_arg_handling(cmd_args)
    delattr(cmd_args, 'path_handler')

    if hasattr(cmd_args, 'body'):
        cmd_args.body = json.load(cmd_args.body)
    request = path_handler.library_function(**vars(cmd_args))
    response = send_request(request)

    # The client library sometimes returns a bytes object instead of a
    # dictionary, so coerce it into a dict if needed
    if hasattr(response, "decode"):
        response = json.loads(response)
    json.dump(response, sys.stdout)
    print()
