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

service=Services.from_auth_context("GoogleAds").analytics_management_service


acct_list_args = [
    {
        "name": "--max-results",
        "data": {
            "help": "Pagination: Max # of accounts to return",
            "type": int
        }
    }, {
        "name": "--start-index",
        "data": {
            "help": "Pagination: Which result to start on",
            "type": int
        }
    }
]

property_get_args = [
    {
        "name": "accountId",
        "data" : {
            "help": "GA Account ID to retrieve web property from",
        }
    }, {
        "name": "webPropertyId",
        "data": {
            "help": "GA property ID to retrieve",
        }
    }
]

property_insert_args = [
    {
        "name": "accountId",
        "data" : {
            "help": "GA Account ID to retrieve web property from",
        }
    }, {
        "name": "body",
        "data": {
            "help": "JSON file representing the web property to create",
            "nargs": '?',
            "type": argparse.FileType('r'),
            "default": sys.stdin
        }
    }
]

property_list_args = [
    {
        "name": "accountId",
        "data" : {
            "help": "GA Account ID to retrieve web property from",
            "nargs": '?',
            "default": "~all"
        }
    }, {
        "name": "--max-results",
        "data": {
            "help": "Pagination: Max # of properties to return",
            "type": int
        }
    }, {
        "name": "--start-index",
        "data": {
            "help": "Pagination: Which result to start on",
            "type": int
        }
    }
]

property_patch_args = [
    {
        "name": "accountId",
        "data" : {
            "help": "GA Account ID to retrieve web property from",
        }
    }, {
        "name": "webPropertyId",
        "data": {
            "help": "GA property ID to retrieve",
        }
    }, {
        "name": "body",
        "data": {
            "help": "JSON file representing the web property fields to update",
        }
    }
]

view_delete_args = [
    {
        "name": "accountId",
        "data": {
            "help": "GA Account ID to delete view from"
        }
    }, {
        "name": "webPropertyId",
        "data": {
            "help": "GA Web Property ID to delete view from"
        }
    }, {
        "name": "profileId",
        "data": {
            "help": "GA view to delete"
        }
    }
]
view_get_args = [
    {
        "name": "accountId",
        "data": {
            "help": "GA Account ID to retrieve view from"
        }
    }, {
        "name": "webPropertyId",
        "data": {
            "help": "GA Web Property ID to retrieve view from"
        }
    }, {
        "name": "profileId",
        "data": {
            "help": "GA view to retrieve"
        }
    }
]
view_insert_args = [
    {
        "name": "accountId",
        "data": {
            "help": "GA Account ID to retrieve view from"
        }
    }, {
        "name": "webPropertyId",
        "data": {
            "help": "GA Web Property ID to retrieve view from"
        }
    }, {
        "name": "body",
        "data": {
            "help": "JSON file representing the view to create",
            "type": argparse.FileType('r'),
            "default": sys.stdin
        }
    }
]
view_list_args = [
    {
        "name": "accountId",
        "data": {
            "help": "GA Account ID to retrieve view from",
            "nargs": '?',
            "default": "~all"
        }
    }, {
        "name": "webPropertyId",
        "data": {
            "help": "GA Web Property ID to retrieve view from",
            "nargs": '?',
            "default": "~all"
        }
    }, {
        "name": "--max-results",
        "data": {
            "help": "Pagination: Max # of properties to return",
            "type": int
        }
    }, {
        "name": "--start-index",
        "data": {
            "help": "Pagination: Which result to start on",
            "type": int
        }
    }
]
view_patch_args = [
    {
        "name": "accountId",
        "data": {
            "help": "GA Account ID to retrieve view from"
        }
    }, {
        "name": "webPropertyId",
        "data": {
            "help": "GA Web Property ID to retrieve view from"
        }
    }, {
        "name": "profileId",
        "data": {
            "help": "GA View ID to update"
        }
    }, {
        "name": "body",
        "data": {
            "help": "JSON file representing the view fields to update"
        }
    }
]

adwords_link_delete_args = [
    {
        "name": "accountId",
        "data": {
            "help": "ID of the account which the given web property belongs to"
        }
    }, {
        "name": "webPropertyId",
        "data": {
            "help": "Web property Google Ads link ID"
        }
    }, {
        "name": "webPropertyAdWordsLinkId",
        "data": {
            "help": "Web property ID to delete the Google Ads link for",
        }
    }
]

adwords_link_get_args = [
    {
        "name": "accountId",
        "data": {
            "help": "ID of the account which the given web property belongs to"
        }
    }, {
        "name": "webPropertyId",
        "data": {
            "help": "Web property Google Ads link ID"
        }
    }, {
        "name": "webPropertyAdWordsLinkId",
        "data": {
            "help": "Web property ID to retrieve the Google Ads link for",
        }
    }
]

adwords_link_insert_args = [
    {
        "name": "accountId",
        "data": {
            "help": "ID of the account which the given web property belongs to"
        }
    }, {
        "name": "webPropertyId",
        "data": {
            "help": "Web property Google Ads link ID"
        }
    }, {
        "name": "body",
        "data": {
            "help": "Google Ads Links resource JSON to create",
            "type": argparse.FileType('r'),
            "default": sys.stdin
        }
    }
]

adwords_link_list_args = [
    {
        "name": "accountId",
        "data": {
            "help": "ID of the account which the given web property belongs to",
            "nargs": '?',
            "default": "~all"
        }
    }, {
        "name": "webPropertyId",
        "data": {
            "help": "Web property Google Ads link ID",
            "nargs": '?',
            "default": "~all"
        }
    }, {
        "name": "--max-results",
        "data": {
            "help": "Pagination: Max # of properties to return",
            "type": int
        }
    }, {
        "name": "--start-index",
        "data": {
            "help": "Pagination: Which result to start on",
            "type": int
        }
    }
]

adwords_link_patch_args = [
    {
        "name": "accountId",
        "data": {
            "help": "ID of the account which the given web property belongs to"
        }
    }, {
        "name": "webPropertyId",
        "data": {
            "help": "Web property Google Ads link ID"
        }
    }, {
        "name": "webPropertyAdWordsLinkId",
        "data": {
            "help": "Web property ID to retrieve the Google Ads link for",
        }
    }, {
        "name": "body",
        "data": {
            "help": "Modified fields to update on requested Google Ads link",
        }
    }
]

entity_types = [
    {
        "name": "accounts",
        "help": "Operations on accounts",
        "endpoints": [
            {
                "name": "list",
                "help": "List data on all accounts",
                "args": acct_list_args,
                "library_func": service.management().accounts().list
            }
        ]
    },
    {
        "name": "accountSummaries",
        "help": "Get data on full account hierarchies",
        "endpoints": [
            {
                "name": "list",
                "help": "List summary data on full account hierarchy",
                "args": acct_list_args,
                "library_func": service.management().accountSummaries().list
            }
        ]
    },
    {
        "name": "webproperties",
        "help": "Operations on properties",
        "endpoints": [
            {
                "name": "get",
                "help": "Get data for one web property",
                "args": property_get_args,
                "library_func": service.management().webproperties().get
            }, {
                "name": "insert",
                "help": "Create a new web property",
                "args": property_insert_args,
                "library_func": service.management().webproperties().insert
            }, {
                "name": "list",
                "help": "List data on all web properties",
                "args": property_list_args,
                "library_func": service.management().webproperties().list
            }, {
                "name": "patch",
                "help": "Update web property",
                "args": property_patch_args,
                "library_func": service.management().webproperties().patch
            }
        ]
    }, {
        "name": "profiles",
        "help": "Operations on profiles (views)",
        "endpoints": [
            {
                "name": "delete",
                "help": "Delete an existing view",
                "args": view_delete_args,
                "library_func": service.management().profiles().delete
            }, {
                "name": "get",
                "help": "Get data for one view",
                "args": view_get_args,
                "library_func": service.management().profiles().get
            }, {
                "name": "insert",
                "help": "Create a new view",
                "args": view_insert_args,
                "library_func": service.management().profiles().insert
            }, {
                "name": "list",
                "help": "List data on all views",
                "args": view_list_args,
                "library_func": service.management().profiles().list
            }, {
                "name": "patch",
                "help": "Update requested view",
                "args": view_patch_args,
                "library_func": service.management().profiles().patch
            }
        ]
    }, {
        "name": "webPropertyAdWordsLinks",
        "help": "Operations on Google Ads links",
        "endpoints": [
            {
                "name": "delete",
                "help": "Delete an existing Google Ads link",
                "args": adwords_link_delete_args,
                "library_func": service.management().webPropertyAdWordsLinks().delete
            }, {
                "name": "get",
                "help": "Get data for one Google Ads link",
                "args": adwords_link_get_args,
                "library_func": service.management().webPropertyAdWordsLinks().get
            }, {
                "name": "insert",
                "help": "Create a new Google Ads link",
                "args": adwords_link_insert_args,
                "library_func": service.management().webPropertyAdWordsLinks().insert
            }, {
                "name": "list",
                "help": "List data on multiple Google Ads links",
                "args": adwords_link_list_args,
                "library_func": service.management().webPropertyAdWordsLinks().list
            }, {
                "name": "patch",
                "help": "Update an existing Google Ads link",
                "args": adwords_link_patch_args,
                "library_func": service.management().webPropertyAdWordsLinks().patch
            }
        ]
    }
]


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

    for n in range(0, 5):
        try:
            return request.execute()

        except HttpError as error:
            if error.resp.reason in retryable_errors:
                time.sleep((2 ** n) + random.random())
            else:
                break

    print(json.dumps(error), file=sys.stderr)


def init_parsers(parser: argparse.ArgumentParser) -> None:
    """Add an argument parser for each supported API entity type"""
    subparser = parser.add_subparsers(
        description="Declare which GA entity type to invoke",
        required=True)

    for entity_type in entity_types:
        add_entity_type_parser(subparser, entity_type)


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
