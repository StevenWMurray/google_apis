import argparse
from google_auth import Services

serv = Services.from_auth_context('GoogleAds')
client = serv.ads_client
gads = serv.ads_service('GoogleAdsService')

def init_parsers(parser: argparse.ArgumentParser) -> None:
    """Command line argument parser

    Expects a customer ID to pull from, a service to send a query to,
    and a query to execute
    """
    parser.add_argument(
        '--customer-id',
        help='Customer ID to query',
        type=str)
    # parser.add_argument(
    #     'query',
    #     help='GAQL query to send to server',
    #     type=str)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create requests against the Google Ads API")
    init_parsers(parser)
    cmd_args = parser.parse_args()
