import json
import argparse
from google_auth import Services

if __name__ == "__main__":
    context_key = 'GoogleAds'
    serv = Services.from_auth_context(context_key).analytics_service

    parser = argparse.ArgumentParser(description='Call GA reporting API')
    parser.add_argument(
        "file",
        help='Report request body JSON',
        type=argparse.FileType('r')
    )
    args = parser.parse_args()
    body = json.load(args.file)

    response = serv.reports().batchGet(body=body).execute()
    print(json.dumps(response))
