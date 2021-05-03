import json
from google_auth import Services

def list_account_summaries(serv: Services):
    """Produce a list of accounts"""
    return serv.management().accountSummaries().list().execute()

if __name__ == "__main__":
    context_key = 'GoogleAds'
    serv = Services.from_auth_context(context_key).analytics_management_service
    print(json.dumps(list_account_summaries(serv)))
