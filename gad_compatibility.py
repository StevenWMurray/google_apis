from functools import reduce
from typing import Iterable
from google_auth import Services

serv = Services.from_auth_context('GoogleAds').ads_client().get_service('GoogleAdsFieldService')

def field_compatibility(field_name: str, service=serv) -> set[str]:
    resp = serv.get_google_ads_field(resource_name=f"googleAdsFields/{field_name}")
    return set(resp.selectable_with)

def set_intersection(collection: Iterable[set]) -> set:
    """Return the intersection of all sets in the collection"""
    def _intersect(acc: set, el: set) -> set:
        return acc.intersection(el)

    return reduce(_intersect, collection)

def format_output(table_name: str, metric_name: str):
    return f"{table_name}\t{metric_name.split('.')[1]}"


if __name__ == "__main__":
    entities = ['customer', 'campaign', 'ad_group', 'video']
    segments = ['date', 'hour', 'device', 'ad_network_type', 'slot', 'ad_destination_type', 'click_type']

    compat_dict = {
        entity: field_compatibility(entity) for entity in entities
    } | {
        segment: field_compatibility(f"segments.{segment}") for segment in segments
    }

    table_keys = {
        'AD_GROUP_DATA': ['ad_group', 'date'],
        'CAMPAIGN_DATA': ['campaign', 'date'],
        'VIDEO_AD_DATA': ['video', 'ad_group', 'campaign', 'date', 'device', 'ad_network_type'],
        'AD_GROUP_HOURLY_DATA': ['ad_group', 'date', 'hour', 'device', 'ad_network_type'],
        'CAMPAIGN_HOURLY_DATA': ['campaign', 'date', 'hour', 'device', 'ad_network_type'],
    }

    for table_name in table_keys:
        fields = set_intersection(compat_dict[el] for el in table_keys[table_name])
        for field_name in fields:
            if field_name.startswith('metrics.'):
                print(format_output(table_name, field_name))
