from itertools import chain
from functools import reduce
from typing import Iterable
from google_auth import Services

serv = Services.from_auth_context('GoogleAds').ads_client().get_service('GoogleAdsFieldService')

def field_compatibility(field_name: str, is_entity_type: bool, service=serv) -> set[str]:
    resp = serv.get_google_ads_field(resource_name=f"googleAdsFields/{field_name}")
    if is_entity_type:
        return set(resp.segments).union(resp.metrics)
    return set(resp.selectable_with)

def set_intersection(collection: Iterable[set]) -> set:
    """Return the intersection of all sets in the collection"""
    def _intersect(acc: set, el: set) -> set:
        return acc.intersection(el)

    return reduce(_intersect, collection)

def initcap(instr: str):
    return instr[0].upper() + instr[1:].lower()

def format_output(table_name: str, field_type: str, field_name: str) -> str:
    prefix = field_name.split('.')[0]
    field_prefix = f"{prefix}_" if prefix not in ('segments', 'metrics') else ''
    field_suffix = '_'.join(field_name.split('.')[1:])
    field_ref = field_prefix + field_suffix
    ods_name = field_ref.upper()

    if table_name == 'VIDEO_AD_DATA':
        matillion_name = field_name.replace('_', '')
    else:
        matillion_name = ''.join(initcap(fld) for fld in field_ref.split('_'))

    return f"{table_name}\t{field_type}\t{field_name}\t{matillion_name}\t{ods_name}"


if __name__ == "__main__":
    entities = {'customer', 'campaign', 'ad_group', 'video'}
    segments = {'date', 'hour', 'device', 'ad_network_type', 'slot', 'ad_destination_type', 'click_type'}

    compat_dict = {
        entity: field_compatibility(entity, True) for entity in entities
    } | {
        segment: field_compatibility(f"segments.{segment}", False) for segment in segments
    }

    attr_table_keys = {
        'CUSTOMER_ATTRIBUTE': ['customer']
    }

    data_table_keys = {
        'AD_GROUP_DATA': ['date', 'ad_group'],
        'CAMPAIGN_DATA': ['date', 'campaign'],
        'VIDEO_AD_DATA': ['date', 'video', 'campaign', 'ad_group', 'device', 'ad_network_type'],
        'AD_GROUP_HOURLY_DATA': ['date', 'hour', 'ad_group', 'device', 'ad_network_type'],
        'CAMPAIGN_HOURLY_DATA': ['date', 'hour', 'campaign', 'device', 'ad_network_type'],
    }

    print("Table Name\tField Type\tGAD Field Name\tGAD Field Ref")
    for table_name in attr_table_keys:
        fields = set_intersection(compat_dict[el] for el in attr_table_keys[table_name])
        for entity in attr_table_keys[table_name]:
            for field_name in sorted(filter(lambda name: name.startswith(entity), fields)):
                print(format_output(table_name, "Attribute", f"{entity}.field_name"))


    for table_name in data_table_keys:
        fields = set_intersection(compat_dict[el] for el in data_table_keys[table_name])
        for field_name in data_table_keys[table_name]:
            if field_name in entities:
                print(format_output(table_name, "Attribute", f"{field_name}.resource_name"))
            else:
                print(format_output(table_name, "Segment", f"segments.{field_name}"))
        for field_name in sorted(filter(lambda name: name.startswith('metrics.'), fields)):
            if not field_name.startswith('metrics.auction_insight'):
                print(format_output(table_name, "Metric", field_name))
