#!/usr/bin/env jq

include "ga-reporting";

(.reports | getJoinHeaders) as $headers |
joinReports($headers) |
{
    "sessions": ["sessions_a", "sessions_b"],
    "bounces": ["dummy_metric", "bounces"],
    "newUsers": ["dummy_metric"]
} as $key_mapping |
.[:3] |
["_rowid", "date", "adwordsCampaignId", "sessions_a", "sessions_b", 
    "dummy_metric", "bounces", "sessionDuration"] as $ordering |
rekeyObjects($headers; $ordering; $key_mapping) |
objectsToRows($ordering; @tsv)
