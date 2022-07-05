#!/usr/bin/env jq

flatten |
map(if (.campaign == "HITRUST Academy®") then (.campaign |= "HITRUST Academy® De-ID") else . end) |
map(if (.campaign | test("Display")) then (.channelGrouping |= "Display") else . end) |
group_by(.yearMonth + .campaign) |
map(add) |
map(
  .timePeriod = .yearMonth |
  .campaignOwner = (.campaign | if startswith("WD_") then "WD" elif (. == "(not set)") then "(not set)" else "HT Old" end) |
  .channel = (if (.campaignOwner == "(not set)") then "(not set)" elif (.campaignOwner == "HT Old") then "Mixed" else .channelGrouping end) |
  delpaths([["yearMonth"], ["channelGrouping"], ["source"], ["campaign"]])
) |
group_by(.campaignOwner + .channel + .timePeriod) | 
map(
  (.[0] | {timePeriod, campaignOwner, channel}) as $dims |
  map({impressions, adClicks, adCost, sessions, sessionDuration, pageviews}) |
  map(to_entries) |
  flatten |
  group_by(.key) |
  map({key: .[0].key, value: (map(.value) | add)}) |
  from_entries |
  $dims + .
) | 
flatten |
map(
  {
    month: .timePeriod,
    campaignOwner,
    channel,
    investment: .adCost,
    impressions,
    clicks: .adClicks,
    "Avg. CPC": (if .adClicks then .adCost / .adClicks else null end),
    "CTR": (if .impressions then .adClicks / .impressions else null end),
    sessions,
    "Avg. Session Duration": (.sessionDuration / .sessions),
    "Avg. Pageviews / Session": (.pageviews / .sessions) }
) |
# (.[0] | keys_unsorted), (.[] | map(.)) |
# @csv
.
#map({timePeriod: .[0].timePeriod, channel: .[0].channel, campaign: .[0].campaign, sessions: (map(.sessions) | add)})
