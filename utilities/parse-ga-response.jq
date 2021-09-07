#!/usr/bin/env jq -s

# Parse multiple GA reporting API responses.
# To-do:
#   Metric type handling
#   Add more standard transformations to API response fields

def flattenRow($row; $dateRanges):
  $row |
  ($dateRanges | length) as $dlen |
  if ($dlen) == 2 then [
    .dimensions + [$dateRanges[0]] + (.metrics[0].values | map(tonumber)),
    .dimensions + [$dateRanges[1]] + (.metrics[1].values | map(tonumber))][]
  else
    .dimensions + (.metrics[0].values | map(tonumber))
  end;

def flattenHeader($header; $dateRanges):
  $header |
  ($dateRanges | length) as $dlen |
  (.dimensions + (if ($dlen == 2) then ["dateRange"] else null end)) as $dimHeaders |
  (.metricHeader.metricHeaderEntries | map(.name)) as $metHeaders |
  ($dimHeaders + $metHeaders | map(sub("ga:"; "")));

[
  .[] |
  (.request.reportRequests[0].dateRanges | map(.startDate + " to " + .endDate)) as $dateRanges |
  .reports[] |
  flattenHeader(.columnHeader; $dateRanges) as $headers |
  .data |
  .rows // [] |
  map(
    flattenRow(.; $dateRanges) |
    [$headers, .] |
    transpose |
    map({key: .[0], value: .[1]}) |
    from_entries)
] |
flatten |
map(
  if (.date) then (.date |= .[:4] + "-" + .[4:6] + "-" + .[6:]) else . end |
  if (.yearMonth) then (.yearMonth |= .[:4] + "-" + .[4:]) else . end
)
