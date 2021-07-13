#!/usr/bin/env jq

# Naive single report parser. Assumes a single report, produces a single csv.
# To-do:
#   Alias handling
#   Metric type handling
#   Multi-date range handling
#   Sampling / isDataGolden handling

.reports[0] |
.columnHeader.dimensions as $dimHeaders |
(.columnHeader.metricHeader.metricHeaderEntries | map(.name)) as $metHeaders |
($dimHeaders + $metHeaders) as $headers |
.data |
.rows |
map(.dimensions + (.metrics[0].values | map(tonumber))) |
$headers, .[] |
@csv
