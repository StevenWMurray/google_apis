#!/usr/bin/env jq

# Require that $viewId, $startDate, $endDate args were passed in
# Require a $goals array, with name & category

# Get count of metrics to pull
# Assume all goals + sessions, newUsers, bounces, uniquePageviews, sessionDuration


($goals[0] | map(select(.viewId == $viewId))) as $goals | 
(
    [
        "ga:newUsers",
        "ga:sessions",
        "ga:bounces",
        "ga:uniquePageviews",
        "ga:sessionDuration"
    ] |
    map({"expression": .})
) as $baseMetrics |
($goals | map({ expression: .gaMetric })) as $goalMetrics | 
.reportRequests[0].viewId |= $viewId |
.reportRequests[0].dateRanges[0].startDate |= $startDate |
.reportRequests[0].dateRanges[0].endDate |= $endDate |
.reportRequests += .reportRequests |
.reportRequests[0].metrics |= $baseMetrics |
.reportRequests[1].metrics |= $goalMetrics
