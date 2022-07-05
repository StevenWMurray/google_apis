#!/usr/bin/env jq

# Builds ESP GA goal request data

group_by(.viewId) |
map(
  .[0].viewId as $viewId |
  group_by(.category) |
  map(
    .[0].category as $category |
    reduce .[] as $goal (
      {alias: $category, apiNames: []};
      .apiNames += [$goal.gaMetric]) |
    .apiNames |= join("+") |
    .apiNames + "(alias=" + .alias + ")") |
  {
    viewId: $viewId,
    dates: $dates,
    dimensions: ($ARGS.named["dimensions"] // "ga:yearMonth ga:adwordsCampaignID"),
    dimensionFilters: "ga:adwordsCustomerID == 2187933501",
    metrics: (. | "ga:sessions " + join(" "))
  })[]
