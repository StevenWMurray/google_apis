#!/usr/bin/env jq

# Parse page view data w/ query parameters
# Dimension orders:
#   - hostname
#   - pagePath

def sum(f): reduce .[] as $row (0; . + ($row|f));
def mapSum:
  . as $in |
  reduce (.[0] | keys)[] as $key
    ({}; . + {($key): ($in | sum(.[$key]))});

map((
  .pagePath |
  capture("(?<path>[^?]+)\\??(?<query>(?:[^&=]*=[^&]*&?)*)$") |
  .query |= (
    split("&") |
    map(capture("(?<key>[^&=]*)=(?<value>[^&]*)")) |
    from_entries)) as $pageData |
  .pagePath |= $pageData.path |
  (keys_unsorted | index("pagePath")) as $idx |
  to_entries |
  .[:$idx+1] + [{"key": "query", "value": $pageData.query}] + .[$idx+1:] |
  from_entries) |
group_by(.hostname) |
map({
  hostname: .[0].hostname,
  pages: (
    map(del(.hostname)) |
    group_by(.pagePath) |
    map(
    {
      pagePath: .[0].pagePath,
      query: map(del(.pagePath)),
      stats: (map(map_values(numbers)) | mapSum)
    })),
  stats: (map(map_values(numbers)) | mapSum)
})
