#!/usr/bin/env jq

# Transforms a list of heterogenous flat objects into tabular form

(
  map(to_entries | map(.key)) |
  flatten |
  reduce .[] as $key ([]; if any(. == $key) then . else . + [$key] end)
) as $keys |
map([getpath([$keys] | transpose | .[])]) |
$keys, .[] |
@csv
