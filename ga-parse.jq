#!/usr/bin/env jq

def getHeaders($f):
    $f.columnHeader | 
    ["_rowid"] + .dimensions + (.metricHeader.metricHeaderEntries | map(.name)) |
    map(sub("^ga:"; ""));

def toObjects($f):
    ($f.columnHeader.dimensions | index("ga:date")) as $date_index |
    getHeaders($f) as $cols |
    [
        $f.data.rows |
        map(.dimensions[$date_index] |= (strptime("%Y%m%d") | strftime("%Y-%m-%d"))) |
        map([(.dimensions | join(";"))] + .dimensions + (.metrics[0].values | map(tonumber)))[] |
        [$cols, .] |
        transpose |
        map({"key": .[0], "value": .[1]}) |
        from_entries
    ];

# Add sampling / isGolden handling
.reports |
(
    map(getHeaders(.)) | 
    flatten |
    reduce .[] as $item ([]; if any(. == $item) then . else . += [$item] end)
) as $headers |
map(toObjects(.)) | 
flatten |
group_by(._rowid) |
map(add) |
(reduce $headers[] as $item ({}; .[$item] |= null)) as $null_row |
map($null_row + .) |
map(del(._rowid)) |
[$headers[1:]] + [to_entries[] | (.value | to_entries | map(.value))] |
map(@tsv)[]
