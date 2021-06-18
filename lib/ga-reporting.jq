#!/usr/bin/env jq

# Library file containing useful functions for constructing parsers of GA API responses

# To-do:
#   - Add sampling handling
#   - Add "isGolden" check

def getHeaders($useRowId):
    ########################################
    # Read the column headers from the API response, stripping leading 'ga:' prefixes
    ########################################
    .columnHeader | 
    (if $useRowId then ["_rowid"] else [] end) + 
        .dimensions + 
        (.metricHeader.metricHeaderEntries | map(.name)) |
    map(sub("^ga:"; ""));

def getJoinHeaders:
    ########################################
    # Produces the output column orderings of the joined reports
    ########################################
    map(getHeaders(true)) | 
    flatten |
    reduce .[] as $item ([]; if any(. == $item) then . else . += [$item] end);


def rowsToObjects(value_format):
    ########################################
    # Converts data from column/row format to a list of objects 
    #
    # Arguments:
    #   - value_format: a filter that applies to each row in the input. Used to
    #       reformat column values in the API response, e.g. date formatting
    ########################################
    (.columnHeader.dimensions | index("ga:date")) as $date_index |
    getHeaders(true) as $cols |
    [
        .data.rows |
        map(value_format) | 
        map(
            [(.dimensions | join(";"))] + 
            .dimensions + 
            (.metrics[0].values | map(tonumber))
        )[] |
        [$cols, .] |
        transpose |
        map({"key": .[0], "value": .[1]}) |
        from_entries
    ];

def joinReports($headers):
    ########################################
    # Merge multiple reports via an outer join
    ########################################
    .reports |
    map(if (.data | has("rows")) then . else empty end) |
    ($headers | index("date") - 1) as $date_index |  # -1 b/c of extra row-id column
    map(rowsToObjects(
        .dimensions[$date_index] |= (strptime("%Y%m%d") | strftime("%Y-%m-%d")))
    ) | 
    flatten |
    group_by(._rowid) |
    map(add) |
    (reduce $headers[] as $item ({}; .[$item] |= null)) as $null_row |
    map($null_row + .);

def objectsToRows($ordering; format):
    ########################################
    # Converts from object to the requested tabular form
    # Arguments:
    #   - format: desired output format filter, e.g. @tsv
    ########################################
    map(del(._rowid)) |
    # ($ordering[1:]) as $ordering |
    map(. as $row | $ordering | map($row[.])) |
    $ordering, .[] |
    format;

def rekeyObjects($headers; $ordering; $key_mapping):
    ########################################
    # Alter column names, aggregating columns mapped to the same names
    #
    # Arguments:
    #   - stdin: A list of objects to rekey
    #   - mapping: object in {OLD_KEY: [NEW_KEY...]} format
    ########################################
    ($key_mapping | keys) as $keylist |
    ($key_mapping | to_entries) as $key_mapping |
    . as $rows |
    $headers |
    map(. as $key | $keylist | if index($key) then empty 
        else {key: $key, value: [$key]} end) |
    ($key_mapping + .) as $key_mapping |
    (reduce $ordering[] as $item ({}; .[$item] |= null)) as $null_row |
    $rows |
    map(
        to_entries |
        . as $row |
        map(
            . as $entry |
            [$key_mapping[] | select(.key == $entry.key)] as $mapping | 
            .value as $e_val |
            ($mapping[] | .value) |
            reduce .[] as $keyval ([]; . + [{"key": $keyval, "value": $e_val}])
        ) |
        flatten |
        group_by(.key) |
        map(
            . as $root |
            {"key": (.[0] | .key), "value": null} |
            reduce $root[] as $item (.; .value += $item.value)) |
        $null_row + from_entries
    );
