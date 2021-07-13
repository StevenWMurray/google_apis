#!/usr/bin/env jq

# All input variables should be named exactly as described below
# Required variables:
#   - viewId
#   - dates
#   - dimensions
#   - metrics
#
# Optional arguments (NYI):
#   - dimensionFilters
#   - metricFilters

def parseOpts(constructor):
    [
        capture("(?<arg>[[:alnum:]:\\+\\-\\*\\/]+)(?<opts>[(][^)]*[)])?"; "g") |
        {
            arg, opts: ([
                .opts |
                if . == null then "" else .[1:-1] end |
                capture("(?<key>[^ =]+)=(?<value>[^ \"']+|\"[^\"]+\"|'[^']+')"; "g")
            ] | from_entries )
        } |
        constructor |
        with_entries(if .value==null then empty else . end)
    ];

def splitFilters($key):
    if $ARGS.named[$key] then (
        $ARGS.named[$key] |
        (
            split(" -o ") as $osplit |
            split(" -a ") as $asplit |
            if ((($osplit | length) > 1) and (($asplit | length) > 1)) then
                error("Filter clauses can only use one of -o or -a chaining, not both")
            elif (($asplit | length) > 1) then
                {operator: "AND", filters: $asplit}
            else
                {operator: "OR", filters: $osplit}
            end
        )
    ) else null end;

    
{
    "~=": "REGEXP",
    "^=": "BEGINS_WITH",
    "$=": "ENDS_WITH",
    "*=": "PARTIAL",
    "==": "EXACT",
    "-eq": "NUMERIC_EQUAL",
    "-gt": "GREATER_THAN",
    "-lt": "LESS_THAN",
    "-in": "IN_LIST"
} as $dFilterOpTypes |
$dimensions |
parseOpts(
    { name: .arg, histogramBuckets: (.opts.buckets[1:-1]? | split(" ")? // null) }
) as $dimensions |
$metrics |
parseOpts(
    { expression: .arg, alias: .opts.alias, formattingType: .opts.format }
) as $metrics |
$dates |
split(" -o ")[:2] |
map(split(" -to ") | {startDate: .[0], endDate: .[1]}) as $dates |
splitFilters("dimensionFilters") |
if . then
    .filters |= map(
        capture(
            "(?:(?<not>-not) )?(?:(?<case>-[iI]) )?(?<dim>[a-zA-Z0-9_:]+) ?" +
            "(?<op>~=|\\^=|\\$=|\\*=|==|-eq|-lt|-gt|-in) ?(?<expr>.*$)"
        ) |
        {
            dimensionName: .dim,
            "not": (.not != null),
            operator: $dFilterOpTypes[.op],
            expressions: (
                if .op != "-in" then
                    [.expr | gsub("\""; "")]
                else
                    (.expr[1:-1] | [scan("([^ \"]+|\"[^\"]+\")")[] | gsub("\""; "")])
                end
            ),
            caseSensitive: (.case == "-I")
        }
    )
else null end |
. as $dimensionFilters |
{
    viewId: $viewId,
    dateRanges: $dates,
    samplingLevel: ($ARGS.named["sampling"] // "LARGE"),
    dimensions: $dimensions,
    dimensionFilterClauses: (if ($dimensionFilters == null) then null else $dimensionFilters end),
    metrics: $metrics,
    pageSize: ($ARGS.named["pageSize"] // 100000),
    hideValueRanges: ($ARGS.named["hideValueRanges"] // false)
} |
with_entries(if .value == null then empty else . end)
