.reports[] |
(
    .columnHeader.dimensions |
    map(sub("ga:"; ""))
) as $dimHeaders | 
(
    .columnHeader.metricHeader.metricHeaderEntries |
    map(.name | sub("ga:"; ""))
) as $metHeaders |
($dimHeaders + $metHeaders) as $headers |
(.data.rows | map(.dimensions + (.metrics[0].values | map(tonumber)))) |
$headers, .[] |
@csv
