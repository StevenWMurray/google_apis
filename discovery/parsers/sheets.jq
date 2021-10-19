#!/usr/bin/env jq

{
  "valueInputOption": "RAW",
  "valueRenderOption": "UNFORMATTED_VALUE",
  "dateTimeRenderOption": "FORMATTED_STRING",
  "responseValueRenderOption": "UNFORMATTED_VALUE",
  "responseDateTimeRenderOption": "FORMATTED_STRING"
} as $defaultOpts |
(
  [paths(.methods?)] |
  map(.[-1]) |
  map({key: ., value: "Operations on \(.)."}) |
  from_entries
) as $resourceDescriptions |
.schemas as $schemas |
([
  path(.. | select(.resources?).resources | .[]) |
  {"key": .[-1], "value": map(select(. != "resources"))}
] | from_entries) as $libraryPath |
[.. | select(.resources?).resources | map_values(.methods)] |
add |
to_entries | map({
  name: .key,
  help: $resourceDescriptions[.key],
  libraryPath: $libraryPath[.key],
  endpoints: (
    .key as $etype |
    .value |
    to_entries |
    map(
      .key as $method |
      .value | {
        name: $method,
        help: .description,
        args: ((
          (.parameterOrder | map({(.): null}) | add) + .parameters |
          to_entries |
          map(
            .key as $name |
            .value |
            {
              name: (if (.required | not) then "--" + $name else $name end),
              data: (
                {
                  help: .description,
                  "type": .["type"],
                  nargs: (
                    if (.repeated) then "+"
                    else null end
                  ),
                  default: $defaultOpts[$name]?
                } | 
                reduce keys[] as $k (.; if .[$k] == null then del(.[$k]) else . end))
            }
          )) + (
            if .request then [{
              name: "body", 
              data: {
                help: "JSON file representing the \(.request["$ref"]) to \($method)",
                "type": "FILE",
                nargs: "?",
                default: "sys.stdin"
            }}]
            else null end)
          )
      }
    )
  )
})

# For each $Resource:
#   - /// Add Description
# For each $Resource.$Method:
#   - /// Build help string
#   - /// Parse Path / Parent for variables
#   - /// Add all other parameters
#   - /// Add "body" parameter if there's a request > "$ref" object
#     - /// Fetch description from the linked schema
# [scan("\\{([^}]+)\\}")[] | sub("sId$"; "Id")]
