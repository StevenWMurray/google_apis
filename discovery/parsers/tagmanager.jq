#!/usr/bin/env jq

{
  Template: "CustomTemplate",
  VersionHeader: "ContainerVersionHeader",
  Version: "ContainerVersion"
} as $customSchemaMapping |
.schemas as $schemas |
([
  path(.. | select(objects and has("resources")).resources | .[]) |
  {"key": .[-1], "value": map(select(. != "resources"))}
] | from_entries) as $libraryPath |
[.. | select(objects and has("resources")).resources | map_values(.methods)] |
add | (
  keys |
  map(
    (split("_") | map((.[:1] | ascii_upcase) + .[1:]) | join(" ")) as $formattedName |
    # {
    #   "key": .,
    #   "value": "Actions on \($formattedName)"
    # } | from_entries) as $resourceDescriptions
    {
      "key": .,
      "value": (
        $formattedName |
        gsub(" "; "")[:-1] |
        if (in($schemas)) then . else $customSchemaMapping[.] end |
        (. as $name | $schemas | .[$name]).properties |
        map_values({type, help: .description}))}) |
  from_entries
) as $bodyArgs | (
  keys |
  map(
    (split("_") | map((.[:1] | ascii_upcase) + .[1:]) | join(" ")) as $formattedName |
    {
      "key": .,
      "value": "Actions on \($formattedName)"
    }) |
  from_entries
) as $resourceDescriptions |
to_entries | map({
  name: (.["key"] | split("_") | map((.[:1] | ascii_upcase) + .[1:]) | join("") | (.[:1] | ascii_downcase) + .[1:]),
  help: ($resourceDescriptions[.key]),
  libraryPath: $libraryPath[.key],
  endpoints: (
    .key as $etype |
    .value |
    to_entries |
    map(
    .key as $method |
    .value | {
      "name": $method,
      "help": .description,
      "pathVar": (if .parameters.path then "path" else (if .parameters.parent then "parent" else null end) end),
      "args": (
        (
          .flatPath |
          [scan("\\{([^}]+)\\}")[] | sub("sId$"; "Id")] |
          map(
            . as $argName |
            $bodyArgs[$etype] |
            (.[$argName] // {type: "string"}) as $argData |
            {name: $argName, data: $argData})
        ) + (
          # For parameter in .parameterPath: $pdata
          .parameters |
          del(.path // .parent) |
          to_entries |
          map(
            (if (.value.location == "query") then false else true end) as $isRequired |
            {
              name: .key,
              data: ( .value | { help: .description, type })
            } |
            .name |= (if ($isRequired | not) then "--" + . else . end))
        ) + (
          if .request then
            [{name: "body", data: {
              help: "JSON file representing the \(.request["$ref"]) to \($method)",
              "type": "FILE"
            }}]
          else null end)
      )
    })
  )
})

# Current: $Resource.$Method
# For each $Resource:
#   - /// Add Description
# For each $Resource.$Method:
#   - /// Build help string
#   - /// Parse Path / Parent for variables
#   - /// Add all other parameters
#   - /// Add "body" parameter if there's a request > "$ref" object
#     - /// Fetch description from the linked schema
# [scan("\\{([^}]+)\\}")[] | sub("sId$"; "Id")]
