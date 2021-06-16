#!/usr/bin/env jq

def order_args($context):
    (.parameters // {}) as $in |
    reduce (.parameterOrder // [])[] as $arg ({}; . + { ($arg): ($in[$arg]) }) +
    (if $context.has_body then {
        body: {
            description: "JSON file representing the \($context.entity) to \($context.method).",
            required: true,
            "type": "FILE"
        }
    } else {} end) +
    ($in | map_values((select(has("required") | not) | . + {required: false})));

["PUT", "POST", "PATCH"] as $http_with_body |
.resources.management.resources | 
with_entries(
    .key as $entity |
    .value.methods |
    with_entries(
        .key as $method |
        .value |
        .description as $description |
        (.httpMethod as $http | $http_with_body | index($http) != null) as $has_body |
        {entity: $entity, method: $method, has_body: $has_body} as $context |
        order_args($context) |
        to_entries |
        map(
            (if (.value.required) then .key else ("--" + .key) end) as $arg_name |
            {
                name: $arg_name,
                data: (.value | {help: .description, "type": .type} +
                (if ($method == "list" and .required) then 
                    {"type": "string", "nargs": "?", "default": "~all"}
                else {} end))
            }) |
        {key: $method, value: {
            name: $method,
            help: $description,
            args: map(.)
        }}
    ) |
    {key: $entity, value: {
        name: $entity,
        help: null,
        endpoints: map(.)
    }}
) | map(.)
