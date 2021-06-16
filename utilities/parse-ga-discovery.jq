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
        (.httpMethod as $http | $http_with_body | index($http) != null) as $has_body |
        {entity: $entity, method: $method, has_body: $has_body} as $context |
        order_args($context) |
        to_entries |
        map({
            name: (if (.value.required) then .key else ("--" + .key) end),
            data: (.value | {help: .description, "type": .type, required})
        }) |
        {key: $method, value: .}
    ) |
    {key: $entity, value: .}
)
