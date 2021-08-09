#!/usr/bin/env bash

# Pull a GA report
# Arguments:
#   - The GA View ID to pull from
#   - A file containing the report request data
# Output:
#   - On Success: A JSONL containing every API response
#   - On Error: The error stack trace from the failed API request
# Returns:
#   - 0 on success
#   - Non-0 on any failed request

jq '{viewId: (. | tostring)}' <<< "$1" \
| jq -s 'add' - "$2" \
| jq -f utilities/build-ga-request.jq \
| python ga-reporting.py -
