#!/usr/bin/env bash

# Script for ESP GA reporting
# Arguments:
#   - The start & end dates of the report request in ISO format

export GOAL_FILE="data/esp/categorized_goals.json"
export OUT_DIR="data/esp/${2:0:7}"

export ordering=$(jq -c '.' <<< '[
    "date",
    "adwordsCampaignId",
    "newUsers",
    "sessions",
    "bounces",
    "uniquePageviews",
    "sessionDuration",
    "CallClicks",
    "QuizCompletions",
    "ApptFormSubmissions",
    "ContactFormSubmissions",
    "GetDirections",
    "LongSessions",
    "SavingsCalculatorSubmits",
    "ContactsStoreClicks",
    "AppointmentSchedulerClicks",
    "LocationDetailsViews",
    "EmailClicks",
    "NewPatientFormDownloads"
    ]')

function fetch-parse() {
    ########################################
    # Builds, executes, and parses a GA Reporting API request
    # Arguments:
    #   - The viewId to retrieve data from
    #   - The start & end dates of the report request in ISO format
    # Outputs:
    #   - TSV data with a header and report rows
    ########################################
    local goals
    goals="$(jq -c --arg viewId "$1" \
        'map(select(.viewId == $viewId) | 
        {"key": (.gaMetric | sub("^ga:";"")), "value": [.category]}) |
        from_entries' \
        $GOAL_FILE)"
    echo "viewId: $1" >2
    jq -c -f reports/esp/request_builder.jq \
        --arg viewId "$1" \
        --arg startDate "$2" \
        --arg endDate "$3" \
        --slurpfile goals "$GOAL_FILE" \
        reports/esp/request_template.json |
    python ga-reporting.py - |
    jq -r -f reports/esp/ga-parse.jq --arg viewId "$1" \
        --argjson key_mapping "$goals" -
}
    
export -f fetch-parse

awk 'FS="\t" { print $3 }' data/esp/esp_views.tsv | 
    parallel --jobs 2 --results "$OUT_DIR" fetch-parse {} "$1" "$2" |
    jq -r --argjson ordering "$ordering" --slurp 'include "ga-reporting"; 
    flatten | objectsToRows($ordering; @csv)'
