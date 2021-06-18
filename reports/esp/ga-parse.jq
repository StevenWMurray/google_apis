#!/usr/bin/env jq

include "ga-reporting";

[
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
] as $ordering |
(.reports | getJoinHeaders) as $headers |
joinReports($headers) |
rekeyObjects($headers; $ordering; $key_mapping)
# objectsToRows($ordering; @tsv)
