#!/usr/bin/env jq

[
    {key: "Call", category: "CallClicks"},
    {key: "Quiz", category: "QuizCompletions"},
    {key: "Appt Form", category: "ApptFormSubmissions"},
    {key: "Contact Form", category: "ContactFormSubmissions"},
    {key: "Directions", category: "GetDirections"},
    {key: "2 Minute Session", category: "LongSessions"},
    {key: "LASIK Savings Calculator", category: "SavingsCalculatorSubmits"},
    {key: "Contacts Store", category: "ContactsStoreClicks"},
    {key: "Appt Scheduler", category: "AppointmentSchedulerClicks"},
    {key: "View Location Details", category: "LocationDetailsViews"},
    {key: "Email", category: "EmailClicks"},
    {key: "New Patient Form", category: "NewPatientFormDownloads"}
] as $categories |
map(
    select(.name | startswith("WD - ")) |
    {
        viewId: .profileId,
        goalId: .id,
        gaMetric: "ga:goal\(.id)Completions",
        name,
        category: (
            .name as $name |
            reduce $categories[] as $test (""; . |= (
                . as $value | $name | 
                if contains($test.key) then $test.category else $value end))),
        isActive: .active
    }
)
