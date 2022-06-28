#!/usr/bin/env jq

[
    {key: "Call", category: "callClicks"},
    {key: "Quiz", category: "quizCompletions"},
    {key: "Appt Form", category: "apptFormSubmissions"},
    {key: "Contact Form", category: "contactFormSubmissions"},
    {key: "Directions", category: "getDirections"},
    {key: "2 Minute Session", category: "longSessions"},
    {key: "LASIK Savings Calculator", category: "savingsCalculatorSubmits"},
    {key: "Contacts Store", category: "contactsStoreClicks"},
    {key: "Appt Scheduler", category: "appointmentSchedulerClicks"},
    {key: "View Location Details", category: "locationDetailsViews"},
    {key: "Email", category: "emailClicks"},
    {key: "New Patient Form", category: "newPatientFormDownloads"}
] as $categories |
.items |
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
