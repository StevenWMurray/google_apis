#!/usr/bin/env jq
# Build list of GA view Ids
# Input is GA Account Summaries API endpoint response

.items |
map(
  {accountId: .id, accountName: .name} as $acct |
  .webProperties |
  map(
    {propertyId: .id, propertyName: .name} as $property |
    .profiles |
    map($acct + $property + {viewId: .id, viewName: .name}))) |
flatten
