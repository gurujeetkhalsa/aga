# AGA Lookup App

Standalone Azure Functions package for public AGA member lookup APIs.

Target standalone URL after deployment:

https://aga-lookup.azurewebsites.net/api/AGALookup

Routes:

- `GET /api/lookup-members`
- `GET /api/AGALookup`

This app intentionally excludes:

- Membership/chapter imports.
- ClubExpress mailbox processing.
- TD list publishing.
- Ratings Explorer display.

Required setting:

- `SQL_CONNECTION_STRING`
