# AGA Lookup App

Clean separated Azure Functions source folder for public AGA member lookup APIs.

Production deployment:

These routes currently deploy from `membership-data-app/` to
`aga-membership-functions`; this folder exists so the lookup implementation is
easy to inspect without unrelated membership import or mailbox code.

Current production URL:

https://aga-membership-functions-fmgchkbxa3hxd8h0.westus-01.azurewebsites.net/api/AGALookup

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
