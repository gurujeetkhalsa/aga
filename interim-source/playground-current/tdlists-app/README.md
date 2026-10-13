<!-- Copyright 2026, American Go Association, All rights reserved -->

# TD Lists App

Clean separated Azure Functions source folder for AGA TD list generation and
short redirects.

Production deployment:

These routes currently deploy from `membership-data-app/` to
`aga-membership-functions`; this folder exists so the TD list implementation is
easy to inspect without unrelated membership import or mailbox code.

Current production URL:

https://aga-membership-functions-fmgchkbxa3hxd8h0.westus-01.azurewebsites.net/api/GenerateTDListA

Routes:

- `GET /api/GenerateTDListA`
- `GET /api/GenerateTDListB`
- `GET /api/GenerateTDListN`
- `GET /api/tda`
- `GET /api/tdb`
- `GET /api/tdn`

`TDListN` uses fixed member type labels so columns stay aligned:
`Youth`, `Full`, `Life`, `Comp`, and `Pass`.

TD list queries include only members whose status is `Active` or `Expired`;
`Dropped` members are excluded.

This app intentionally excludes:

- Membership/chapter imports.
- ClubExpress mailbox processing.
- AGA Lookup APIs.
- Ratings Explorer display.

Required settings:

- `SQL_CONNECTION_STRING`
- `TDLIST_REDIRECT_URL_A`
- `TDLIST_REDIRECT_URL_B`
- `TDLIST_REDIRECT_URL_N`
