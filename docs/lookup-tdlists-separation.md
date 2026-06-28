# AGA Lookup And TD Lists Separation

## Purpose

Separate public AGA member lookup and TD list publishing from the broader
`membership-data-app/` package so read-only/search surfaces and text-list
publishing can be deployed independently from membership/chapter imports,
ClubExpress mailbox processing, and parser helpers.

## Standalone Apps

`aga-lookup-app/` owns:

- `GET /api/AGALookup`
- `GET /api/lookup-members`

`tdlists-app/` owns:

- `GET /api/GenerateTDListA`
- `GET /api/GenerateTDListB`
- `GET /api/GenerateTDListN`
- `GET /api/tda`
- `GET /api/tdb`
- `GET /api/tdn`

Both apps are read-only against Azure SQL. They use `SQL_CONNECTION_STRING`
with a local-settings fallback for development.

## Migration Notes

Until the standalone apps are deployed and traffic is moved, the existing
`membership-data-app/` production host may still serve the lookup and TD list
routes. After cutover, `membership-data-app/` should be narrowed to
membership/chapter imports and SQL-backed import staging.

TD list short redirects should be updated to point at the standalone TD list
host when it is deployed:

- `TDLIST_REDIRECT_URL_A`
- `TDLIST_REDIRECT_URL_B`
- `TDLIST_REDIRECT_URL_N`
