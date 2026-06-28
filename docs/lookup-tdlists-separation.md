<!-- Copyright 2026, American Go Association, All rights reserved -->

# AGA Lookup And TD Lists Separation

## Purpose

Separate public AGA member lookup and TD list publishing from the broader
`membership-data-app/` package at the source-code level so read-only/search
surfaces and text-list publishing are easy to inspect without membership/chapter
imports, ClubExpress mailbox processing, and parser helpers.

## Clean Source Folders

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

Both folders are read-only against Azure SQL. They use `SQL_CONNECTION_STRING`
with a local-settings fallback for development.

## Production Deployment

The public URLs are unchanged. Lookup and TD list routes are still deployed from
`membership-data-app/` to the existing `aga-membership-functions` Azure Function
App. The separated folders are present so GitHub readers can see the clean
versions without confusing them with unrelated code.

TD list short redirects remain configured on `aga-membership-functions`:

- `TDLIST_REDIRECT_URL_A`
- `TDLIST_REDIRECT_URL_B`
- `TDLIST_REDIRECT_URL_N`
