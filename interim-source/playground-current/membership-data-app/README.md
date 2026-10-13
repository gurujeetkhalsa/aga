<!-- Copyright 2026, American Go Association, All rights reserved -->

# Membership Data App

This app contains:

- membership/chapter import endpoint
- member lookup endpoint
- TD list publishing endpoints
- SQL-backed import staging helpers

Current source of truth:

- code: this GitHub repo, `membership-data-app/`
- production host: `https://aga-membership-functions-fmgchkbxa3hxd8h0.westus-01.azurewebsites.net`
- runtime CSV/email artifacts: Azure Blob Storage container from `CLUBEXPRESS_ARCHIVE_CONTAINER`

Current trigger surface:

- `POST /api/import_memchap`
- `GET /api/lookup-members`
- `GET /api/AGALookup`
- `GET /api/GenerateTDListA`
- `GET /api/GenerateTDListB`
- `GET /api/GenerateTDListN`
- `GET /api/tda`
- `GET /api/tdb`
- `GET /api/tdn`

Migration note:

- category import currently remains helper logic used by mailbox ingestion
- helper code is still duplicated locally for safety and will be reduced later

`/api/AGALookup` accepts case-insensitive parameter names. Dashes, underscores,
and camelCase are treated equivalently, e.g. `memberType`, `member_type`, and
`member-type`. Supported filters include `agaid`, `id`, `firstName`,
`lastName`, `status`, `minRating`, `maxRating`, `chapterCode`, `chapterName`,
`memberType`, `state`, `limit`, and `offset`.

User-facing usage examples live in `../docs/agalookup-user-guide.md`.
