# TD Lists App

Standalone Azure Functions package for AGA TD list generation and short redirects.

Target standalone URL after deployment:

https://aga-tdlists.azurewebsites.net/api/GenerateTDListA

Routes:

- `GET /api/GenerateTDListA`
- `GET /api/GenerateTDListB`
- `GET /api/GenerateTDListN`
- `GET /api/tda`
- `GET /api/tdb`
- `GET /api/tdn`

`TDListN` uses fixed member type labels so columns stay aligned:
`Youth`, `Full`, `Life`, `Comp`, and `Pass`.

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
