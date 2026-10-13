# Ratings Explorer SGF Upload

Standalone Azure Functions app for uploading an existing Ratings Explorer SGF archive. It does not contact Leago and contains no Ratings Explorer display or snapshot functions.

## Routes

- `GET /api/sgf-upload` — signed-in upload page
- `GET /api/sgf-upload/me` — current `sgf_import` authorization
- `POST /api/sgf-upload/preview` — validate archive and live game matches without writes
- `POST /api/sgf-upload/apply` — revalidate the plan, upload SGFs, and update `ratings.games.Sgf_Code`
- `GET /api/sgf-upload/health` — unauthenticated liveness check

The apply request must include the preview's exact `plan_hash` and `confirm_apply: true`. Existing SGFs are replaced only when the operator enables replacement and the individual manifest row has `Replace_Existing=true`.

## Required app settings

- `SQL_CONNECTION_STRING`
- `AzureWebJobsStorage` (or identity-based Blob settings)
- `RATINGS_SGF_CONTAINER` (defaults to `ratings-game-sgf`)
- `BAYRATE_TRUST_EASY_AUTH=true`

Azure App Service Authentication must be enabled. Authorization uses the same `[ratings].[admin_permissions]` `sgf_import`/`admin_all` records as Ratings Explorer.
