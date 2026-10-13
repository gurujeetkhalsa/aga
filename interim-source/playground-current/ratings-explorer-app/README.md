<!-- Copyright 2026, American Go Association, All rights reserved -->

# Ratings Explorer App

This app contains:

- Ratings Explorer HTML shell
- player and tournament APIs
- filter options API
- SGF upload and viewing support
- admin-only SGF import UI for Leago references or validated archive ZIPs at `/api/ratings-explorer/sgf-import`
- snapshot refresh and status endpoints
- snapshot timer jobs
- SGF bulk upload helper scripts and viewer assets
- BayRate staging/replay/member-merge endpoints when the sibling `bayrate/` package is included in the deployment package
- authorized Chapter Rewards debit-entry endpoints and form
- Go Services Directory prototype at `/api/ratings-explorer/go-services`

Current source of truth:

- code: this GitHub repo, `ratings-explorer-app/`
- production host: `https://aga-ratings-explorer.azurewebsites.net`
- Ratings Explorer snapshots: Azure Blob Storage container `ratings-explorer`
- SGF files: Azure Blob Storage container from `RATINGS_SGF_CONTAINER`, defaulting to `ratings-game-sgf`

Deployment note:

- Run `.\scripts\prepare-ratings-explorer-deploy.ps1` from the repo root and publish from the generated `_deploy\ratings-explorer-app-bayrate-<timestamp>\` directory. Publishing directly from this folder omits the sibling `bayrate/` package.
- Admin routes require Azure App Service Authentication, `BAYRATE_TRUST_EASY_AUTH=true`, and active rows in `ratings.admin_permissions`.
- BayRate routes require `bayrate_run` or `admin_all`; Chapter Rewards debit and receipt routes require `rewards_redemptions` or `admin_all`; SGF import requires `sgf_import` or `admin_all`.
- Grant or revoke scoped admins with `ratings.sp_grant_admin_permission` and `ratings.sp_revoke_admin_permission`.
- Receipt uploads use `REWARDS_RECEIPT_CONTAINER`, defaulting to `chapter-rewards-receipts`, and Azure Blob Storage from `AzureWebJobsStorage` or the managed-identity `AzureWebJobsStorage__blobServiceUri` settings.
- The Go Services Directory prototype is static/local-storage only. Its product design and schema draft are in `../docs/go-services-directory-design.md` and `sql/go_services_directory_schema.sql`.
- SGF bulk-upload manifests should keep `Sgf_File` values in Azure Blob Storage, using `azure-blob://<container>/<blob>` or a Blob HTTPS URL. Local paths are still accepted for one-off local testing, but should not be committed as current sources.
- Archive imports accept a ZIP containing exactly one `ratings-explorer-manifest.csv` and its relative `Sgf_File` entries. Preview and apply revalidate exact SQL game fields, reject unsafe ZIP paths, keep replacement opt-in, and require an unchanged plan hash.

BayRate member AGAID merge endpoints:

- `GET /api/ratings-explorer/bayrate/member-merge`
- `POST /api/ratings-explorer/bayrate/member-merge-preview`
- `POST /api/ratings-explorer/bayrate/member-merge`

Preview body:

```json
{"source_agaid": 31575, "target_agaid": 32194}
```

Execute body uses the preview response `member_merge_plan.plan_hash` and `confirmation_text`:

```json
{
  "source_agaid": 31575,
  "target_agaid": 32194,
  "confirm_member_rating_merge": true,
  "confirmation_text": "MERGE 31575 INTO 32194",
  "member_merge_plan_hash": "..."
}
```

Intentionally not copied into git:

- `local.settings.json`
- runtime caches like `.python_packages/` and `__pycache__/`
- generated snapshot data under `data/`
- temporary scratch CSV/result files
