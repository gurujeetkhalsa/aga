# Ratings Explorer App

This app contains:

- Ratings Explorer HTML shell
- player and tournament APIs
- filter options API
- SGF upload and viewing support
- admin-only Leago SGF import UI at `/api/ratings-explorer/sgf-import`
- snapshot refresh and status endpoints
- snapshot timer jobs
- SGF bulk upload helper scripts and viewer assets
- BayRate staging/replay/member-merge endpoints when the sibling `bayrate/` package is included in the deployment package
- authorized Chapter Rewards debit-entry endpoints and form

Current migration source:

- `C:\Users\guruj\aga-functions-sgf-phase1\ratings-explorer-app\`

Deployment note:

- Run `.\scripts\prepare-ratings-explorer-deploy.ps1` from the repo root and publish from the generated `_deploy\ratings-explorer-app-bayrate-<timestamp>\` directory. Publishing directly from this folder omits the sibling `bayrate/` package.
- Admin routes require Azure App Service Authentication, `BAYRATE_TRUST_EASY_AUTH=true`, and active rows in `ratings.admin_permissions`.
- BayRate routes require `bayrate_run` or `admin_all`; Chapter Rewards debit and receipt routes require `rewards_redemptions` or `admin_all`; Leago SGF import requires `sgf_import` or `admin_all`.
- Grant or revoke scoped admins with `ratings.sp_grant_admin_permission` and `ratings.sp_revoke_admin_permission`.
- Receipt uploads use `REWARDS_RECEIPT_CONTAINER`, defaulting to `chapter-rewards-receipts`, and Azure Blob Storage from `AzureWebJobsStorage` or the managed-identity `AzureWebJobsStorage__blobServiceUri` settings.

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
