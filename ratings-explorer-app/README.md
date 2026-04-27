# Ratings Explorer App

This app contains:

- Ratings Explorer HTML shell
- player and tournament APIs
- filter options API
- SGF upload and viewing support
- snapshot refresh and status endpoints
- snapshot timer jobs
- SGF bulk upload helper scripts and viewer assets
- BayRate staging/replay endpoints when the sibling `bayrate/` package is included in the deployment package

Current migration source:

- `C:\Users\guruj\aga-functions-sgf-phase1\ratings-explorer-app\`

Deployment note:

- Run `.\scripts\prepare-ratings-explorer-deploy.ps1` from the repo root and publish from the generated `_deploy\ratings-explorer-app-bayrate-<timestamp>\` directory. Publishing directly from this folder omits the sibling `bayrate/` package.
- BayRate routes require Azure App Service Authentication, `BAYRATE_TRUST_EASY_AUTH=true`, and active rows in `ratings.bayrate_admins`.

Intentionally not copied into git:

- `local.settings.json`
- runtime caches like `.python_packages/` and `__pycache__/`
- generated snapshot data under `data/`
- temporary scratch CSV/result files
