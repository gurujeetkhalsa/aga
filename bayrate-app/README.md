# BayRate App

Standalone Azure Functions package for the BayRate tournament rating workflow.

Target standalone URL after deployment:

https://aga-bayrate.azurewebsites.net/api/bayrate

Included responsibilities:

- Operator UI at `/api/bayrate`.
- Report preview and duplicate checks.
- Staging report runs.
- Tournament review decisions, host chapter metadata, and reward-event metadata captured during rating.
- Staged run reload and replay.
- Production commit preview and commit.

Excluded responsibilities:

- Ratings Explorer public display routes.
- Chapter Rewards public display or admin debit routes.
- SGF import/upload tools.
- Member AGAID rating merge tools.
- Sigma experiments, Optuna tuning, sigma slice harnesses, simulations, SVG overlays, generated charts, and generated BayRate output artifacts.

Deployment notes:

- Prepare the deployment package with `.\scripts\prepare-bayrate-deploy.ps1` from the repo root.
- Publish from the generated `_deploy\bayrate-app-<timestamp>\` directory.
- The prep script copies this app plus only the allowlisted `bayrate/` modules needed for the rating process.
- Admin routes require Azure App Service Authentication, `BAYRATE_TRUST_EASY_AUTH=true`, and active `bayrate_run` or `admin_all` rows in `ratings.admin_permissions`.
- The app requires `SQL_CONNECTION_STRING` or `MYSQL_SYNC_SQL_CONNECTION_STRING`.

The core rating engine still calculates and persists rating uncertainty because BayRate ratings require it. What is intentionally excluded here is code for changing, tuning, simulating, or visualizing alternate sigma behavior.
