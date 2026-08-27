# BayRate App

Standalone Azure Functions package for the BayRate tournament rating workflow.

Target standalone URL after deployment:

https://aga-bayrate.azurewebsites.net/api/bayrate

Copyright and license: original BayRate portions are copyright 2010 Philip
Waldron; AGA-authored Python port, Azure Functions workflow, staging/replay,
commit integration, report parsing, SQL adapters, tests, and deployment
packaging portions are copyright 2026 American Go Association. BayRate is
licensed under the GNU General Public License, version 3 or later
(`GPL-3.0-or-later`). See `bayrate/COPYING` and `bayrate/NOTICE.md` in prepared
deployment packages.

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
- Set `RATINGS_EXPLORER_SNAPSHOT_REFRESH_KEY` to the function key for the Ratings Explorer
  `snapshot-refresh` endpoint. After a successful production commit, BayRate queues a snapshot
  refresh and reports the queue result without misreporting an already-completed commit as failed.
- `RATINGS_EXPLORER_SNAPSHOT_REFRESH_URL` may override the default standalone Ratings Explorer
  endpoint when needed.

The core rating engine still calculates and persists rating uncertainty because BayRate ratings require it. What is intentionally excluded here is code for changing, tuning, simulating, or visualizing alternate sigma behavior.
