# BayRate Separation

Created: 2026-06-27

## Goal

Separate the BayRate tournament rating workflow from the mixed `ratings-explorer-app` package so rating operations can be deployed independently from the public Ratings Explorer display, rewards display/admin, SGF import, chapter reports, and BayRate experiment tooling.

## New Package

`bayrate-app/` is the standalone BayRate operator package.

Target standalone URL after deployment:

https://aga-bayrate.azurewebsites.net/api/bayrate

Included responsibilities:

- Operator page at `/api/bayrate`.
- Report preview, duplicate checks, and staging.
- Staged tournament review and host/reward metadata.
- Staged run load and replay.
- Production commit preview and commit.

Excluded responsibilities:

- Ratings Explorer public display and snapshot routes.
- Chapter Rewards public/admin routes.
- SGF import/upload workflows.
- Member AGAID rating merge tools.
- BayRate sigma experiments, tuning, simulations, overlays, chart renderers, and generated output artifacts.

## Deployment Notes

Run `.\scripts\prepare-bayrate-deploy.ps1` from the repo root and publish from the generated `_deploy\bayrate-app-<timestamp>\` directory. The deploy-prep script copies only the app plus this allowlisted `bayrate/` module set:

- `__init__.py`
- `auth.py`
- `commit_staged_run.py`
- `core.py`
- `report_parser.py`
- `replay_staged_run.py`
- `snapshot_refresh.py`
- `sql_adapter.py`
- `stage_reports.py`

The standalone app requires `RATINGS_EXPLORER_SNAPSHOT_REFRESH_KEY` so a successful
production commit can queue the protected Ratings Explorer snapshot refresh endpoint.
`RATINGS_EXPLORER_SNAPSHOT_REFRESH_URL` can override the default standalone endpoint.

`core.py` remains included because it is the rating engine. Its normal rating-uncertainty calculations are part of BayRate itself; the excluded material is code for changing, tuning, simulating, or visualizing alternate sigma behavior.
