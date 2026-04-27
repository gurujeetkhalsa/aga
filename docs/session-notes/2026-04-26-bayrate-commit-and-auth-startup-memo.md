# BayRate Commit Preview And Authorization Startup Memo

Date: 2026-04-26

## Current Branch And Context

- Repo: `C:\Users\guruj\OneDrive\Documents\Playground\aga`
- Branch: `codex/bayrate-clean-python`
- Local preview URL: `http://localhost:7071/api/ratings-explorer/bayrate`
- Local Functions host was running at the end of the session on port `7071`.
- The BayRate UI is ready for operator testing of report staging, replay, staged ratings, and production commit preview.
- No production rows were written to `ratings.tournaments`, `ratings.games`, or `ratings.ratings`.
- The next session should start by looking at authorization for who can run ratings updates.

## What Changed This Session

### Staged Ratings

- Added `ratings.bayrate_staged_ratings` to the BayRate staging schema.
- `Run Replay` now persists computed rating rows into staging by default.
- Staged rating rows include staged and production cascade events, player IDs, rating/sigma after the event, prior rating/sigma, event ordinal, tournament code, and planned production rating row ID fields.
- Replay still does not write production ratings.

### Integer RunIDs

- Replaced GUID-style BayRate staging RunIDs with integer RunIDs.
- Added SQL sequence `ratings.bayrate_run_id_seq`.
- Added `reserve_bayrate_run_id`, `assign_payload_run_id`, and `ensure_payload_run_id` in `bayrate/stage_reports.py`.
- Reset script added at `bayrate/sql/reset_bayrate_staging_tables_for_integer_run_ids.sql`.
- With explicit approval during the session, BayRate staging test rows were wiped/recreated because all staged rows were known test data.
- Production tables were not touched.

### UI Operator Improvements

- Drag/drop report loading now works for report files and pasted text drops.
- Same-date event ordering still works before preview/staging.
- Status text no longer uses pill backgrounds, to avoid looking like buttons.
- Replay display no longer shows the large starting/final rating state blobs.
- Replay summary now focuses on staged rating count, pre accuracy, post accuracy, and event summary.

### Commit Preview

- Added `bayrate/commit_staged_run.py`.
- Added read-only commit planner:
  - Loads a staged run.
  - Requires run status `ready_for_rating`.
  - Requires staged rating rows, so `Run Replay` must happen before commit preview.
  - Plans `ratings.tournaments` upserts.
  - Plans `ratings.games` replacement/inserts.
  - Plans `ratings.ratings` replacement/inserts for all affected staged and cascade tournaments.
  - Warns when replacing existing production tournaments, replacing cascade ratings, or replacing games with existing `Sgf_Code` values.
- Added `POST /api/ratings-explorer/bayrate/commit-preview`.
- Added **Preview Production Commit** button to the BayRate UI after `Run Replay`.
- The UI only exposes preview. It does not expose an actual production commit button.
- `commit_staged_run()` exists as an executable code path, but it now requires `confirm_production_commit=True`.

### Production Table Conventions

- Checked production `ratings.games.Komi`: it is `int` with existing integer values.
- Commit writer now follows the existing production convention and coerces staged Komi to integer for `ratings.games`.
- Staging can still retain parsed decimal Komi before production commit planning.

## Main Files

- `bayrate/stage_reports.py`
  - Integer RunID reservation.
  - Staging payload and staging table writes.
  - Review/status update paths.

- `bayrate/replay_staged_run.py`
  - Runs staged report replacement plus production cascade replay.
  - Persists computed staged ratings into `ratings.bayrate_staged_ratings`.

- `bayrate/commit_staged_run.py`
  - Builds the production commit plan.
  - Generates SQL statements for production `tournaments`, `games`, and `ratings`.
  - Has a guarded execute path requiring explicit confirmation.

- `ratings-explorer-app/function_app.py`
  - BayRate staging UI and API endpoints:
    - `/api/ratings-explorer/bayrate`
    - `/api/ratings-explorer/bayrate/preview`
    - `/api/ratings-explorer/bayrate/stage`
    - `/api/ratings-explorer/bayrate/review`
    - `/api/ratings-explorer/bayrate/replay`
    - `/api/ratings-explorer/bayrate/commit-preview`

- `ratings-explorer-app/bayrate_staging.html`
  - Operator UI for report input, preview, staging write, review decisions, replay, and commit preview.

- `bayrate/sql/bayrate_staging_schema.sql`
  - Current staging schema including integer RunIDs and staged ratings.

- `bayrate/tests/test_commit_staged_run.py`
  - Commit planner and guarded execution tests.

## Current UI Workflow

```text
Open /api/ratings-explorer/bayrate
Drag/drop, choose, or paste a new ratings report
Preview Run
Write Staged Run
Handle review/duplicate decisions if needed
Run Replay
Preview Production Commit
```

`Preview Production Commit` should show:

- affected rating tournament codes
- staged tournament codes
- existing production tournament codes
- cascade tournament count
- tournament upsert count
- game insert count
- rating insert count
- planned production game ID range
- planned production rating row ID range
- warnings

## Tests And Verification

Latest verification run:

```powershell
py -3 -m unittest discover bayrate\tests
```

Result:

```text
Ran 37 tests
OK
```

Also verified:

- BayRate page returns HTTP `200`.
- Function host advertises `BayRateStagingCommitPreview`.
- HTML contains `Preview Production Commit` and `commit-preview`.

## Important Safety Boundaries

- Production commit was not run.
- No production `ratings.tournaments`, `ratings.games`, or `ratings.ratings` rows were modified.
- Commit preview is read-only.
- `commit_staged_run()` is not exposed in the web UI.
- Any future production commit endpoint must have server-side authorization, explicit confirmation, clear audit logging, and a stale-run check.

## Known Design Questions

- Should parser/name/rank/membership warnings ever force `needs_review`, or should some remain informational?
- When one staged run is committed, should other staged runs be marked stale/invalidated?
- Should staged runs record the production max IDs or a production snapshot hash used during commit preview?
- How should existing `Sgf_Code` values be preserved or reconciled when replacing production games?
- Should actual production commit preserve existing game IDs on exact replacement only, or support more complex remapping?
- Should committed staged runs get a final `committed` status? The current staging status constraint does not include it.

## Next Session: Authorization For Ratings Updates

Start by inventorying how the current Azure Functions app is authenticated and how the public Ratings Explorer endpoints are exposed.

Questions to answer:

- Who should be allowed to preview BayRate runs?
- Who should be allowed to write staging rows?
- Who should be allowed to mark a staged tournament `ready_for_rating`?
- Who should be allowed to run replay?
- Who should be allowed to execute an eventual production commit?
- Should commit authority be different from staging/review authority?
- What audit trail is required for each operation?

Implementation topics to evaluate:

- Azure App Service Authentication / Microsoft Entra ID.
- Function-level auth keys versus real user identity.
- A ratings-admin allowlist in app settings or SQL.
- Role claims or app roles for ratings operators.
- Server-side enforcement in every BayRate write endpoint.
- UI hiding/disable states as convenience only, not security.
- Audit columns for staged actions: user principal, email/name, timestamp, source IP, and action metadata.
- A separate confirmation phrase for production commit.

Suggested next-session first steps:

```text
1. Inspect the current Function App auth settings locally and in Azure.
2. Decide the minimal authorization model for BayRate staging/review/replay/commit.
3. Add a server-side auth helper for BayRate write endpoints.
4. Add audit fields to staging tables if needed.
5. Keep production commit unavailable until authorization and audit are designed.
```
