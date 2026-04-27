# BayRate Staging Operator Startup Memo

Date: 2026-04-25

## Current Branch And Context

- Repo: `C:\Users\guruj\OneDrive\Documents\Playground\aga`
- Branch: `codex/bayrate-clean-python`
- This session added the report-to-staging preprocessor path and an interactive operator layer.
- No production writes to `ratings.games` or `ratings.ratings` were implemented or run.
- Existing unrelated Ratings Explorer/manual dirty files remain in the worktree; do not touch them unless explicitly requested.

## Database Changes Made

Legacy March 25 BayRate experiment tables were archived by renaming them. No data was deleted.

Renamed:

```text
ratings.bayrate_control -> ratings.zz_20260325_bayrate_control                 1 row
ratings.bayrate_event_game_results -> ratings.zz_20260325_bayrate_event_game_results 60 rows
ratings.bayrate_event_index -> ratings.zz_20260325_bayrate_event_index         3241 rows
ratings.bayrate_event_player_ratings -> ratings.zz_20260325_bayrate_event_player_ratings 37 rows
ratings.bayrate_player_checkpoint -> ratings.zz_20260325_bayrate_player_checkpoint 37 rows
ratings.bayrate_run_metrics -> ratings.zz_20260325_bayrate_run_metrics         16 rows
ratings.bayrate_runs -> ratings.zz_20260325_bayrate_runs                       3 rows
```

Then the new staging schema was applied and verified:

```text
ratings.bayrate_runs
ratings.bayrate_staged_tournaments
ratings.bayrate_staged_games
```

Important note: the archived old `ratings.bayrate_runs` table kept its old `PK_bayrate_runs` constraint name, so the new staging schema was patched to use distinct constraint names such as `PK_bayrate_stage_runs`.

## New Files And Main Responsibilities

- `bayrate/report_parser.py`
  - Parses one or more AGA tournament reports into `ratings.tournaments` and `ratings.games` shaped rows.
  - Multi-report command:

    ```powershell
    py -3 -m bayrate.report_parser report1.txt report2.txt --pretty
    ```

- `bayrate/sql_adapter.py`
  - SQL helper modeled after Ratings Explorer connection-string patterns.
  - Uses `SQL_CONNECTION_STRING` / local settings lookup.
  - Falls back through ODBC/TDS style access.

- `bayrate/stage_reports.py`
  - Builds staged payloads.
  - Validates BayRate-required fields.
  - Detects duplicate/newness candidates by date/title/location/player overlap/game overlap.
  - Inserts rows into the new staging tables.
  - Can load and update existing staged runs.
  - Can explain review diffs against production.

- `bayrate/operator.py`
  - Interactive operator experience.
  - Supports staging new report files and reviewing existing staged `RunID`s.
  - Asks explicit yes/no questions before SQL writes.
  - Prompts for an optional approval note when marking a tournament `ready_for_rating`; the note is stored in staged tournament metadata and appended to `Review_Reason`.
  - Supports read-only review explanation mode:

    ```powershell
    py -3 -m bayrate.operator --run-id <RunID> --explain-only
    ```
  - Supports read-only staged replay mode:

    ```powershell
    py -3 -m bayrate.operator --run-id <RunID> --replay-only
    ```

- `bayrate/replay_staged_run.py`
  - Builds a read-only BayRate replay from a staged run.
  - Uses staged games for replacement/rerun tournaments.
  - Uses production `ratings.games` for later cascade tournaments.
  - Uses `ratings.ratings` latest row per player before the anchor rating row as the starter tdList.
  - Writes only a local JSON artifact; it has no production write path.

- `bayrate/compare_replay_to_production.py`
  - Compares a read-only replay artifact to production `ratings.ratings`.
  - Matches by `Tournament_Code` and `Pin_Player`.
  - Writes only a local JSON comparison artifact.

- `ratings-explorer-app/bayrate_staging.html`
  - First BayRate operator UI surface.
  - Supports drag/drop text report files.
  - Supports pasted report text.
  - Shows loaded source reports, inferred event dates, and local processing order.
  - Lets the operator reorder uploaded reports that have the same event date before preview.
  - Calls preview, staging write, review approval, and read-only replay endpoints.
  - Staging write and review approval require explicit browser confirmation and API confirmation flags.

- `ratings-explorer-app/function_app.py`
  - Adds `GET /api/ratings-explorer/bayrate`.
  - Adds `POST /api/ratings-explorer/bayrate/preview`.
  - Adds `POST /api/ratings-explorer/bayrate/stage`.
  - Adds `POST /api/ratings-explorer/bayrate/review`.
  - Adds `POST /api/ratings-explorer/bayrate/replay`.
  - The preview endpoint builds a staging payload in memory, runs duplicate/review checks when SQL is available, returns same-date groups and review explanations, and writes no SQL rows.
  - The stage endpoint writes only `ratings.bayrate_runs`, `ratings.bayrate_staged_tournaments`, and `ratings.bayrate_staged_games`.
  - The review endpoint only updates staging review fields/status.
  - The replay endpoint is read-only and returns a summary rather than a full artifact body.

- `bayrate/sql/bayrate_staging_schema.sql`
  - Creates the three new staging tables.

- `bayrate/sql/archive_legacy_bayrate_experiment_tables_20260425.sql`
  - Reversible archive-by-rename script for the March 25 experiment tables.

- Tests:
  - `bayrate/tests/test_report_parser.py`
  - `bayrate/tests/test_stage_reports.py`
  - `bayrate/tests/test_operator.py`
  - `bayrate/tests/test_replay_staged_run.py`
  - `bayrate/tests/test_compare_replay_to_production.py`
  - New fixtures under `bayrate/tests/fixtures/`.

## Operator Commands

Stage new report files interactively:

```powershell
py -3 -m bayrate.operator path\to\report1.txt path\to\report2.txt
```

Review an existing staged run:

```powershell
py -3 -m bayrate.operator --run-id b2eb52bf-dd3f-478c-8f49-e906a91c30b1
```

Explain an existing staged run without prompts or writes:

```powershell
py -3 -m bayrate.operator --run-id b2eb52bf-dd3f-478c-8f49-e906a91c30b1 --explain-only
```

Run an existing staged run through BayRate without prompts or SQL writes:

```powershell
py -3 -m bayrate.operator --run-id b2eb52bf-dd3f-478c-8f49-e906a91c30b1 --replay-only
```

Compare a replay artifact to production ratings rows without SQL writes:

```powershell
py -3 -m bayrate.compare_replay_to_production bayrate\output\cherry_blossom_staged_replay_b2eb52bf.json
```

Open the BayRate staging UI in the Functions app:

```text
/api/ratings-explorer/bayrate
```

BayRate staging UI workflow:

```text
load reports by drop/choose/paste
order same-date reports
Preview Run
Write Staged Run
Approve Duplicate where needed
Run Replay
```

## Cherry Blossom Test Run

The pasted Cherry Blossom report was saved as:

```text
bayrate/tests/fixtures/cherry_blossom_20260418_report.txt
```

It was staged into SQL:

```text
RunID: b2eb52bf-dd3f-478c-8f49-e906a91c30b1
Status: needs_review
Tournament count: 1
Game count: 54
Validation errors: 0
Staged generated code: 2026ngcche20260418
Duplicate candidate: chrryblsm20260418
Duplicate score: 0.7586
```

Read-back verified:

```text
ratings.bayrate_runs: Status needs_review, Game_Count 54
ratings.bayrate_staged_tournaments: Duplicate_Candidate_Code chrryblsm20260418
ratings.bayrate_staged_games: 54 rows
```

## Cherry Blossom Review Explanation

Read-only explanation command found:

```text
matched 53 of 54 staged games against 54 production games
staged-only:    #44 r4 8822-32491 H4 K0 result=W
production-only Game_ID 1294883 r3 8822-32491 H4 K0 result=W
```

Interpretation:

- The only difference is round number for the `8822` vs `32491` game.
- The uploaded/staged report puts it in round 4.
- Existing production has it in round 3.
- The user noted this is a known correction: Ratings Explorer previously showed a player with two games in round 3, and its crosstab logic was changed to push such a game into round 4 by not allowing a player to have multiple games in a round.
- This makes Cherry Blossom an intentionally interesting rerun case: the staged report likely corrects production round assignment while leaving game participants/result/handicap/komi the same.

Same-date production order from `ratings.ratings.id`:

```text
1. chrryblsm20260418 ratings=110901..110930 games=54
2. chgointra20260418 ratings=110931..110948 games=15
```

Implication:

- Cherry Blossom is the first production event on `2026-04-18`.
- If replay replaces Cherry Blossom, the cascade must include `chgointra20260418` after it.

## Refined Replay Rule

Original rough rule:

```text
cascade date = earliest staged tournament event date
starter tdList = ratings.ratings latest row per player before cascade date
```

Refined rule after same-date discussion:

```text
cascade anchor = earliest staged tournament in the production event sequence
starter tdList = latest rating before the anchor date plus any production tournaments earlier on the same date
replay = staged replacement tournament(s) plus production tournaments later on the same date and all future production tournaments
```

For Cherry Blossom:

```text
anchor = chrryblsm20260418
starter prior = ratings before 2026-04-18; no same-day predecessors
replay order = staged Cherry Blossom replacement, then chgointra20260418
```

Important refinement from the Gotham test:

- Do not use only `ratings.ratings.id < anchor_min_id` as the starter boundary.
- Same-day tournament rating row id ranges can overlap.
- Use event semantics instead: previous dates plus explicitly earlier same-day tournament codes.

## Cherry Blossom Read-Only Replay

The staged Cherry Blossom run was replayed without SQL writes:

```text
RunID: b2eb52bf-dd3f-478c-8f49-e906a91c30b1
Staged status: needs_review
Anchor: chrryblsm20260418 on 2026-04-18 rating_row=110901
Events: 2 (1 staged, 1 production cascade)
Games: 69
Starter players: 16343
BayRate players after replay: 16355
SQL writes: 0
```

Replay event order:

```text
1. staged     chrryblsm20260418 2026-04-18 games=54 ratings=110901..110930
2. production chgointra20260418 2026-04-18 games=15 ratings=110931..110948
```

Output artifact:

```text
bayrate/output/cherry_blossom_staged_replay_b2eb52bf.json
```

Replay metrics:

```text
pre-event:  games=69 accuracy=0.637681 average_log_loss=0.815130 average_brier=0.247005
post-fit:   games=69 accuracy=0.797101 average_log_loss=0.365527 average_brier=0.123347
```

## Cherry Blossom Replay Vs Production Comparison

The replay artifact was compared against production `ratings.ratings` for both cascade tournaments:

```text
chrryblsm20260418
  matched 30 of 30 replay / 30 production players
  rating delta avg_abs=0.000133 max_abs=0.001250 player=32491
  sigma delta  avg_abs=0.000022 max_abs=0.000332 player=32491

chgointra20260418
  matched 18 of 18 replay / 18 production players
  rating delta avg_abs=0.000255 max_abs=0.001347 player=32495
  sigma delta  avg_abs=0.000066 max_abs=0.000393 player=32495

overall
  rating delta avg_abs=0.000179 max_abs=0.001347 player=32495
  sigma delta  avg_abs=0.000038 max_abs=0.000393 player=32495
```

Output artifact:

```text
bayrate/output/cherry_blossom_staged_replay_b2eb52bf_production_compare.json
```

Interpretation:

- The replay matches production player coverage for both events.
- Differences are tiny, around one thousandth of a rank at worst.
- The Cherry Blossom round correction does not materially change BayRate ratings in this replay because the current BayRate core rates a tournament as one event rather than by individual round order.

## Gotham Full-Cascade Read-Only Replay

The pasted Gotham report was saved as:

```text
bayrate/tests/fixtures/gotham_go_fall_20251108_report.txt
```

The parser accepted it:

```text
Tournament_Code generated: gothamfall20251108
Players: 98
Games: 185
Rounds: 5 inferred from game order
```

Read-only replay command shape:

```powershell
py -3 -m bayrate.operator bayrate\tests\fixtures\gotham_go_fall_20251108_report.txt --replay-only --replay-output bayrate\output\gotham_fall_20251108_full_replay.json
```

Staging/review result:

```text
Status: needs_review
Duplicate candidate: gothm20251108 score=0.9001
Game comparison: matched 90 of 185 staged games against 185 production games
Same-date production order:
  1. hstn20251108 ratings=106418..108120 games=85
  2. gothm20251108 ratings=106449..108218 games=185
```

The 95 game mismatches are round-number placement differences, not different player/result/handicap/komi signatures. This is caused by inferring rounds from a flat `GAMES (185)` block while production has its own round assignment.

The corrected replay used this starter boundary:

```text
ratings.ratings latest row per player before 2025-11-08 plus same-day predecessors: hstn20251108
```

Replay summary:

```text
RunID: 76a299d2-6ff9-4151-a9c6-8946e91abe85
Anchor: gothm20251108 on 2025-11-08 rating_row=106449
Events: 57 (1 staged, 56 production cascade)
Games: 2647
Starter players: 16148
BayRate players after replay: 16355
SQL writes: 0
```

First and last replay events:

```text
first: staged gothm20251108 2025-11-08 games=185
last:  production chgointra20260418 2026-04-18 games=15
```

Output artifacts:

```text
bayrate/output/gotham_fall_20251108_full_replay.json
bayrate/output/gotham_fall_20251108_full_replay_production_compare.json
```

Comparison against production `ratings.ratings`:

```text
Tournaments compared: 57
Overall rating delta avg_abs=0.000094 max_abs=0.006032 player=32390
Overall sigma delta  avg_abs=0.000021 max_abs=0.002191 player=32390

gothm20251108
  matched 98 of 98 replay / 98 production players
  rating delta avg_abs=0.000031 max_abs=0.001212 player=32188
  sigma delta  avg_abs=0.000008 max_abs=0.000448 player=32188
```

Interpretation:

- The cascade correctly reached the latest production tournament in the database.
- After fixing same-day predecessor handling, the full cascade is acceptably close to production.
- Worst observed rating delta was about 0.006 rank across 57 events.

## Verification

Latest local verification:

```powershell
py -3 -m unittest discover bayrate\tests
py -3 -m bayrate.operator --help
py -3 -m py_compile ratings-explorer-app\function_app.py bayrate\stage_reports.py bayrate\report_parser.py
```

Result:

```text
Ran 23 tests in 0.031s
OK
```

Direct function smoke test for the BayRate staging page/preview endpoint:

```text
HTML loaded: true
Preview status: 200
Preview summary: 2 tournaments, 2 games, status=staged
Same-date groups: 1 group with 2 events
```

Direct function smoke test for the new action endpoints used fake adapters and no SQL:

```text
PREVIEW 200 1
STAGE 200 True 3
REVIEW 200 ready_for_rating
REPLAY 200 True 1
```

`pyOpenSSL` was installed in the user Python environment so `pytds` can connect to encrypted Azure SQL.

## Important Open Decisions

1. Whether to approve staged Cherry Blossom as `ready_for_rating` with production code `chrryblsm20260418`.
   - It should not be approved blindly just because it is a duplicate.
   - The one round mismatch appears to be an intentional correction, but the operator should understand that before marking ready.

2. Approval can now preserve a structured note such as:

   ```text
   Operator approved reuse of chrryblsm20260418; staged report corrects production round for 8822-32491 from round 3 to round 4.
   ```

3. Next engineering milestone:
   - Decide whether to approve Cherry Blossom as `ready_for_rating` with the correction note.
   - Only after that, design a separate explicit commit step for production writes.
   - Extend the UI from read-only preview into the interactive approval/stage/replay flow.

## Suggested Next Steps

1. Approve Cherry Blossom with duplicate code reuse only after confirming the round correction note.
2. Review `bayrate/output/cherry_blossom_staged_replay_b2eb52bf_production_compare.json` for affected player deltas.
3. Add an explicit operator approval step for Cherry Blossom that stores the round-correction note and marks the staged run `ready_for_rating`.
4. Add UI actions for "stage to SQL", "review duplicate", and "run read-only replay" once the preview screen feels right.
5. Before production deployment, decide whether the BayRate write/review/replay endpoints should move behind Function auth or another operator-only gate.
