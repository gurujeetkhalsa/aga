# Chapter Rewards Restart Memo

Date: 2026-05-02

## Current Branch And Context

- Repo: `C:\Users\guruj\OneDrive\Documents\Playground\aga`
- Branch: `codex/bayrate-clean-python`
- Production SQL database: existing AGA Azure SQL database used by membership and ratings
- Production mailbox/timer app: `aga-clubexpress-mail`
- This session paused BayRate UI work and started the AGA Chapter Rewards automation project.
- This memo was written before committing the session changes.

## Key Product Decisions

- Opening legacy balances are grandfathered into the new ledger as if earned on the system start date, currently `2026-05-02`.
- The uploaded balance PDF (`C:\Users\guruj\Downloads\data1.pdf`) reports balances as of `2026-02-08`, but those balances were intentionally imported with earned/effective date `2026-05-02`.
- The user plans to later add transactions that cover the gap between `2026-02-08` and the new-system start.
- Grandfathered opening lots expire on `2028-05-02`.
- Award lots expire based on their original earned date.
- Membership/reward chapter size is based on active affiliated members, excluding `Tournament Pass`.
- New membership and renewal awards are credited only once per member event rule, and only if an eligible chapter association exists within 30 days.
- Rated-game awards use game date and game-date snapshots.
- Club size multiplier is determined from the snapshot at the time the points are earned.

## What We Built

### Rewards Schema And Ledger

Added the Chapter Rewards SQL schema under `rewards/sql/`:

- `chapter_rewards_schema.sql`
- `membership_event_logging.sql`
- `snapshot_processing.sql`
- `membership_award_processing.sql`
- `rated_game_award_processing.sql`
- `opening_balance_import.sql`
- `reporting_views.sql`

Core SQL objects include:

- `rewards.reward_runs`
- `rewards.member_daily_snapshot`
- `rewards.chapter_daily_snapshot`
- `rewards.membership_events`
- `rewards.transactions`
- `rewards.point_lots`
- `rewards.lot_allocations`
- `rewards.chapter_eligibility_periods`

Important procedures:

- `rewards.sp_record_membership_event`
- `rewards.sp_create_daily_snapshot`
- `rewards.sp_process_membership_awards`
- `rewards.sp_process_rated_game_awards`
- `rewards.sp_import_opening_balances`

Reporting views:

- `rewards.v_chapter_balances`
- `rewards.v_chapter_transaction_history`
- `rewards.v_point_lot_aging`
- `rewards.v_reward_run_history`
- `rewards.v_membership_event_audit`

### Python Reward Tools

Added the `rewards` package:

- `rewards/snapshot_generator.py`
- `rewards/membership_awards.py`
- `rewards/rated_game_awards.py`
- `rewards/opening_balances.py`
- `rewards/reports.py`
- tests under `rewards/tests/`

Useful commands:

```powershell
py -3 -m rewards.snapshot_generator --date 2026-05-02 --dry-run
py -3 -m rewards.membership_awards --date 2026-05-02 --dry-run
py -3 -m rewards.rated_game_awards --date 2026-05-02 --dry-run
py -3 -m rewards.opening_balances 'C:\Users\guruj\Downloads\data1.pdf' --effective-date 2026-05-02 --dry-run
py -3 -m rewards.reports balances --top 10
py -3 -m rewards.reports runs --top 10
py -3 -m rewards.reports lots --top 10
py -3 -m rewards.reports transactions --top 10
py -3 -m rewards.reports membership-events --top 10
```

### ClubExpress Mail App Integration

Updated `clubexpress-mail-app/function_app.py`:

- New and renewing membership emails now record reward source events by calling `rewards.sp_record_membership_event`.
- Membership reward event recording is in the same SQL transaction as the membership email update.
- Added daily rewards timers:
  - `create_rewards_daily_snapshot`
  - `process_rewards_membership_awards`
  - `process_rewards_rated_game_awards`

Default timer schedules:

- Snapshot: `05:10 UTC`
- Membership awards: `05:20 UTC`
- Rated-game awards: `05:30 UTC`

Relevant app settings:

- `REWARDS_SNAPSHOT_ENABLED`
- `REWARDS_SNAPSHOT_SCHEDULE`
- `REWARDS_MEMBERSHIP_AWARDS_ENABLED`
- `REWARDS_MEMBERSHIP_AWARDS_SCHEDULE`
- `REWARDS_RATED_GAME_AWARDS_ENABLED`
- `REWARDS_RATED_GAME_AWARDS_SCHEDULE`

The code defaults these timers to enabled if the settings are absent.

### Membership Data App Copy

Updated `membership-data-app/function_app.py` with matching membership event logging logic, but the active production mailbox processor is `aga-clubexpress-mail`.

## Production Work Completed

### SQL Applied To Azure

The following SQL has been applied to the AGA Azure SQL database:

- Chapter Rewards schema
- Membership event logging procedure
- Daily snapshot procedure
- Membership award procedure
- Rated-game award procedure
- Reporting views
- Opening balance import procedure

### Snapshot Created

Created the first real Chapter Rewards snapshot:

- Snapshot date: `2026-05-02`
- RunID: `1`
- Member snapshot rows: `17,172`
- Active members: `2,368`
- Tournament pass members: `2,753`
- Chapter snapshot rows: `158`
- Current chapters: `158`
- Multiplier counts:
  - `1x`: `43`
  - `2x`: `17`
  - `3x`: `98`

### Opening Balances Imported

Imported `C:\Users\guruj\Downloads\data1.pdf` as grandfathered opening balances.

Important: the PDF balance date is `2026-02-08`, but the imported ledger earned/effective date is intentionally `2026-05-02`.

Import results:

- Import RunID: `2`
- PDF rows parsed: `151`
- Current snapshot chapters set up: `158`
- Current chapters absent from the PDF and set up at zero: `7`
- Positive opening balance lots created: `125`
- Zero-balance setup rows: `33`
- Total opening points: `52,281,641`
- Opening earned/effective date: `2026-05-02`
- Opening lot expiration date: `2028-05-02`
- Missing snapshot matches: `0`
- Reconciliation issues in PDF math: `0`

The 7 current chapters absent from the PDF:

```text
BRRE  Barre Go Club
FVGC  Fox Valley Go Club
GRNG  Greenville / Upstate Go Club
SNIH  Ghosts of Seattle
SUNN  SiliValley Go Club
TIDW  Tidewater Go Club
UIUC  UUIC Go
```

Post-import idempotency check:

- Already imported: `125`
- New imports on rerun: `0`

### Azure Function Deployment

Deployed `clubexpress-mail-app/` to `aga-clubexpress-mail`.

Azure listed these functions after deployment:

- `create_rewards_daily_snapshot`
- `poll_clubexpress_mailbox`
- `process_rewards_membership_awards`
- `process_rewards_rated_game_awards`

Note: `az functionapp config appsettings set` failed earlier due MFA, but deployment through `func azure functionapp publish` succeeded and the code defaults have the rewards timers enabled.

## Current Live Behavior

Points should now start being recorded automatically for the implemented reward types:

- New membership and renewal source events are logged as ClubExpress emails are processed.
- Daily membership-award processing credits eligible pending membership events once the 30-day chapter decision window can be evaluated.
- Daily rated-game processing credits eligible rated, non-online, non-excluded player appearances using game-date snapshots.
- Duplicate source keys prevent reruns from double-crediting the same membership event, rated-game player appearance, or opening balance.

At the original 2026-05-02 deployment point, these were not yet implemented. Follow-up sections below document later work:

- State Championship fixed `200,000` point awards
- Tournament-host formula awards
- Redemptions and receipt review
- Point expirations
- Chapter-to-chapter transfers
- Manual adjustments/reversals
- Admin/public UI for reward balances

## Verification Completed

Local tests/checks:

```powershell
py -3 -m unittest discover rewards\tests
py -3 -m unittest discover clubexpress-mail-app\tests
py -3 -m py_compile rewards\snapshot_generator.py rewards\membership_awards.py rewards\rated_game_awards.py rewards\opening_balances.py rewards\reports.py clubexpress-mail-app\function_app.py
git diff --check
```

Known passing counts during the session:

- Rewards tests: `21`
- ClubExpress mail tests: `6`

Live report checks:

```powershell
py -3 -m rewards.reports balances --top 10
py -3 -m rewards.reports runs --top 5
py -3 -m rewards.reports lots --top 5
```

Observed live state after import:

- Top balance chapters show nonzero opening points.
- RunID `2` appears as `opening_balance` with `52,281,641` net/credit points.
- Opening lots show earned date `2026-05-02` and expiration `2028-05-02`.
- Balance reconciliation deltas were `0` in sampled reports.

## Important Caution From This Session

There was a brief discussion about changing opening balances to the PDF date `2026-02-08`. The user clarified not to do that. The intended and verified final state is:

- Opening balances are treated as of today/system start: `2026-05-02`
- Gap transactions can be added later.

A guarded date-correction SQL batch was attempted, but reports afterward showed the database remained in the desired `2026-05-02` state:

- lots earned `2026-05-02`
- lots expire `2028-05-02`
- opening source keys use `20260502`
- RunID `2` remains dated `2026-05-02`

Do not change the opening balance earned date unless the user explicitly asks again.

## Files Changed Or Added

Key new files:

- `docs/chapter-rewards-design.md`
- `rewards/__init__.py`
- `rewards/snapshot_generator.py`
- `rewards/membership_awards.py`
- `rewards/rated_game_awards.py`
- `rewards/opening_balances.py`
- `rewards/reports.py`
- `rewards/sql/chapter_rewards_schema.sql`
- `rewards/sql/membership_event_logging.sql`
- `rewards/sql/snapshot_processing.sql`
- `rewards/sql/membership_award_processing.sql`
- `rewards/sql/rated_game_award_processing.sql`
- `rewards/sql/opening_balance_import.sql`
- `rewards/sql/reporting_views.sql`
- `rewards/tests/test_snapshot_generator.py`
- `rewards/tests/test_membership_awards.py`
- `rewards/tests/test_rated_game_awards.py`
- `rewards/tests/test_opening_balances.py`
- `rewards/tests/test_reports.py`

Key modified files:

- `clubexpress-mail-app/function_app.py`
- `clubexpress-mail-app/tests/test_journal_parser.py`
- `membership-data-app/function_app.py`
- `docs/deployment-memo.md`

There are older unrelated untracked files in the working tree. Do not assume every untracked file belongs to Chapter Rewards.

## Recommended Next Steps

1. Watch live membership emails as they arrive:
   - confirm `rewards.membership_events` receives rows
   - run `py -3 -m rewards.reports membership-events --top 20`
   - run `py -3 -m rewards.membership_awards --date <date> --dry-run`
2. Watch rated games after new games are loaded:
   - run `py -3 -m rewards.rated_game_awards --date <game-date> --dry-run`
   - run `py -3 -m rewards.reports transactions --source-type rated_game_participation --top 20`
3. Build redemption workflow:
   - request table
   - receipt metadata
   - approval fields
   - FIFO lot allocation
   - reimbursement audit trail
4. Apply and deploy the tournament-host and State Championship processor when ready.
5. Consider a small admin UI after the ledger and processors settle.

## Follow-Up Deployment: Point Expirations

Completed after the original restart memo:

- Added and applied `rewards/sql/point_expiration_processing.sql` to Azure SQL.
- New procedure: `rewards.sp_process_point_expirations`.
- Added CLI: `py -3 -m rewards.expirations --date YYYY-MM-DD --dry-run`.
- Deployed `clubexpress-mail-app/` to `aga-clubexpress-mail`.
- Azure listed the new timer function after deployment:
  - `process_rewards_point_expirations`

Expiration rule:

- Lots remain usable on their `Expires_On` date.
- The processor expires lots only when `Expires_On < @AsOfDate`.

Production dry-run checks after applying the procedure:

```text
py -3 -m rewards.expirations --date 2026-05-03 --dry-run
  Expiring lots considered: 0
  New expirations: 0
  Expired points: 0

py -3 -m rewards.expirations --date 2028-05-02 --dry-run
  Expiring lots considered: 0
  New expirations: 0
  Expired points: 0

py -3 -m rewards.expirations --date 2028-05-03 --dry-run
  Expiring lots considered: 125
  New expirations: 125
  Expired points: 52,281,641
```

## Follow-Up Development: Chapter CSV Import

Added support for ClubExpress `Chapterx.csv` and `Immediate_Chapterx.csv` attachments:

- New mailbox message type: `chapter_csv`
- New SQL script: `membership-data-app/sql/chapter_import.sql`
- New staging table: `staging.chapters`
- New procedure: `membership.sp_import_chapters`
- `clubexpress-mail-app/function_app.py` now detects chapter CSV attachments by filename containing `chapterx` or by headers containing all three required fields: `ChapterID`, `ChapterCode`, and `ChapterName`.
- The parser accepts common ClubExpress aliases such as `ID`, `Name`, `Short Name`, `Primary Contact Member ID`, and `Date Created`.
- Extra ClubExpress columns are ignored.
- The import upserts `membership.chapters` by `ChapterID`.
- `ChapterRepID` is applied only when the referenced AGAID already exists in `membership.members`, preserving existing reps on update if the incoming rep is not yet present.
- As a precaution, files or staged rows missing `ChapterID`, `ChapterCode`, or `ChapterName` are rejected before inserting into `membership.chapters`.

Deployment completed after implementation:

- Applied `membership-data-app/sql/chapter_import.sql` to Azure SQL.
- Verified `staging.chapters` and `membership.sp_import_chapters` exist.
- Ran a rollback-wrapped smoke test that inserted a fake staging chapter, called `membership.sp_import_chapters`, verified the upsert inside the transaction, and confirmed the fake chapter did not persist after rollback.
- Deployed `clubexpress-mail-app/` to `aga-clubexpress-mail`.
- Later tightened and redeployed the parser/procedure so `ChapterID`, `ChapterCode`, and `ChapterName` are all mandatory.

Important operational note:

- Older `Chapterx.csv` / `Immediate_Chapterx.csv` emails that were already classified as ignored and marked processed will not be picked up automatically. They need to be replayed or unmarked/reintroduced if they should be imported.

## Follow-Up Development: BayRate Host Chapter Capture

Started the tournament-host reward prerequisite work:

- Added nullable host chapter fields to BayRate staged tournaments and production `ratings.tournaments`:
  - `Host_ChapterID`
  - `Host_ChapterCode`
  - `Host_ChapterName`
- Added nullable reward-event grouping fields:
  - `Reward_Event_Key`
  - `Reward_Event_Name`
- Added State Championship reward flag:
  - `Reward_Is_State_Championship`
- Updated `bayrate/sql/bayrate_staging_schema.sql` so existing databases get those columns and host-chapter indexes.
- BayRate now adds a review-required `host_chapter_required` warning to each parsed tournament.
- A tournament cannot be marked `ready_for_rating` until a host chapter is selected.
- Production commit planning also rejects ready runs if any staged tournament is missing host chapter metadata.
- The BayRate staging UI now loads chapter options from `membership.chapters`, renders host chapter, reward group, and State Championship controls per tournament, and sends those details through the review endpoint.
- The BayRate operator CLI can resolve a typed `ChapterID` or `ChapterCode` from `membership.chapters`, and can set or accept the reward event key/name and State Championship flag.

Reward-event grouping decision:

- For a normal one-section event, `Reward_Event_Key` defaults to that tournament's `Tournament_Code`.
- For a split event, such as separate open and handicap reports, the sections should be assigned the same `Reward_Event_Key`.
- The tournament-host reward formula should use total rated games grouped by host chapter plus `Reward_Event_Key`, not by each individual `Tournament_Code`.
- State Championship awards should also be grouped by host chapter plus `Reward_Event_Key`; if any section in that group has `Reward_Is_State_Championship = 1`, award the sponsoring chapter `200,000` points once for that grouped event.

Verification:

```powershell
py -3 -m py_compile bayrate\stage_reports.py bayrate\commit_staged_run.py bayrate\operator.py bayrate\report_parser.py ratings-explorer-app\function_app.py
py -3 -m unittest discover bayrate\tests
```

Both checks passed locally.

## Follow-Up Development: Tournament And State Championship Awards

Implemented locally after the BayRate metadata work:

- Added `rewards/sql/tournament_award_processing.sql`.
- New procedure: `rewards.sp_process_tournament_awards`.
- Added CLI: `py -3 -m rewards.tournament_awards --date-to YYYY-MM-DD --dry-run`.
- Added daily `aga-clubexpress-mail` timer function:
  - `process_rewards_tournament_awards`
- Default timer schedule: `05:35 UTC`.

Award logic:

- Groups committed `ratings.tournaments` by `Host_ChapterID` plus `Reward_Event_Key`.
- Counts rated, non-excluded games across all tournament sections in the grouped event.
- Supports any number of split sections in a group.
- Tournament-host points:
  - `0` when `games <= 15`
  - `1,000,000` when `games >= 700`
  - otherwise `1000 * ((games - 15) / (700 - 15)) ^ 0.93 * 1000`, rounded to the nearest whole point
- State Championship points:
  - if any section in the group has `Reward_Is_State_Championship = 1`, award `200,000` points once for that grouped event.
- If a later split section increases the event's total rated games, the processor inserts only the positive top-up instead of duplicating the original award.

Deployment status:

- This follow-up is local code only until `rewards/sql/tournament_award_processing.sql` is applied to Azure SQL and `clubexpress-mail-app/` is redeployed.
