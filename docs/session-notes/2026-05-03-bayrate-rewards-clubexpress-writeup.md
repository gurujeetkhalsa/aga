# BayRate, Chapter Rewards, And ClubExpress Writeup

Date: 2026-05-03

Branch: `codex/bayrate-clean-python`

## Scope

This writeup covers the development arc that connected ClubExpress email processing, membership/chapter data imports, BayRate tournament metadata, ratings-table commits, and Chapter Rewards processing.

The main goal was to make Chapter Rewards auditable and automatable while giving BayRate enough operator-entered metadata to award chapters for hosted tournaments and State Championships.

## ClubExpress Email Processing

### Membership reward events

ClubExpress new-member and renewal emails now do two things in one SQL transaction:

- update membership data through the existing membership procedures
- record a rewards source event through `rewards.sp_record_membership_event`

This gives rewards processing a durable source row before points are credited. The event processors remain idempotent through unique message/member/type keys.

### Chapter CSV imports

ClubExpress `Chapterx.csv` and `Immediate_Chapterx.csv` attachments are now recognized by the mailbox processor.

The chapter CSV parser:

- detects chapter files by filename or required headers
- accepts common ClubExpress aliases such as `ID`, `Name`, `Short Name`, `Primary Contact Member ID`, and `Date Created`
- ignores extra columns
- rejects files missing `ChapterID`, `ChapterCode`, or `ChapterName`
- rejects blank required values and duplicate `ChapterID` values

The import path stages rows in `staging.chapters`, then calls `membership.sp_import_chapters`.

`membership.sp_import_chapters` upserts `membership.chapters` by `ChapterID`. As a precaution, it only inserts or updates chapters that have all three required values: `ChapterID`, `ChapterCode`, and `ChapterName`.

## Chapter Rewards Ledger

Added a new `rewards` package and SQL schema for a points ledger with daily snapshots, transaction history, and point lots.

Core SQL tables:

- `rewards.reward_runs`
- `rewards.member_daily_snapshot`
- `rewards.chapter_daily_snapshot`
- `rewards.membership_events`
- `rewards.transactions`
- `rewards.point_lots`
- `rewards.lot_allocations`
- `rewards.chapter_eligibility_periods`

Core reward procedures:

- `rewards.sp_record_membership_event`
- `rewards.sp_create_daily_snapshot`
- `rewards.sp_process_membership_awards`
- `rewards.sp_process_rated_game_awards`
- `rewards.sp_process_point_expirations`
- `rewards.sp_process_tournament_awards`
- `rewards.sp_import_opening_balances`

Python CLIs:

- `py -3 -m rewards.snapshot_generator`
- `py -3 -m rewards.membership_awards`
- `py -3 -m rewards.rated_game_awards`
- `py -3 -m rewards.expirations`
- `py -3 -m rewards.tournament_awards`
- `py -3 -m rewards.opening_balances`
- `py -3 -m rewards.reports`

## Opening Balances

The legacy balances from `C:\Users\guruj\Downloads\data1.pdf` were imported as grandfathered opening balances.

Important date decision:

- the PDF reports balances as of `2026-02-08`
- the new ledger intentionally treats those opening balances as earned/effective on `2026-05-02`
- opening lots expire on `2028-05-02`

The imported opening total was `52,281,641` points across `125` positive opening-balance lots.

## Reward Processors

### Daily snapshots

The daily snapshot processor builds member and chapter snapshots from current membership data.

Chapter size/multiplier logic excludes `Tournament Pass` members from active affiliated member counts.

### Membership awards

Membership awards are credited from durable source events.

Rules:

- new membership and renewal events are credited once
- the chapter association must be eligible within the 30-day window
- missing snapshot coverage blocks final evaluation until the data exists
- events with no eligible chapter after the window expire as `expired_no_chapter`

### Rated-game awards

Rated-game participation awards use game-date snapshots.

Rules:

- one award per eligible player appearance
- rated games only
- online and excluded games are skipped
- missing member or chapter snapshots block the award
- source keys prevent duplicate player/game awards

### Point expirations

Point expiration consumes old positive lots by creating `expire` transactions and `lot_allocations`.

Expiration rule:

- lots remain usable on their `Expires_On` date
- lots expire only when `Expires_On < @AsOfDate`

### Tournament-host and State Championship awards

Tournament awards consume metadata committed from BayRate into `ratings.tournaments`.

Required tournament metadata:

- `Host_ChapterID`
- `Host_ChapterCode`
- `Host_ChapterName`
- `Reward_Event_Key`
- `Reward_Event_Name`
- `Reward_Is_State_Championship`

Grouping:

- grouped by `Host_ChapterID + Reward_Event_Key`
- supports any number of split sections
- counts rated, non-excluded games across all sections in the group

Tournament-host formula:

- `0` points when `games <= 15`
- `1,000,000` points when `games >= 700`
- otherwise `1000 * ((games - 15) / (700 - 15)) ^ 0.93 * 1000`, rounded to the nearest whole point

State Championship rule:

- if any section in the grouped event is marked `Reward_Is_State_Championship = 1`, the sponsoring chapter receives `200,000` points once

Late split-section behavior:

- if another section is committed later and increases the total game count, the processor posts only the positive top-up
- reruns with no increased entitlement do not duplicate awards

## BayRate Metadata Capture

BayRate now captures the metadata that Chapter Rewards needs.

Staged and production tournament rows now include:

- `Host_ChapterID`
- `Host_ChapterCode`
- `Host_ChapterName`
- `Reward_Event_Key`
- `Reward_Event_Name`
- `Reward_Is_State_Championship`

BayRate behavior:

- parsed tournaments default `Reward_Event_Key` to `Tournament_Code`
- parsed tournaments default `Reward_Event_Name` to `Tournament_Descr`
- staged tournaments require host chapter review before `ready_for_rating`
- commit planning rejects ready runs if required host metadata is missing
- split tournament reports can share a `Reward_Event_Key`
- any number of sections can share the same reward group
- State Championship status is captured per section, and the rewards processor treats the group as a State Championship if any section is flagged

## Ratings Explorer / BayRate UI

The Ratings Explorer app now serves the BayRate staging UI with reward metadata controls.

The UI now:

- loads host chapter choices from `membership.chapters`
- renders a host chapter selector for each staged tournament
- renders reward group key/name inputs
- renders a State Championship checkbox
- sends metadata through the review endpoint
- includes that metadata when marking a tournament ready or approving a duplicate

The ratings app review endpoint validates selected host chapters against `membership.chapters`.

## Azure Function Timers

`aga-clubexpress-mail` now includes daily rewards timers:

- `create_rewards_daily_snapshot` at `05:10 UTC`
- `process_rewards_membership_awards` at `05:20 UTC`
- `process_rewards_rated_game_awards` at `05:30 UTC`
- `process_rewards_tournament_awards` at `05:35 UTC`
- `process_rewards_point_expirations` at `05:40 UTC`

Each timer can be disabled or rescheduled with app settings.

## SQL / Deployment Status

Already applied/deployed during the work:

- Chapter Rewards base schema
- membership event logging
- daily snapshot processing
- membership award processing
- rated-game award processing
- opening-balance import
- point expiration processing
- reporting views
- chapter CSV import SQL
- `clubexpress-mail-app` deployments for rewards timers, point expirations, and chapter CSV processing

Local but still requiring apply/deploy after this commit:

- `bayrate/sql/bayrate_staging_schema.sql` updates for host/reward metadata, if not already applied in the target environment
- `rewards/sql/tournament_award_processing.sql`
- `ratings-explorer-app` BayRate UI/API changes
- `clubexpress-mail-app` tournament awards timer change

## Verification

Recent local verification passed:

```powershell
py -3 -m py_compile bayrate\stage_reports.py bayrate\commit_staged_run.py bayrate\operator.py bayrate\report_parser.py ratings-explorer-app\function_app.py
py -3 -m py_compile rewards\tournament_awards.py clubexpress-mail-app\function_app.py
py -3 -m unittest discover bayrate\tests
py -3 -m unittest discover rewards\tests
py -3 -m unittest discover clubexpress-mail-app\tests
git diff --check
```

Known latest counts:

- BayRate tests: `59` passing
- Rewards tests: `28` passing
- ClubExpress mail tests: `13` passing

`git diff --check` reports only LF/CRLF warnings in existing working-copy files.

## Next Operational Steps

1. Apply the BayRate schema update to Azure SQL if the target database does not yet have the metadata columns.
2. Apply `rewards/sql/tournament_award_processing.sql` to Azure SQL.
3. Redeploy `ratings-explorer-app` so operators can capture host chapter, reward group, and State Championship metadata in the BayRate UI.
4. Redeploy `clubexpress-mail-app` so the tournament awards timer is live.
5. Run a dry-run tournament awards check after the first metadata-bearing BayRate commit:

```powershell
py -3 -m rewards.tournament_awards --date-to 2026-05-03 --dry-run
```
