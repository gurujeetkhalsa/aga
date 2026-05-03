# BayRate NYGA Rewards Operations Summary

Date: 2026-05-03

Branch: `codex/bayrate-clean-python`

## Context

The user rated another tournament through the production Azure BayRate UI at:

```text
https://aga-ratings-explorer.azurewebsites.net/api/ratings-explorer/bayrate
```

BayRate should be left running in Azure. No local Functions host is needed.

## Azure BayRate And Ratings Explorer

Confirmed production Function App:

- App: `aga-ratings-explorer`
- Resource group: `aga-data-platform`
- State: `Running`
- BayRate page redirects through AAD Easy Auth as expected.

Production BayRate routes present after deploy:

- `/api/ratings-explorer/bayrate`
- `/api/ratings-explorer/bayrate/preview`
- `/api/ratings-explorer/bayrate/stage`
- `/api/ratings-explorer/bayrate/review`
- `/api/ratings-explorer/bayrate/replay`
- `/api/ratings-explorer/bayrate/commit-preview`
- `/api/ratings-explorer/bayrate/commit`
- `/api/ratings-explorer/bayrate/run`

Forced a Ratings Explorer snapshot refresh after the BayRate commit:

- Request queued at `2026-05-03T20:03:24Z`
- Manually invoked `RatingsExplorerPendingSnapshotRefresh` through the Azure Functions admin API
- Completed at `2026-05-03T20:04:42Z`
- Snapshot generated at `2026-05-03T20:04:06Z`
- Player count: `15,366`
- Tournament count: `3,255`
- Tournament detail snapshot count: `3,255`
- Small artifact warm check returned `ok: true`

## Missing Production Metadata Schema

The user noticed BayRate did not ask whether the event was a State Championship or whether the two entered tournaments should be linked for Chapter Rewards.

Root cause:

- Production SQL was missing the BayRate reward metadata columns.
- Production code was then redeployed after the schema was applied.

Applied production schema:

- `bayrate/sql/bayrate_staging_schema.sql`
- Applied in split batches because SQL Server compiled later index statements before newly added columns existed.

Confirmed production columns now exist on both:

- `ratings.bayrate_staged_tournaments`
- `ratings.tournaments`

Columns:

- `Host_ChapterID`
- `Host_ChapterCode`
- `Host_ChapterName`
- `Reward_Event_Key`
- `Reward_Event_Name`
- `Reward_Is_State_Championship`

Redeployed `aga-ratings-explorer` from prepared bundle:

```text
_deploy/ratings-explorer-app-bayrate-20260503-161241
```

Deploy command used from that generated folder:

```powershell
func azure functionapp publish aga-ratings-explorer --python --build remote
```

Deploy succeeded and Azure listed all Ratings Explorer and BayRate functions.

## BayRate Run 13

The latest BayRate run was:

- RunID: `13`
- Status: `ready_for_rating`
- Tournaments: `2`
- Games: `32`

Committed tournaments:

```text
adultcircu20260501   Adult Circuit , , May 1 2026   date 2026-05-01   games 20
youthcircu20260502   Youth Circuit , , May 1 2026   date 2026-05-02   games 12
```

Because the old production path had committed these without reward metadata, both the staged audit rows and production tournament rows were backfilled.

Backfilled metadata:

- Host chapter: `NYGA`
- ChapterID: `22910`
- Chapter name: `New York Go Association`
- Shared reward event key: `nyga-circuit-20260501`
- Shared reward event name: `NYGA Adult/Youth Circuit, May 2026`
- State Championship: `false`

## Tournament Host Award

Production was missing the tournament-awards procedure, so this was applied:

- `rewards/sql/tournament_award_processing.sql`
- Procedure: `rewards.sp_process_tournament_awards`

Dry-run for `2026-05-01` through `2026-05-03` showed:

- Event groups: `1`
- Tournament sections: `2`
- Rated games: `32`
- Host awards: `1 new / 0 already`
- Host points: `32,146`
- State Championship awards: `0`
- Missing host chapter sections: `0`
- Missing reward event key sections: `0`

Posted tournament host award:

- Rewards RunID: `7`
- TransactionID: `126`
- LotID: `126`
- Chapter: `NYGA`
- Points: `32,146`
- Earned/effective date: `2026-05-01`
- Expires on: `2028-05-01`
- Source type: `tournament_host`
- Source key: `22910:nyga-circuit-20260501:points:32146`

Formula used:

```text
points = round(1,000,000 * ((rated_games - 15) / (700 - 15)) ^ 0.93)
points = round(1,000,000 * ((32 - 15) / 685) ^ 0.93)
points = 32,146
```

Post-run dry-run showed `0 new / 1 already`, confirming idempotency.

## Rated Game Participation Awards

Question checked: whether points had been awarded to the chapter each rated-game player belonged to.

Initial result:

- No `rated_game_participation` transactions existed for the RunID 13 games.
- May 1 games were blocked because there was no `2026-05-01` rewards member/chapter snapshot.

Created missing rewards snapshot:

- Snapshot date: `2026-05-01`
- Rewards RunID: `8`
- Member snapshots: `17,188`
- Active members: `2,391`
- Tournament pass members: `2,752`
- Chapter snapshots: `158`
- Current chapters: `158`
- Multipliers: `1x=43`, `2x=17`, `3x=98`

After snapshot backfill, rated-game dry-run for `2026-05-01` through `2026-05-02` showed:

- Participant rows: `64`
- Eligible awards: `31`
- Already awarded: `0`
- New awards: `31`
- New points: `18,500`
- Missing member snapshot: `0`
- Missing chapter snapshot: `0`
- Inactive player: `4`
- No chapter: `29`
- Chapter not current: `0`

Posted rated-game awards:

- Rewards RunID: `9`
- New awards: `31`
- New points: `18,500`

By tournament:

```text
adultcircu20260501   23 awards   14,500 points
youthcircu20260502    8 awards    4,000 points
```

By chapter:

```text
NYGA   16 awards    8,000 points
CNST    3 awards    4,500 points
GOTH    6 awards    3,000 points
FYGS    3 awards    1,500 points
MGA     3 awards    1,500 points
```

CNST detail:

- ChapterID: `3123`
- Chapter name: `Crane's Nest Go Club`
- Active member count on May 1 and May 2 snapshots: `2`
- Multiplier: `3`
- Each award: `500 * 3 = 1,500`

Post-run dry-run showed:

- Eligible awards: `31`
- Already awarded: `31`
- New awards: `0`
- New points: `0`

## NYGA Balance After Postings

NYGA current balance:

```text
1,752,716 points
```

This matches both:

- Open lot remaining total: `1,752,716`
- Net transaction total: `1,752,716`

NYGA lots:

```text
opening_balance             1 lot    1,712,570 remaining
tournament_host             1 lot       32,146 remaining
rated_game_participation   16 lots       8,000 remaining
```

Expiration buckets:

```text
2028-05-01    9 lots       36,146 remaining
2028-05-02    9 lots    1,716,570 remaining
```

No NYGA debits, expirations, or redemptions have been posted yet.

## Legacy Gap Notes

Opening balances:

- Legacy PDF reported balances as of `2026-02-08`.
- The imported opening balances intentionally use earned/effective date `2026-05-02`.
- Opening balance lots expire on `2028-05-02`.

For redemptions between `2026-02-08` and `2026-05-02`:

- Use the actual redemption date as the redemption effective date.
- Allocate against the opening-balance lot.
- Mark or metadata-tag these as legacy-gap redemptions so reports can explain that the redemption happened before the new ledger start but after the source balance report.

The points just earned for RunID 13 should not be marked `legacy_gap`.

They are real new-system awards:

- `tournament_host`, earned `2026-05-01`
- `rated_game_participation`, earned `2026-05-01` and `2026-05-02`

## Current Deployment State

BayRate is up in Azure and should be usable next session without a local restart:

```text
https://aga-ratings-explorer.azurewebsites.net/api/ratings-explorer/bayrate
```

Do not stop or restart it unless needed.

## Next Session: Rewards Reporting

Planned focus:

1. Build clearer rewards reporting for chapter balances, point lots, transactions, expirations, and source-event audit trails.
2. Add rollups that collapse many rated-game lots into readable display groups, while keeping the underlying ledger granular.
3. Add reports for:
   - current chapter balance
   - lots by expiration bucket
   - transactions by source type
   - tournament host awards by reward event
   - rated-game participation awards by tournament/date/chapter
   - legacy-gap redemptions once entered
4. Consider whether these reports should be CLI-only first, SQL views, or exposed through an Azure Function endpoint.
5. Confirm `aga-clubexpress-mail` has the latest tournament-awards timer deployment if automatic tournament-host award posting should be active.

## Verification From This Session

Local tests before production deploy:

```powershell
py -3 -m unittest discover bayrate\tests
```

Result:

```text
Ran 59 tests
OK
```

Production verification:

- Ratings Explorer snapshot status returned `completed`.
- BayRate page returned AAD login redirect.
- RunID 13 metadata backfill verified in both `ratings.tournaments` and `ratings.bayrate_staged_tournaments`.
- Tournament host award dry-run after posting showed `0 new / 1 already`.
- Rated-game award dry-run after posting showed `31 already / 0 new`.
