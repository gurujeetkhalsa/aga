# BayRate Run 16 and Workflow Restart Memo

Date: 2026-05-04

Branch: `codex/bayrate-clean-python`

BayRate production URL:

```text
https://aga-ratings-explorer.azurewebsites.net/api/ratings-explorer/bayrate
```

## Context

This session verified a linked two-section BayRate tournament run from April 2026, forced the related tournament reward processing after confirming it was safe, and deployed a clearer BayRate workflow UI.

The current working branch also contains rewards reporting work in the Ratings Explorer app that was already part of the local deploy bundle. Keep unrelated local docs, SQL scratch files, and legacy reward loader scripts out of this commit unless intentionally cleaning up that larger workstream.

## BayRate Run 16 Verification

RunID `16` was created on 2026-05-04 at `20:30:51` and committed at `2026-05-04T20:33:48Z`.

The run staged and committed two linked tournament reports:

| Tournament Code | Date | Host | ChapterID | Reward Event Key |
| --- | --- | --- | ---: | --- |
| `springhand20260419` | 2026-04-19 | SEAG | 7911 | `spring-open-tournament-seattle-20260419` |
| `springopen20260419` | 2026-04-19 | SEAG | 7911 | `spring-open-tournament-seattle-20260419` |

Production verification showed:

- 22 production games inserted, Game IDs `1295052` through `1295073`.
- No missing or mismatched production game rows.
- Ratings cascade touched 7 events:
  - `springhand20260419`
  - `springopen20260419`
  - `2026vermon20260425`
  - `2026badukp20260425`
  - `2026badukp20260425-2`
  - `adultcircu20260501`
  - `youthcircu20260502`
- 95 staged/cascade rating rows, Rating IDs `111361` through `111455`.
- Production rating comparison returned 0 missing rows and 0 max rating/sigma delta.

Conclusion: the BayRate commit and the downstream cascade looked correct.

## Tournament Rewards

The daily tournament reward job had already run at `2026-05-04T05:35Z`, before BayRate RunID `16` was committed, so no SEAG tournament reward transaction existed yet when checked.

A scoped dry-run for `2026-04-19` showed exactly one combined SEAG host award pending:

- Host: SEAG / ChapterID `7911`
- Sections: 2
- Rated games: 22
- Points: `14,085`
- Reward event key: `spring-open-tournament-seattle-20260419`
- State championship award: no

The production tournament reward stored procedure was then run manually for `2026-04-19`.

Result:

- Reward RunID: `19`
- TransactionID: `1268`
- Chapter: SEAG
- Event type: `tournament_host`
- Points: `14,085`
- Source key: `7911:spring-open-tournament-seattle-20260419:points:14085`
- Tournament codes: `springhand20260419,springopen20260419`
- Rated game count: 22

The follow-up dry-run returned `HostAlreadyAwardedCount=1`, `HostNewAwardCount=0`, and `PointTotal=0`, confirming idempotency after the manual post.

## BayRate UI and API Changes

The BayRate staging page now has a sticky workflow panel at the top of the page with the main controls and current status visible without scrolling.

Current workflow steps:

1. Reports
2. Metadata
3. Reward Links
4. Preview
5. Stage
6. Review
7. Replay
8. Commit Plan
9. Commit

The top panel now includes:

- main action buttons for preview, stage, replay, commit preview, and production commit
- a `Confirm Reward Links` button
- a "Last action" summary
- a "Waiting for" explanation
- a pre-stage metadata issue list
- a reward-link grouping summary
- review queue controls for Mark Ready, Approve Duplicate, and Details

Metadata and reward linking are now explicit workflow gates:

- Preview and stage are disabled until required report metadata is complete.
- Multi-report local runs require reward links to be confirmed.
- Link actions mark reward links confirmed.
- Manual report metadata changes clear reward-link confirmation so the operator rechecks the grouping.

The backend now accepts report metadata before staging payload review, including tournament name, city/state/country, host chapter fields, reward event key/name, and state championship flag.

The Ratings Explorer app also has a BayRate metadata options endpoint:

```text
/api/ratings-explorer/bayrate/metadata-options
```

## Rewards Report Page

The local Ratings Explorer function app changes include public rewards report routes and an HTML report page:

- `RewardsPublicReportPage`
- `RewardsPublicBalances`
- `RewardsPublicChapter`
- `ratings-explorer-app/rewards_report.html`

Because `function_app.py` references `rewards_report.html`, commit the HTML page with the function changes.

## Deployment

Ratings Explorer was deployed twice to Azure Function App `aga-ratings-explorer`.

First deploy package:

```text
C:\Users\guruj\OneDrive\Documents\Playground\aga\_deploy\ratings-explorer-app-bayrate-20260504-171456
```

Second deploy package, with Metadata and Reward Links steps:

```text
C:\Users\guruj\OneDrive\Documents\Playground\aga\_deploy\ratings-explorer-app-bayrate-20260504-172340
```

Both publishes completed successfully with:

```text
func azure functionapp publish aga-ratings-explorer --python --build remote
```

If the UI still looks stale, hard refresh the browser because the page is served as static HTML through the function app.

## Verification Already Run

Local verification during the session:

- extracted BayRate page script and ran `node --check`
- ran `git diff --check -- ratings-explorer-app/bayrate_staging.html`
- ran `py -3 -m py_compile function_app.py` inside the deploy package
- verified the deploy package included the new workflow panel and Metadata/Reward Links labels
- confirmed Azure publish listed the BayRate and rewards routes after deployment

Before committing, rerun the local unit test and compile check:

```text
py -3 -m unittest bayrate.tests.test_stage_reports
py -3 -m py_compile ratings-explorer-app\function_app.py
```

## Next Session Focus

- User will test the BayRate UI more.
- Watch whether the metadata and reward-link gates feel too strict. One possible adjustment is allowing exploratory preview before metadata is complete while still blocking staging.
- Continue rewards reporting polish after the BayRate workflow settles.
- Clean up or separately commit the unrelated local rewards legacy loader docs/scripts if they are still needed.
