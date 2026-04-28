# BayRate Commit UI And Journal Parser Restart Memo

Date: 2026-04-28

## Current Branch And Context

- Repo: `C:\Users\guruj\OneDrive\Documents\Playground\aga`
- Branch: `codex/bayrate-clean-python`
- Ratings Explorer production app: `aga-ratings-explorer`
- BayRate production URL: `https://aga-ratings-explorer.azurewebsites.net/api/ratings-explorer/bayrate`
- ClubExpress mail production app: `aga-clubexpress-mail`
- This memo was written before committing the session changes.

## What We Finished

### BayRate Commit Logic

- Added the actual production commit execution path to the Ratings Explorer BayRate API.
- Commit execution now requires:
  - BayRate admin authorization
  - `confirm_production_commit=true`
  - exact confirmation text: `COMMIT RUN <RunID>`
  - a commit preview `plan_hash`
  - explicit SGF replacement acknowledgement if existing production SGF-linked game rows would be replaced
- Added a transaction guard so a staged run with production `Game_ID` or `Planned_Rating_Row_ID` markers cannot be committed again.
- Added stale-preview protection by hashing the printable commit plan.
- Added commit audit metadata into `ratings.bayrate_runs.SummaryJson`.
- Changed rating row allocation to append after current max `ratings.ratings.id`, because production rating IDs are not guaranteed to be contiguous by tournament. This fixed the RunID 6 duplicate-key failure around `id=110494`.

### BayRate UI

- Added a staged RunID loader to the BayRate UI.
- The UI can load an existing staged run by RunID, including direct `?run_id=<id>` links.
- The UI now shows commit state:
  - `needs_replay`
  - `replayed_uncommitted`
  - `committed`
  - `partial_commit_marker`
- Added a production commit button after a valid commit preview.
- The UI prompts for a browser confirm and exact typed confirmation before posting to the commit endpoint.
- Committed or partially marked runs do not expose replay/commit actions.

### RunID 6 Production Commit

- The first UI commit attempt for RunID 6 hung and did not write production rows.
- A direct retry initially failed with:
  - duplicate key in `ratings.ratings`, value `110494`
- Root cause:
  - the old planner tried to reuse an existing cascade rating ID range
  - unrelated production rows were interleaved in that range
- After the append-only rating ID fix, RunID 6 committed successfully.
- Final RunID 6 commit details:
  - plan hash: `478d0b3032798dfafa495b5d0e84459462f3026582f594f42204ec88e8939639`
  - planned rating IDs: `110949` to `111295`
  - planned game IDs: `1294894` to `1294933`
  - production write count: `391`
  - staged ratings with planned IDs: `347` of `347`
  - staged games with production IDs: `40` of `40`
- The commit guard should now block any attempt to recommit RunID 6.

### Ratings Explorer Snapshot

- A forced Ratings Explorer snapshot refresh was run after RunID 6.
- First refresh attempt failed due to SQL alias issue:
  - `The multi-part identifier "g.Game_ID" could not be bound.`
- Local snapshot artifact build succeeded against production.
- A second forced Azure refresh completed successfully:
  - generated at: `2026-04-28T13:51:51Z`
  - player count: `15363`
  - tournament count: `3251`
  - tournament detail snapshot count: `3251`
  - completed at: `2026-04-28T13:52:26Z`

### E-Journal Context Parser

- Investigated Jonathan Green `22833` showing context under `5 MINUTES WITH: Ofer Zivony, Israel`.
- Root cause:
  - the E-Journal parser used the linked headline list as article-body boundaries
  - later Vermont tournament text was attached to the final `5 MINUTES WITH` link
- Fixed parser behavior in both duplicated copies:
  - `clubexpress-mail-app/function_app.py`
  - `membership-data-app/function_app.py`
- New behavior:
  - parse actual HTML article headings first
  - use linked headlines only as a URL lookup for those headings
  - fall back to the older line-based parser only if block parsing finds no articles
- Added regression test:
  - `clubexpress-mail-app/tests/test_journal_parser.py`

### E-Journal Data Repairs

- Corrected 2026-04-27 E-Journal rows for message `19dd0b5838eb0a14`.
- Jonathan Green and Wren Perchlik now point to:
  - `Youth Takes Top Prize at Vermont Spring Go Tournament`
- Also repaired the same issue's other article assignments for Shu/Archives/Vermont names.
- Audited all 66 processed E-Journal source messages.
- Found one additional article-association problem:
  - 2026-02-22 message `19dade8cd9322724`
  - stored rows had a URL as the article title
- Replaced that message's 5 bad rows with 8 correct rows under:
  - `World-Class Professionals Headline 2026 U.S. Go Congress`
- Post-repair audit result:
  - processed E-Journals checked: `66`
  - source messages readable: `66`
  - remaining article-misassociation cases: `0`
- Deployed the final parser fix to `aga-clubexpress-mail`.

## Production Verification

- Ratings Explorer live `player-context?agaid=22833` returns:
  - `Youth Takes Top Prize at Vermont Spring Go Tournament`
- `aga-clubexpress-mail` deployment succeeded and Azure lists:
  - `poll_clubexpress_mailbox`
- RunID 6 production markers are present in staging and the audit summary marks it committed.
- The user later confirmed the earlier concern about `ratings.tournaments` was a misread.

## Tests And Checks Run

BayRate:

```powershell
py -3 -m unittest discover bayrate\tests
py -3 -m py_compile bayrate\commit_staged_run.py ratings-explorer-app\function_app.py
```

E-Journal parser:

```powershell
py -3 -m unittest discover clubexpress-mail-app\tests
py -3 -m py_compile clubexpress-mail-app\function_app.py membership-data-app\function_app.py
```

BayRate HTML syntax check:

```powershell
node -e "<inline script extraction and Function constructor check>"
```

## Current Safety Boundaries

- RunID 6 is committed. Do not try to replay or recommit it.
- New staged run work should use a fresh RunID.
- The user started loading a new report expected to become RunID 7.
- The new report needs review, so do not proceed with rating submission/commit until the report submission/review issue is fixed.
- BayRate production commit is now possible from the UI, so keep the typed confirmation, plan-hash check, SGF acknowledgement, admin auth, and SQL transaction guard intact.
- Older unrelated untracked files are still present in the working tree and were not part of this session.

## Next Session: BayRate UI

Start with production BayRate UI:

```text
https://aga-ratings-explorer.azurewebsites.net/api/ratings-explorer/bayrate
```

Suggested next tasks:

1. Load the new report/run and fix the report submission/review blocker.
2. Make the "needs review" flow clearer for operators:
   - exact blocker reason
   - required operator action
   - retry path after approval or correction
3. Improve commit UX:
   - visible success message after commit returns
   - committed-state banner after reload
   - clearer disabled state for already committed runs
4. Consider adding a small operator activity/audit view for:
   - preview hash
   - commit status
   - committed by
   - committed at
5. Re-test load-run, replay, preview, and commit-state behavior using already committed RunID 6 and a new uncommitted run.

## Useful Commands

Run BayRate tests:

```powershell
py -3 -m unittest discover bayrate\tests
```

Prepare Ratings Explorer deploy package:

```powershell
.\scripts\prepare-ratings-explorer-deploy.ps1
```

Publish Ratings Explorer from the generated deploy folder:

```powershell
func azure functionapp publish aga-ratings-explorer --python --build remote
```

Publish ClubExpress mail app:

```powershell
func azure functionapp publish aga-clubexpress-mail --python --build remote
```
