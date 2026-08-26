# BayRate Python Baseline

This directory contains a clean Python port of the original C++ BayRate rating calculation. It is meant to be the fresh starting point for future BayRate work.

Included:

- Event grouping by tournament code/date.
- C++ BayRate seeding behavior, including inactivity growth and self-promotion handling.
- Handicap equivalent and game likelihood calculations.
- Event rating optimization and posterior sigma integration.
- Simple CSV loaders and a JSON-producing CLI.

Not included:

- Later sigma experiments, performance triggers, momentum logic, or checkpoint replay.
- Tournament report parsing or report-to-game preprocessing.
- Database writes.

The original BayRate code was GPL-licensed; the copied license is in `COPYING`.

## Copyright And License

Original BayRate portions are copyright 2010 Philip Waldron. The Python port,
Azure Functions workflow, staging/replay/commit integration, report parsing,
SQL adapters, tests, and deployment packaging portions are copyright 2026
American Go Association.

BayRate is licensed under the GNU General Public License, version 3 or later
(`GPL-3.0-or-later`). See `COPYING` for the license text and `NOTICE.md` for
source/provenance details.

## Input Files

`--games` expects a CSV with the game-export style columns used by the ratings database:

```text
Game_ID,Tournament_Code,Game_Date,Round,Pin_Player_1,Pin_Player_2,Rank_1,Rank_2,Color_1,Handicap,Komi,Result,Rated,Exclude,Online
```

`--ratings` expects prior rating rows with no required header:

```text
AGAID,Rating,Sigma,Elab_Date,Tournament_Code,row_id
```

## Run

From the repository root:

```powershell
py -3 -m bayrate.run_bayrate --games path\to\games.csv --ratings path\to\ratings.csv --output bayrate-output.json
```

The output JSON includes per-event player results, per-game expected values, and simple pre/post fit metrics.

Malformed CSV values are reported with the input path, line number, and column name. Domain filters still apply silently:
unrated rows, excluded rows, and online rows are skipped unless `--allow-online-games` is set.

## Merge Duplicate Member AGAIDs

Use `merge_member_ratings` when a member has production rating/game history under more than one AGAID and the rows need to be consolidated under one current AGAID without rerunning BayRate.

Preview first. This reads `ratings.ratings` and `ratings.games`, reports the rows that would move, and blocks if the merge would create duplicate rating snapshots for the same tournament or a game where the merged AGAID plays itself.

In the Ratings Explorer Function App, use the authenticated BayRate admin endpoints:

```text
GET /api/ratings-explorer/bayrate/member-merge
POST /api/ratings-explorer/bayrate/member-merge-preview
POST /api/ratings-explorer/bayrate/member-merge
```

Preview body:

```json
{"source_agaid": 31575, "target_agaid": 32194}
```

The execute endpoint requires the preview response `member_merge_plan.plan_hash`, `confirmation_text`, and `confirm_member_rating_merge=true`.

The same operation is also available as a local maintenance CLI:

```powershell
py -3 -m bayrate.merge_member_ratings --source-agaid 31575 --target-agaid 32194
```

Execute only after the preview is clean:

```powershell
py -3 -m bayrate.merge_member_ratings --source-agaid 31575 --target-agaid 32194 --execute --confirm-member-rating-merge
```

The command updates production `ratings.ratings.Pin_Player`, `ratings.games.Pin_Player_1`, and `ratings.games.Pin_Player_2` from the source AGAID(s) to the target AGAID. It does not touch ClubExpress membership rows; expire or mark the duplicate membership there after the SQL merge is complete.
