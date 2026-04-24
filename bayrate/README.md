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
