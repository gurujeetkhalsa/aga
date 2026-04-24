# BayRate Clean Python Startup Memo

Date: 2026-04-24

## Branch And Commit

- Repo: `C:\Users\guruj\OneDrive\Documents\Playground\aga`
- Branch: `codex/bayrate-clean-python`
- Baseline commit: `893d8e0 Add clean Python BayRate port`

## Current Goal

Use the clean Python BayRate port as the starting point for running, testing, and modifying the rating calculation. This baseline intentionally excludes the prior sigma experiments, momentum/checkpoint replay work, and tournament-report preprocessing.

## Key Files

- `bayrate/core.py`: rating engine, CSV loaders, event replay, optimizer, posterior sigma integration, JSON output.
- `bayrate/run_bayrate.py`: CLI entry point.
- `bayrate/README.md`: input schemas and run command.
- `bayrate/tests/test_smoke.py`: standard-library smoke tests.
- `bayrate/tests/fixtures/smoke_games.csv`: two-tournament game fixture.
- `bayrate/tests/fixtures/smoke_ratings.csv`: prior rating fixture.

## Run The Smoke Tests

From repo root:

```powershell
py -3 -m unittest bayrate.tests.test_smoke
```

The smoke test verifies:

- rank parsing for kyu/dan ranks,
- basic handicap/komi helper behavior,
- two tournaments process as two events,
- a player's rating/sigma after the first tournament become that player's prior rating/sigma for the second tournament.

## Run The CLI Against The Fixture

From repo root:

```powershell
py -3 -m bayrate.run_bayrate --games bayrate\tests\fixtures\smoke_games.csv --ratings bayrate\tests\fixtures\smoke_ratings.csv --output bayrate-smoke-output.results.json
```

The output file is ignored by `.gitignore` because generated JSON should not be committed.

## Main Workflow In Code

1. `load_games_from_csv` filters and normalizes game rows.
2. `load_official_history` groups prior ratings by player.
3. `build_events` groups games by `Tournament_Code`, or by date when no code is present.
4. `_prepare_event` builds event-local players and games, applies initial rating/sigma seeding, and handles self-promotion behavior.
5. `_solve_event_ratings` optimizes event ratings.
6. `_calc_sigma2` computes posterior sigmas with numerical integration.
7. `_run_events` records per-player and per-game results, updates the rolling in-memory rating list, and moves to the next event.

## Useful Next Changes

- Add regression tests against known C++ BayRate output, if representative historical input/output files are available.
- Decide whether the Python optimizer needs closer parity with GSL BFGS output.
- Add a database/output adapter separately from the clean calculation engine.
- Add explicit validation/error reporting for malformed CSV rows once we start using real exported files.
