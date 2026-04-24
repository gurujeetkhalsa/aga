# BayRate Sample Comparison Startup Memo

Date: 2026-04-24

## Branch And Latest Commit

- Repo: `C:\Users\guruj\OneDrive\Documents\Playground\aga`
- Branch: `codex/bayrate-clean-python`
- Latest pushed BayRate commit: `7fd8a54 Add BayRate CSV validation errors`
- Earlier setup commit: `1f4094e Add BayRate smoke test setup`

## Current State

The clean Python BayRate port successfully ran against the uploaded sample files:

- Games input: `C:\Users\guruj\OneDrive\Documents\games.csv`
- Ratings input: `C:\Users\guruj\OneDrive\Documents\ratings.csv`
- Generated output: `C:\Users\guruj\OneDrive\Documents\Playground\aga\bayrate-sample-output.results.json`

The sample run completed with no CSV validation errors.

## Run Command Used

From repo root:

```powershell
py -3 -m bayrate.run_bayrate --games 'C:\Users\guruj\OneDrive\Documents\games.csv' --ratings 'C:\Users\guruj\OneDrive\Documents\ratings.csv' --output bayrate-sample-output.results.json
```

## Sample Run Summary

- Event count: `1`
- Player count: `30`
- Game results: `54`
- Player results: `30`
- Players with no prior rating row: `4`

Metrics:

```text
pre-event accuracy:   0.6296296296
pre-event log loss:   0.8481088560
post-fit accuracy:    0.7407407407
post-fit log loss:    0.4420224084
```

## Comparison To Actual Ratings Run

The actual run results for tournament `chrryblsm20260418` were pasted into the thread and compared player-by-player against `bayrate-sample-output.results.json`.

Comparison summary:

```text
players compared:          30
max rating difference:      0.000729462952648419
average rating difference:  0.0000605238227652682
max sigma difference:       0.000220190262661291
average sigma difference:   0.0000147407155521417
```

Largest rating differences:

```text
player_id  rating_delta            sigma_delta
32491      +0.000729462952648419   +0.000220190262661291
32485      +0.000461715037332056   -0.0000726792047063274
32372      +0.000120550921476337   -0.0000676936676804463
32371      -0.0000716608653039685  +0.00000192572324286822
30722      +0.000064466870327351   +0.00000182578752361273
```

Interpretation from the thread:

- These differences are extremely small.
- They are most consistent with C++ vs Python numerical differences, especially optimizer and posterior sigma integration behavior.
- They do not look like a data-row, date, or inactivity-growth issue.
- Running the calculation a week later should not matter because inactivity growth uses `event_date - prior_elab_date`, not the wall-clock run date.
- If prior rows, game rows, and parameters are the same, the likely remaining causes are C++ GSL/BFGS optimizer parity, floating-point accumulation, convergence stopping point, and sigma integration details.

## Code Changes Already Made

The current branch includes:

- Standard-library smoke tests in `bayrate/tests/test_smoke.py`.
- Fixture data in `bayrate/tests/fixtures/smoke_games.csv` and `bayrate/tests/fixtures/smoke_ratings.csv`.
- Bad-input validation fixtures in:
  - `bayrate/tests/fixtures/bad_games.csv`
  - `bayrate/tests/fixtures/bad_ratings.csv`
- Explicit CSV validation errors from `bayrate.core.CsvValidationError`.
- CLI handling that reports validation failures in argparse style.
- Fixed CLI default for `--inactivity-growth-per-day`.

## Verification Commands

Use these from repo root:

```powershell
py -3 -m unittest bayrate.tests.test_smoke
py -3 -m py_compile bayrate\core.py bayrate\run_bayrate.py bayrate\tests\test_smoke.py
py -3 -m bayrate.run_bayrate --games bayrate\tests\fixtures\smoke_games.csv --ratings bayrate\tests\fixtures\smoke_ratings.csv --output bayrate-smoke-output.results.json
```

Intentional bad-CSV CLI check:

```powershell
py -3 -m bayrate.run_bayrate --games bayrate\tests\fixtures\bad_games.csv --ratings bayrate\tests\fixtures\smoke_ratings.csv
```

Expected error includes:

```text
CSV validation failed with 2 error(s):
bayrate\tests\fixtures\bad_games.csv:2: Pin_Player_2: players must be different
bayrate\tests\fixtures\bad_games.csv:3: Rank_1: unsupported rank value 'not-a-rank'
```

## Likely Next Work

1. Add the Cherry Blossom sample as a regression fixture.
   - Put a sanitized copy of the sample `games.csv` and `ratings.csv` under `bayrate/tests/fixtures/`.
   - Add an expected actual-output fixture with player id, rating, sigma, date, tournament code, and row id if row ids matter.
   - Add a regression test with tolerances around the actual C++ run.

2. Decide tolerance thresholds.
   - Current observed max rating delta: about `7.3e-4`.
   - Current observed max sigma delta: about `2.3e-4`.
   - A practical first tolerance could be `0.001` for rating and `0.0005` for sigma.

3. If tighter parity is needed, investigate optimizer parity.
   - Compare Python solver with original C++ GSL/BFGS behavior.
   - Record iteration count / final objective / gradient norm from Python.
   - Consider adding optional debug output for optimizer diagnostics.

4. If sigma mismatches matter, inspect posterior sigma integration.
   - Verify loop bounds, erfc factors, constants, and whether the C++ code stores results as float vs double at any point.

## Known Workspace Caveats

- Unrelated Ratings Explorer/manual files remain dirty and should not be touched during BayRate work unless explicitly requested.
- Two temporary directories created by a failed sandboxed test run may still appear under `bayrate/tests/`:
  - `bayrate/tests/tmpmk5hg0vn`
  - `bayrate/tests/tmprxjqru1g`
- Windows denied deletion of those temp directories even after an approved cleanup command. They were not committed.
