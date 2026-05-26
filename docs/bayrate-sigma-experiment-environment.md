# BayRate Sigma Experiment Environment

## Goal

Give BayRate volunteers a safe, repeatable way to change the sigma calculation and measure whether the change improves rating quality. The volunteer workflow should be local and read-only: no production SQL credentials, no writes to Azure, and no dependence on the BayRate operator UI.

The primary quality score is pre-event prediction quality on a fixed historical dataset:

- lower `pre_event_metrics.average_log_loss` is better
- lower `pre_event_metrics.average_brier` is better
- higher `pre_event_metrics.accuracy` is useful but secondary

Sigma and responsiveness diagnostics are supporting evidence. A sigma experiment is interesting only if it improves or preserves prediction quality while making rating movement healthier for players whose tournament performance is far from their prior rating.

## Maintainer Setup

Export one shared benchmark snapshot from production SQL. Put it under `data/`, which is ignored by git.

```powershell
cd C:\Users\guruj\OneDrive\Documents\Playground\aga
py -3 -m bayrate.export_experiment_dataset `
  --min-game-date 2024-01-01 `
  --max-game-date 2026-05-01 `
  --output-dir data\bayrate-sigma-20260526
```

The export writes:

- `games.csv`: historical game rows inside the benchmark window
- `ratings.csv`: prior rating rows before `--min-game-date`, in BayRate's no-header input format
- `metadata.json`: row counts and export parameters

The `ratings.csv` cutoff is intentional. It prevents experiments from using production ratings that were calculated during the benchmark period.

Share the snapshot with volunteers as a private zip. They should unpack it to the same relative path, for example `data\bayrate-sigma-20260526`.

## Volunteer Setup

From a clean clone or fork:

```powershell
cd aga
py -3 -m unittest discover bayrate\tests
py -3 -m bayrate.experiment_benchmark `
  --games data\bayrate-sigma-20260526\games.csv `
  --ratings data\bayrate-sigma-20260526\ratings.csv `
  --name baseline
```

Then make an experiment branch:

```powershell
git switch -c codex/sigma-your-name
```

Change only the rating-engine code needed for the experiment. The likely starting points are:

- `bayrate/core.py` `_calc_sigma2`: posterior sigma calculation after an event
- `bayrate/core.py` `_prepare_event`: prior sigma growth and reseeding behavior
- `BayrateConfig`: experiment parameters that should be easy to tune

Run the experiment against the same snapshot:

```powershell
py -3 -m bayrate.experiment_benchmark `
  --games data\bayrate-sigma-20260526\games.csv `
  --ratings data\bayrate-sigma-20260526\ratings.csv `
  --name sigma-floor-025 `
  --baseline-summary bayrate\output\experiments\baseline\summary.json
```

Artifacts are written under `bayrate\output\experiments\<name>\`, which is ignored by git:

- `summary.json`
- `comparison_vs_baseline.json`
- `player_results.csv`
- `game_results.csv`
- `calibration.csv`

## How To Read Results

Treat `comparison_vs_baseline.json` as the first-pass scorecard.

Good signs:

- `pre_event_metrics.average_log_loss.delta` is near zero or negative
- `pre_event_metrics.average_brier.delta` is near zero or negative
- `sigma_after.median.delta` or `sigma_after.p10.delta` increases if the baseline is too narrow
- `responsiveness.improving_capture_ratio.mean.delta` increases
- narrow sigma counts, such as `lt_0.25`, decrease

Warning signs:

- log loss or Brier gets materially worse
- rating movement becomes much larger without prediction improvement
- calibration bins become extreme, especially predictions near 0 or 1 with poor actual win rates
- the change only helps a small anecdotal case and hurts the full benchmark

`post_event_fit_metrics` is not the main score. It measures in-sample fit after the tournament has already been rated, so it can improve for overfit changes.

## Suggested Experiment Ideas

Start small and measurable:

1. Add a posterior sigma floor, possibly dependent on games played or rating band.
2. Add per-event process noise before `_calc_sigma2`, so repeated play does not collapse sigma too aggressively.
3. Increase sigma when a player's performance rating is far above prior rating and the event is not a handicap-heavy event.
4. Revisit inactivity growth so dormant players re-enter with enough uncertainty.
5. Tune partial reseeding parameters for self-promoting players.

Each experiment should include a short note with:

- the hypothesis
- the code/config change
- benchmark command used
- baseline vs candidate log loss, Brier, median sigma, p10 sigma, and improving capture ratio
- any concerning calibration or rating-movement side effects

## Guardrails

Use the exact same dataset for baseline and candidate runs. Do not refresh the export halfway through comparing experiments.

Do not commit `data/` or `bayrate/output/` artifacts. Commit source changes and, if useful, a short markdown writeup with copied summary numbers.

Do not give volunteers production SQL credentials unless they are also BayRate operators. The normal volunteer loop needs only the CSV snapshot.

Do not judge an experiment only by a few known improving players. Those cases are useful for diagnosis, but the full historical benchmark decides whether the rating system became better overall.
