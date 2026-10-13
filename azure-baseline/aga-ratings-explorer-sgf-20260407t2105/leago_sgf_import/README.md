# Leago AGA SGF Import

This folder is for importing SGF files from the American Go Association section of Leago only.

The process is intentionally split into three steps:

1. Download SGFs and Leago exports into an audit folder.
2. Build and review a CSV manifest.
3. Use `bulk_upload_sgf_csv.py` to upload SGFs to Azure and update `ratings.games.Sgf_Code`.

The same guarded flow is also exposed to scoped admins at:

```text
/api/ratings-explorer/sgf-import
```

The UI accepts Leago event/tournament URLs or keys, previews the resolved upload plan, and requires a separate apply action before uploading SGFs.

## AGA-only rule

The downloader refuses a tournament unless its Leago metadata looks like the American Go Association. It accepts common AGA identifiers such as:

- `aga`
- `american-go-association`
- `American Go Association`

If Leago changes field names, use `--allow-aga-key` to add another known AGA org/arena key or name. Do not use that option for non-AGA tournaments.

## Credentials

Do not put a Leago password in files or command history.

Use a browser login, then copy the authenticated Bearer token from a Leago API request in browser developer tools. Put it in a temporary environment variable:

```powershell
$env:LEAGO_BEARER_TOKEN = "ey..."
```

## Download

Download one or more known AGA Leago event keys. This is the preferred path because Leago event metadata includes `orgKey: "aga"`:

```powershell
cd C:\Users\guruj\OneDrive\Documents\Playground\aga\ratings-explorer-app
py -3 .\leago_sgf_import\download_leago_aga_sgfs.py --event-key LEAGO_EVENT_KEY
```

You can also download one or more known AGA Leago tournament keys:

```powershell
py -3 .\leago_sgf_import\download_leago_aga_sgfs.py --tournament-key LEAGO_TOURNAMENT_KEY
```

Or provide a JSON file containing tournament keys or tournament objects:

```powershell
py -3 .\leago_sgf_import\download_leago_aga_sgfs.py --tournaments-file .\data\leago-sgf\aga-tournaments.json
```

Output defaults to:

```text
data/leago-sgf/downloads/
```

Each run writes:

- downloaded SGFs
- `aga-export.txt`, when Leago provides it
- `tournament.xml`, when Leago provides it
- `leago-download-index.json`

## Build Manifest

Generate a draft CSV. Use `--resolve-sql` when `SQL_CONNECTION_STRING` is available; it fills `Game_ID` and rewrites match fields to the canonical AGA SQL row, including SQL player order.

```powershell
py -3 .\leago_sgf_import\build_leago_sgf_manifest.py .\data\leago-sgf\downloads\leago-download-index.json --resolve-sql
```

The draft manifest is deliberately conservative. Review and fill any missing `Game_ID`, `Tournament_Code`, player pins, round, or date before upload.

## Upload

Dry-run first:

```powershell
py -3 .\bulk_upload_sgf_csv.py .\data\leago-sgf\downloads\leago-sgf-manifest.csv --mode dry-run
```

If the report has no errors:

```powershell
py -3 .\bulk_upload_sgf_csv.py .\data\leago-sgf\downloads\leago-sgf-manifest.csv --mode apply
```

After applying, refresh the Ratings Explorer snapshot.
