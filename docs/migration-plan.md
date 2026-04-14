# Migration Plan

## Goal

Turn the current mixed Azure Functions code into a clean monorepo with separate app folders for each major responsibility.

## Current source inventory

Current sources live in:

- `C:\Users\guruj\aga-functions\function_app.py`
- `C:\Users\guruj\aga-functions\ratings-explorer-app\function_app.py`

## Target apps

### `ratings-explorer-app`

Move here:

- `RatingsExplorerPage`
- `RatingsExplorerPlayers`
- `RatingsExplorerTournaments`
- `RatingsExplorerFilterOptions`
- `RatingsExplorerPlayer`
- `RatingsExplorerTournament`
- `RatingsExplorerSgfUpload`
- `RatingsExplorerPlayerHistorySvg`
- `RatingsExplorerSnapshotStatus`
- `RatingsExplorerSnapshotRefresh`
- `RatingsExplorerNightlySnapshot`
- `RatingsExplorerPendingSnapshotRefresh`
- related HTML, support, SGF, and snapshot code

### `clubexpress-mail-app`

Move here:

- `poll_clubexpress_mailbox`
- Gmail API helpers
- message classification helpers
- attachment extraction helpers
- message archiving helpers
- journal parsing helpers
- NAOL review parsing helpers
- mailbox-driven membership/category import glue

### `membership-data-app`

Move here:

- `import_memchap`
- `lookup_members`
- CSV parsing/import helpers
- SQL stored procedure execution helpers
- membership and category import staging logic

### `tdlists-app`

Move here:

- `GenerateTDListA`
- `GenerateTDListB`
- `GenerateTDListN`
- `TDListShortA`
- `TDListShortB`
- `TDListShortN`
- TD list rendering/query helpers

### `shared`

Potential shared modules after extraction:

- SQL connection/config helpers
- CSV decoding utilities
- common JSON/HTTP response helpers
- environment variable helpers

Only move code into `shared` after at least two apps actually need it.

## Suggested migration order

1. Copy `ratings-explorer-app` into this repo with minimal changes.
2. Commit that baseline.
3. Extract `clubexpress-mail-app` from the legacy `function_app.py`.
4. Extract `tdlists-app`.
5. Extract `membership-data-app`.
6. Reduce duplication only after the split is stable.

## Immediate repository tasks

1. Initialize git in this clean repo.
2. Connect `origin` to `https://github.com/gurujeetkhalsa/aga`.
3. Make the scaffold commit.
4. Copy in `ratings-explorer-app` as the first real app migration.
5. Add a deployment/readme note for each app.

