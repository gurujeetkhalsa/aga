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

## Migration status

Completed:

1. `ratings-explorer-app` copied from the staging sandbox into this repo.
2. `clubexpress-mail-app` created from the legacy monolith with only mailbox polling exposed as a trigger.
3. `membership-data-app` created from the legacy monolith with `import_memchap` and `lookup-members` exposed.
4. `tdlists-app` created from the legacy monolith with only TD list endpoints exposed.

Remaining cleanup:

1. Reduce duplicated helper code across the three legacy-derived apps.
2. Decide whether member category import should also become a direct endpoint in `membership-data-app`.
3. Add app-specific deployment notes and environment variable documentation.
4. Introduce `shared/` modules only where duplication is clearly stable.

## Immediate repository tasks

1. Initialize git in this clean repo.
2. Connect `origin` to `https://github.com/gurujeetkhalsa/aga`.
3. Make the scaffold commit.
4. Copy in `ratings-explorer-app` as the first real app migration.
5. Add a deployment/readme note for each app.
