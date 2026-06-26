# Migration Plan

## Goal

Turn the current mixed Azure Functions code into a clean monorepo with three production app folders:

- `ratings-explorer-app`
- `clubexpress-mail-app`
- `membership-data-app`

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

### `chapter-rewards-automation-app`

Move here:

- `create_rewards_daily_snapshot`
- `process_rewards_membership_awards`
- `process_rewards_rated_game_awards`
- `process_rewards_tournament_awards`
- `process_rewards_point_expirations`
- `send_pending_chapter_renewals_email`
- rewards timer parameter helpers
- pending-renewal digest email sending helpers

### `membership-data-app`

Move here:

- `import_memchap`
- `lookup_members`
- `GenerateTDListA`
- `GenerateTDListB`
- `GenerateTDListN`
- `TDListShortA`
- `TDListShortB`
- `TDListShortN`
- CSV parsing/import helpers
- SQL stored procedure execution helpers
- membership and category import staging logic
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
3. `membership-data-app` created from the legacy monolith with `import_memchap`, `lookup-members`, and TD list endpoints exposed.
4. Production cut over to three Azure Function Apps:
   - `aga-ratings-explorer`
   - `aga-clubexpress-mail`
   - `aga-membership-functions`
5. `chapter-rewards-automation-app` created and deployed to `aga-chapter-rewards-automation`.
6. Rewards timers cut over from `aga-clubexpress-mail` to `aga-chapter-rewards-automation`.

Remaining cleanup:

1. Reduce duplicated helper code across the legacy-derived apps.
2. Decide whether member category import should also become a direct endpoint in `membership-data-app`.
3. Add app-specific deployment notes and environment variable documentation.
4. Introduce `shared/` modules only where duplication is clearly stable.

## Immediate repository tasks

1. Initialize git in this clean repo.
2. Connect `origin` to `https://github.com/gurujeetkhalsa/aga`.
3. Make the scaffold commit.
4. Copy in `ratings-explorer-app` as the first real app migration.
5. Add a deployment/readme note for each app.
