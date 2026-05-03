# Deployment Memo

## Purpose

This memo explains the current production Azure Function Apps, which repo folder maps to each one, what each app is responsible for, and the deployment path that should be used.

## Production apps

### `aga-ratings-explorer`

Repo folder:

- `ratings-explorer-app/`

Purpose:

- Ratings Explorer HTML shell
- player and tournament APIs
- filter options API
- player context API
- SGF/game viewer endpoints
- snapshot refresh, snapshot status, and snapshot timer jobs

Primary functions in this app:

- `RatingsExplorerPage`
- `RatingsExplorerPlayers`
- `RatingsExplorerPlayersStartup`
- `RatingsExplorerTournaments`
- `RatingsExplorerFilterOptions`
- `RatingsExplorerPlayer`
- `RatingsExplorerPlayerContext`
- `RatingsExplorerTournament`
- `RatingsExplorerGameSgf`
- `RatingsExplorerGameSgfViewer`
- `RatingsExplorerAsset`
- `RatingsExplorerPlayerHistorySvg`
- `RatingsExplorerSnapshotStatus`
- `RatingsExplorerSnapshotWarm`
- `RatingsExplorerSnapshotRefresh`
- `RatingsExplorerNightlySnapshot`
- `RatingsExplorerPendingSnapshotRefresh`

Production base URL:

- `https://aga-ratings-explorer.azurewebsites.net/api/ratings-explorer`

Deploy logic:

- publish from a prepared package so the sibling `bayrate/` package is included with `ratings-explorer-app/`
- prepare package from repo root:
  `.\scripts\prepare-ratings-explorer-deploy.ps1`
- publish from the generated `_deploy\ratings-explorer-app-bayrate-<timestamp>\` folder
- command from that generated folder:
  `func azure functionapp publish aga-ratings-explorer --python --build remote`

Notes:

- staging has historically used `aga-ratings-explorer-sgf-20260407t2105`
- generated snapshot data under `data/` should not be committed
- BayRate routes require Azure App Service Authentication plus `BAYRATE_TRUST_EASY_AUTH=true`.
- BayRate operators are controlled in SQL with `ratings.bayrate_admins`; apply `bayrate/sql/bayrate_authorization_schema.sql` before enabling the BayRate UI in Azure.
- BayRate tournament host chapter, reward-event grouping, and State Championship flags require `bayrate/sql/bayrate_staging_schema.sql`; it adds those reward metadata columns to `ratings.bayrate_staged_tournaments` and `ratings.tournaments`.

### `aga-clubexpress-mail`

Repo folder:

- `clubexpress-mail-app/`

Purpose:

- Gmail polling
- ClubExpress message classification
- attachment extraction and archiving
- E-Journal parsing
- NAOL review parsing
- membership/journal stored-procedure orchestration

Primary functions in this app:

- `poll_clubexpress_mailbox`
- `create_rewards_daily_snapshot`
- `process_rewards_membership_awards`
- `process_rewards_rated_game_awards`
- `process_rewards_tournament_awards`
- `process_rewards_point_expirations`

Production host:

- `https://aga-clubexpress-mail.azurewebsites.net`

Deploy logic:

- publish from the app folder
- command:
  `func azure functionapp publish aga-clubexpress-mail --python --build remote`
- working directory:
  `C:\Users\guruj\OneDrive\Documents\Playground\aga\clubexpress-mail-app`

Important settings:

- `CLUBEXPRESS_MAILBOX_ENABLED`
- `CLUBEXPRESS_MAILBOX_FOLDER`
- `CLUBEXPRESS_MAILBOX_BATCH_SIZE`
- `CLUBEXPRESS_PROCESSED_CATEGORY`
- `CLUBEXPRESS_ARCHIVE_CONTAINER`
- `REWARDS_SNAPSHOT_ENABLED`
- `REWARDS_SNAPSHOT_SCHEDULE`
- `REWARDS_MEMBERSHIP_AWARDS_ENABLED`
- `REWARDS_MEMBERSHIP_AWARDS_SCHEDULE`
- `REWARDS_RATED_GAME_AWARDS_ENABLED`
- `REWARDS_RATED_GAME_AWARDS_SCHEDULE`
- `REWARDS_TOURNAMENT_AWARDS_ENABLED`
- `REWARDS_TOURNAMENT_AWARDS_SCHEDULE`
- `REWARDS_EXPIRATIONS_ENABLED`
- `REWARDS_EXPIRATIONS_SCHEDULE`
- `GOOGLE_WORKSPACE_CLIENT_ID`
- `GOOGLE_WORKSPACE_CLIENT_SECRET`
- `GOOGLE_WORKSPACE_REFRESH_TOKEN`
- `GOOGLE_WORKSPACE_MAILBOX`
- `GOOGLE_WORKSPACE_QUERY`
- `SQL_CONNECTION_STRING`

Notes:

- this app should be the only production app with mailbox polling enabled
- current poll schedule is every 5 minutes
- spaCy-based journal person extraction and AGA title-prefix handling live here

### `aga-membership-functions`

Repo folder:

- `membership-data-app/`

Purpose:

- membership/chapter import
- member lookup API
- TD list publishing endpoints

Primary functions in this app:

- `import_memchap`
- `LookupMembers`
- `GenerateTDListA`
- `GenerateTDListB`
- `GenerateTDListN`
- `TDListShortA`
- `TDListShortB`
- `TDListShortN`

Production host:

- `https://aga-membership-functions-fmgchkbxa3hxd8h0.westus-01.azurewebsites.net`

Deploy logic:

- publish from the app folder
- command:
  `func azure functionapp publish aga-membership-functions --python --build remote`
- working directory:
  `C:\Users\guruj\OneDrive\Documents\Playground\aga\membership-data-app`

Important settings:

- `SQL_CONNECTION_STRING`
- `TDLIST_REDIRECT_URL_A`
- `TDLIST_REDIRECT_URL_B`
- `TDLIST_REDIRECT_URL_N`
- `CLUBEXPRESS_MAILBOX_ENABLED`

Notes:

- `CLUBEXPRESS_MAILBOX_ENABLED` should remain `false` here after the split
- the short TD routes currently redirect back to this same app's generated TD-list endpoints

## Current split summary

Production is now intentionally split into three apps:

1. `aga-ratings-explorer`
   Ratings Explorer only
2. `aga-clubexpress-mail`
   ClubExpress mailbox processing only
3. `aga-membership-functions`
   membership data APIs and TD lists only

This means mailbox parser changes should be deployed to `clubexpress-mail-app`, not to `membership-data-app`.

## Chapter Rewards SQL

Apply these SQL files to the AGA Azure SQL database before relying on the automated rewards timers:

- `rewards/sql/chapter_rewards_schema.sql`
- `rewards/sql/membership_event_logging.sql`
- `rewards/sql/snapshot_processing.sql`
- `rewards/sql/membership_award_processing.sql`
- `rewards/sql/rated_game_award_processing.sql`
- `rewards/sql/tournament_award_processing.sql`
- `rewards/sql/opening_balance_import.sql`
- `rewards/sql/point_expiration_processing.sql`
- `rewards/sql/reporting_views.sql`

## Membership Import SQL

Apply this SQL before deploying ClubExpress chapter CSV mailbox support:

- `membership-data-app/sql/chapter_import.sql`

## BayRate SQL

Apply this SQL before deploying the BayRate host-chapter review UI:

- `bayrate/sql/bayrate_staging_schema.sql`

The current BayRate flow requires a host chapter before a staged tournament can be marked `ready_for_rating` or committed to production ratings tables. Split sections of one hosted event, such as open and handicap sections, should share the same `Reward_Event_Key` so Chapter Rewards can total their rated games together. If a section or combined group is a State Championship, set `Reward_Is_State_Championship`; the Chapter Rewards tournament processor awards the sponsoring chapter `200,000` points once for that grouped reward event.

## Legacy note

Earlier production work often deployed from:

- `C:\Users\guruj\aga-functions\function_app.py`
- `C:\Users\guruj\aga-functions\deploy-mainapp`

That legacy monolith/deploy bundle was useful during transition, but the repo-aligned target going forward is:

- deploy `ratings-explorer-app/` to `aga-ratings-explorer`
- deploy `clubexpress-mail-app/` to `aga-clubexpress-mail`
- deploy `membership-data-app/` to `aga-membership-functions`

## Recommended verification after deploy

### Ratings Explorer

- load the main page
- check a player query
- check a tournament query
- check snapshot status endpoint

### ClubExpress Mail

- confirm host is running
- confirm `poll_clubexpress_mailbox` is present
- send or replay a known ClubExpress message and verify it is processed

### Membership Data

- verify `lookup-members` returns `200`
- verify `GenerateTDListA/B/N` return content
- verify `tda/tdb/tdn` redirect correctly
