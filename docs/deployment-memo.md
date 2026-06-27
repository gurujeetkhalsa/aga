# Deployment Memo

## Purpose

This memo explains the current production Azure Function Apps, which repo folder maps to each one, what each app is responsible for, and the deployment path that should be used.

## Current Canonical App Map

Use this table as the source of truth for current separated production deployments.

| Azure Function App | Repo folder | Responsibility |
| --- | --- | --- |
| `aga-bayrate` | `bayrate-app/` | BayRate tournament rating workflow. |
| `aga-ratings-explorer-display` | `ratings-explorer-display-app/` | Public Ratings Explorer display and read-only APIs. |
| `aga-chapter-rewards-display` | `chapter-rewards-display-app/` | Public read-only Chapter Rewards display. |
| `aga-chapter-rewards-admin` | `chapter-rewards-admin-app/` | Authorized Chapter Rewards debit/redemption/receipt administration. |
| `aga-chapter-rewards-automation` | `chapter-rewards-automation-app/` | Rewards timers and pending-renewal digest. |
| `aga-clubexpress-mail` | `clubexpress-mail-app/` | Mailbox polling and ClubExpress email processing. |
| `aga-membership-functions` | `membership-data-app/` | Membership/chapter imports, member lookup, and TD list endpoints. |
| `aga-clubexpress-sso-probe` | `clubexpress-sso-probe-app/` | Temporary ClubExpress SSO diagnostic receiver. |

`ratings-explorer-app/` is the older mixed host retained during transition. Do not use it as the source of truth for new BayRate, rewards, or public Ratings Explorer changes unless explicitly maintaining that legacy mixed deployment.

## App Detail Notes

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
- Admin routes require Azure App Service Authentication plus `BAYRATE_TRUST_EASY_AUTH=true`.
- Operators are controlled in SQL with `ratings.admin_permissions`; apply `bayrate/sql/bayrate_authorization_schema.sql` before enabling protected UIs in Azure. Use `bayrate_run` for BayRate, `rewards_redemptions` for Chapter Rewards debit/receipt entry, or `admin_all` for both. Grant or revoke scoped admins with `ratings.sp_grant_admin_permission` and `ratings.sp_revoke_admin_permission`.
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

Legacy rewards timers may still be present during the migration, but should be disabled on this host after `aga-chapter-rewards-automation` is deployed.

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

### `aga-chapter-rewards-automation`

Repo folder:

- `chapter-rewards-automation-app/`

Purpose:

- Chapter Rewards daily snapshots
- membership, rated-game, tournament, and expiration award timers
- pending ClubExpress chapter-renewal digest email

Primary functions in this app:

- `create_rewards_daily_snapshot`
- `process_rewards_membership_awards`
- `process_rewards_rated_game_awards`
- `process_rewards_tournament_awards`
- `process_rewards_point_expirations`
- `send_pending_chapter_renewals_email`

Production host:

- `https://aga-chapter-rewards-automation.azurewebsites.net`

Deploy logic:

- publish from the app folder
- command:
  `func azure functionapp publish aga-chapter-rewards-automation --python --build remote`
- working directory:
  `C:\Users\guruj\OneDrive\Documents\Playground\aga\chapter-rewards-automation-app`

Important settings:

- `REWARDS_SNAPSHOT_ENABLED`
- `REWARDS_SNAPSHOT_SCHEDULE`
- `REWARDS_MEMBERSHIP_AWARDS_ENABLED`
- `REWARDS_MEMBERSHIP_AWARDS_SCHEDULE`
- `REWARDS_RATED_GAME_AWARDS_ENABLED`
- `REWARDS_RATED_GAME_AWARDS_SCHEDULE`
- `REWARDS_RATED_GAME_AWARDS_DATE_FROM`
- `REWARDS_LEDGER_START_DATE`
- `REWARDS_TOURNAMENT_AWARDS_ENABLED`
- `REWARDS_TOURNAMENT_AWARDS_SCHEDULE`
- `REWARDS_EXPIRATIONS_ENABLED`
- `REWARDS_EXPIRATIONS_SCHEDULE`
- `PENDING_CHAPTER_RENEWALS_EMAIL_ENABLED`
- `PENDING_CHAPTER_RENEWALS_EMAIL_SCHEDULE`
- `CHAPTER_RENEWAL_NOTICE_EMAIL_TO`
- `CHAPTER_RENEWAL_PENDING_EMAIL_TO`
- `CHAPTER_RENEWAL_NOTICE_EMAIL_FROM`
- `GOOGLE_WORKSPACE_CLIENT_ID`
- `GOOGLE_WORKSPACE_CLIENT_SECRET`
- `GOOGLE_WORKSPACE_REFRESH_TOKEN`
- `GOOGLE_WORKSPACE_MAILBOX`
- `SQL_CONNECTION_STRING`

Notes:

- this app should not have `CLUBEXPRESS_MAILBOX_ENABLED`
- after this app is live, disable the matching rewards timers on `aga-clubexpress-mail` to prevent duplicate scheduler executions

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

Production is intentionally split into separate deployable apps:

1. `aga-bayrate`
   BayRate tournament rating workflow only
2. `aga-ratings-explorer-display`
   public Ratings Explorer display only
3. `aga-chapter-rewards-display`
   public Chapter Rewards display only
4. `aga-chapter-rewards-admin`
   Chapter Rewards admin workflows only
5. `aga-chapter-rewards-automation`
   Chapter Rewards background timers only
6. `aga-clubexpress-mail`
   ClubExpress mailbox processing only
7. `aga-membership-functions`
   membership data APIs and TD lists only
8. `aga-clubexpress-sso-probe`
   temporary SSO diagnostic receiver only

This means mailbox parser changes should be deployed to `clubexpress-mail-app`, BayRate changes to `bayrate-app`, display changes to `ratings-explorer-display-app`, and rewards timer changes to `chapter-rewards-automation-app`.

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
- `rewards/sql/chapter_renewal_notice_processing.sql`
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

- deploy `bayrate-app/` to `aga-bayrate`
- deploy `ratings-explorer-display-app/` to `aga-ratings-explorer-display`
- deploy `chapter-rewards-display-app/` to `aga-chapter-rewards-display`
- deploy `chapter-rewards-admin-app/` to `aga-chapter-rewards-admin`
- deploy `chapter-rewards-automation-app/` to `aga-chapter-rewards-automation`
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
