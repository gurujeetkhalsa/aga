# Chapter Rewards Automation App

Standalone timer host for Chapter Rewards background processing.

This app contains:

- daily member/chapter rewards snapshot creation
- membership award posting
- rated-game award posting
- tournament-host and State Championship award posting
- point expiration posting
- pending ClubExpress chapter-renewal digest email

It intentionally does not poll the ClubExpress mailbox, parse inbound ClubExpress emails, import membership data, or serve public/admin UI pages.

## Production host

Azure Function App:

- `aga-chapter-rewards-automation`
- `https://aga-chapter-rewards-automation.azurewebsites.net`

## Functions

- `create_rewards_daily_snapshot`
- `process_rewards_membership_awards`
- `process_rewards_rated_game_awards`
- `process_rewards_tournament_awards`
- `process_rewards_point_expirations`
- `send_pending_chapter_renewals_email`

## Settings

- `SQL_CONNECTION_STRING`
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

The Gmail settings are only used to send the pending chapter-renewal digest. This app never reads or labels inbound mailbox messages.

## Cutover

Production cutover was completed on 2026-06-26.

Current production ownership:

- `aga-chapter-rewards-automation` has the rewards timers enabled.
- `aga-clubexpress-mail` keeps mailbox polling enabled and has the duplicate rewards timers disabled.

The cutover steps were:

1. Deploy this app to `aga-chapter-rewards-automation`.
2. Copy the rewards, Gmail-send, and SQL settings from `aga-clubexpress-mail`.
3. Verify the six timer functions are registered on the new app.
4. Disable the rewards timers on `aga-clubexpress-mail` by setting:
   - `REWARDS_SNAPSHOT_ENABLED=false`
   - `REWARDS_MEMBERSHIP_AWARDS_ENABLED=false`
   - `REWARDS_RATED_GAME_AWARDS_ENABLED=false`
   - `REWARDS_TOURNAMENT_AWARDS_ENABLED=false`
   - `REWARDS_EXPIRATIONS_ENABLED=false`
   - `PENDING_CHAPTER_RENEWALS_EMAIL_ENABLED=false`
5. Keep `CLUBEXPRESS_MAILBOX_ENABLED` enabled only on `aga-clubexpress-mail`.

Deploy from this folder:

```powershell
func azure functionapp publish aga-chapter-rewards-automation --python --build remote
```
