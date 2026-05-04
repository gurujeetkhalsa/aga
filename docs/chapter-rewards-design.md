# Chapter Rewards Design

Date: 2026-05-02
Status: Initial design draft

## Purpose

Automate the AGA Chapter Rewards program with daily calculation, transparent audit history, and a durable points ledger. The first implementation phase establishes the date-based snapshot foundation, membership award events, rated-game awards, tournament-host awards, State Championship awards, and expirations. Transfers and redemptions can then be layered on top of the same ledger model.

Initial SQL schema draft: `rewards/sql/chapter_rewards_schema.sql`.
Membership event logging procedure: `rewards/sql/membership_event_logging.sql`.
Snapshot generator CLI: `py -3 -m rewards.snapshot_generator --date YYYY-MM-DD --dry-run`.
Membership award CLI: `py -3 -m rewards.membership_awards --date YYYY-MM-DD --dry-run`.
Rated game award CLI: `py -3 -m rewards.rated_game_awards --date YYYY-MM-DD --dry-run`.
Automated snapshot procedure: `rewards.sp_create_daily_snapshot`.
Membership award procedure: `rewards.sp_process_membership_awards`.
Rated-game award procedure: `rewards.sp_process_rated_game_awards`.
Opening balance import procedure: `rewards.sp_import_opening_balances`.
Opening balance import CLI: `py -3 -m rewards.opening_balances C:\path\to\balances.pdf --effective-date YYYY-MM-DD --dry-run`.
Point expiration procedure: `rewards.sp_process_point_expirations`.
Point expiration CLI: `py -3 -m rewards.expirations --date YYYY-MM-DD --dry-run`.
Legacy redemption procedure: `rewards.sp_import_legacy_redemptions`.
Reporting views: `rewards/sql/reporting_views.sql`.
Reporting CLI: `py -3 -m rewards.reports balances`.

## Confirmed Program Rules

- Points are valued at `$0.001` each.
- Unused points expire two years after their earned date.
- Redemptions consume points first-in-first-out by original earned date.
- Transferred points keep their original earned date and expiration date.
- Opening balances imported from the legacy spreadsheet system are grandfathered as if earned on the system go-live date.
- Redemption eligibility is based on the redemption request date.
- Chapter size counts active members only.
- All active member types count toward chapter size except `Tournament Pass`.
- A chapter must be current in AGA dues to earn new points.

## Award Rules In Scope For Phase One

### Membership Awards

Base awards:

- Adult Full new membership: `5,000` base points.
- Adult Full renewal: `5,000` base points.
- Youth new membership: `2,000` base points.
- Youth renewal: `2,000` base points.
- Adult Full Lifetime membership: `25,000` base points.
- Tournament Pass: no points.

The base award is multiplied by the eligible chapter's size multiplier:

- Fewer than 5 active members: `3x`.
- 5-9 active members: `2x`.
- 10 or more active members: `1x`.

Membership events are creditable only once:

- If the member has a chapter on the membership event date, that chapter receives the award.
- If the member has no chapter on the event date, the event remains pending for 30 days.
- If the member joins a chapter within 30 days, the first eligible chapter observed receives the award.
- If no chapter is found within 30 days, the event expires with no award.
- If the member later leaves that chapter or joins another chapter, the award is not reassigned.

Current policy does not allow multi-year renewals, so normal renewal awards have a one-year value. The event model can still retain a `term_years` field defaulted to `1` to avoid a later schema change if the policy changes.

### Rated Game Awards

Each eligible rated game can create up to two participant awards:

- one award for player 1
- one award for player 2

Base award:

- `500` base points per eligible player appearance.

Eligibility:

- game is AGA rated
- game is not online
- game is not excluded
- player has an eligible chapter affiliation on the game date
- chapter is current in AGA dues on the game date

The award is calculated as:

```text
500 base points * chapter multiplier on the game date
```

The earned date for rated-game points is the game date, even if the tournament is submitted or processed later.

## Date Model

Every award calculation should track three dates:

- `earned_date`: date the points were earned and the expiration clock begins.
- `valuation_date`: date used for chapter affiliation, chapter active member count, multiplier, member type, and chapter eligibility.
- `posted_at`: timestamp when the automated process wrote the transaction.

For rated games:

- `earned_date = game_date`
- `valuation_date = game_date`
- `posted_at = daily run timestamp`

For membership awards:

- If the member already has a chapter on the event date:
  - `earned_date = event_date`
  - `valuation_date = event_date`
- If the member joins a chapter within the 30-day window:
  - `earned_date = first eligible chapter affiliation date observed by the system`
  - `valuation_date = same date`
- If no chapter is found within 30 days, no points are earned.

For legacy opening balances:

- `earned_date = go-live date`
- `valuation_date = go-live date`
- `posted_at = import timestamp`

## Snapshot Foundation

Daily snapshots are the foundation for repeatable calculation. Award jobs should calculate from snapshot tables rather than directly from mutable current-state tables.

### `rewards.reward_runs`

One row per automated or manual reward calculation run.

Suggested columns:

- `run_id`
- `run_type`
- `snapshot_date`
- `started_at`
- `completed_at`
- `status`
- `summary_json`
- `error_message`

### `rewards.member_daily_snapshot`

One row per member per snapshot date.

Suggested columns:

- `snapshot_date`
- `agaid`
- `member_type`
- `expiration_date`
- `chapter_id`
- `chapter_code`
- `is_active`
- `is_tournament_pass`

Derived rules:

- `is_active = expiration_date >= snapshot_date`
- `is_tournament_pass = member_type = 'Tournament Pass'`

### `rewards.chapter_daily_snapshot`

One row per chapter per snapshot date.

Suggested columns:

- `snapshot_date`
- `chapter_id`
- `chapter_code`
- `chapter_name`
- `is_current`
- `active_member_count`
- `multiplier`

Derived rules:

- `active_member_count` counts active affiliated members, excluding `Tournament Pass`.
- `multiplier = 3` when active member count is less than 5.
- `multiplier = 2` when active member count is between 5 and 9.
- `multiplier = 1` when active member count is 10 or more.

The source for `is_current` still needs to be confirmed or added.

## Membership Event Source

ClubExpress new-member and renewal emails are already processed by the membership pipeline. Going forward, that processing should also log a durable reward source event.

Implementation note: both email-processing app copies call `membership.sp_process_new_member_email` or `membership.sp_process_membership_renewal` and then `rewards.sp_record_membership_event` in the same SQL transaction.

Deployment note: apply `rewards/sql/chapter_rewards_schema.sql` and `rewards/sql/membership_event_logging.sql` before deploying the email-processing app change, so membership emails are retried rather than silently losing reward source events if the rewards procedure is unavailable.

### `rewards.membership_events`

Suggested columns:

- `membership_event_id`
- `message_id`
- `agaid`
- `event_type`
- `event_date`
- `received_at`
- `member_type`
- `base_points`
- `term_years`
- `credit_deadline`
- `status`
- `credited_transaction_id`
- `expired_at`
- `source_payload_json`

Recommended statuses:

- `pending`
- `credited`
- `expired_no_chapter`
- `ineligible`

Idempotency should use the email/message identity plus AGAID and event type so reprocessing an email does not create duplicate source events.

## Ledger Foundation

Phase one can calculate awards directly into a ledger shape even before redemptions are built. That keeps the later reimbursement and expiration work from requiring a migration.

### `rewards.transactions`

Immutable ledger of point movements.

Suggested columns:

- `transaction_id`
- `chapter_id`
- `chapter_code`
- `transaction_type`
- `points_delta`
- `base_points`
- `multiplier`
- `chapter_active_member_count`
- `earned_date`
- `valuation_date`
- `posted_at`
- `run_id`
- `source_type`
- `source_key`
- `rule_version`
- `metadata_json`

Unique source keys should prevent duplicate awards:

- membership: `membership_event_id`
- rated game participation: `Game_ID + ':' + AGAID`

### `rewards.point_lots`

One row per positive earn transaction.

Suggested columns:

- `lot_id`
- `earn_transaction_id`
- `chapter_id`
- `chapter_code`
- `original_points`
- `remaining_points`
- `earned_date`
- `expires_on`
- `source_type`
- `source_key`

Expiration should be based on `earned_date + 2 years`.

### `rewards.lot_allocations`

Maps debit transactions to the lots they consume. This is needed for redemptions, expirations, transfers, and corrections.

Suggested columns:

- `allocation_id`
- `debit_transaction_id`
- `lot_id`
- `points_allocated`
- `allocated_at`

## Daily Processing Order

1. Create a `reward_runs` row for the snapshot date.
2. Build `member_daily_snapshot` for the snapshot date from `membership.members`.
3. Build `chapter_daily_snapshot` for the snapshot date from member snapshots and chapter data.
4. Process pending membership events whose event date or 30-day window can be evaluated from available snapshots.
5. Process uncredited rated-game participant awards using the game date snapshots.
6. Process hosted-tournament and State Championship awards from committed BayRate tournament metadata.
7. Expire unused point lots whose expiration date is before the processing date.
8. Record summary counts, warnings, and exceptions on the run.

Current implementation:

- `rewards.snapshot_generator` builds the member and chapter snapshots.
- `aga-clubexpress-mail` runs `rewards.sp_create_daily_snapshot` daily at `05:10 UTC` by default.
- `aga-clubexpress-mail` runs `rewards.sp_process_membership_awards` daily at `05:20 UTC` by default.
- `aga-clubexpress-mail` runs `rewards.sp_process_rated_game_awards` daily at `05:30 UTC` by default.
- `aga-clubexpress-mail` runs `rewards.sp_process_tournament_awards` daily at `05:35 UTC` by default.
- `aga-clubexpress-mail` runs `rewards.sp_process_point_expirations` daily at `05:40 UTC` by default.
- The daily timers are controlled by `REWARDS_SNAPSHOT_ENABLED`, `REWARDS_SNAPSHOT_SCHEDULE`, `REWARDS_MEMBERSHIP_AWARDS_ENABLED`, `REWARDS_MEMBERSHIP_AWARDS_SCHEDULE`, `REWARDS_RATED_GAME_AWARDS_ENABLED`, `REWARDS_RATED_GAME_AWARDS_SCHEDULE`, `REWARDS_TOURNAMENT_AWARDS_ENABLED`, `REWARDS_TOURNAMENT_AWARDS_SCHEDULE`, `REWARDS_EXPIRATIONS_ENABLED`, and `REWARDS_EXPIRATIONS_SCHEDULE`.
- The SQL procedure returns existing snapshot counts and does not create duplicate snapshots when rerun for the same date.
- By default it refuses to overwrite existing snapshots for a date.
- Use `--replace` only when intentionally rebuilding a date before downstream awards have been posted from that snapshot.
- Use `--dry-run` to preview counts without writing.
- `rewards.rated_game_awards` creates one earn transaction and point lot per eligible rated-game player appearance.
- Rated-game awards require member and chapter snapshots for the game date; missing snapshots block awards rather than using current membership state.
- Rated-game source keys use `rated_game_participation:<Game_ID>:<AGAID>` to prevent duplicate awards on reruns.
- `rewards.membership_awards` creates one earn transaction and point lot per eligible pending membership event, then marks the source event `credited`.
- Pending membership events expire as `expired_no_chapter` only after the 30-day deadline and only when snapshot coverage exists through the deadline.
- `rewards.sql.reporting_views` exposes balances, transaction history, point-lot aging, run history, and membership event audit views.
- `rewards.reports` provides terminal reports for `balances`, `transactions`, `lots`, `runs`, and `membership-events`.
- `rewards.opening_balances` imports the legacy balance sheet as one grandfathered earn transaction and point lot per chapter with a positive opening balance.
- Opening balance lots use the chosen effective date as `earned_date`, `valuation_date`, and the start of the two-year expiration window.
- Opening balance imports also count current snapshot chapters that are absent from the PDF as zero-balance setup rows. Those chapters do not receive zero-point transactions because the ledger stores only nonzero point movements.
- The imported `data1.pdf` opening balance sheet reports balances as of `2026-02-08`, but the legacy balances are intentionally grandfathered into the new ledger as of the system start date, `2026-05-02`. Those grandfathered lots expire on `2028-05-02`.
- `rewards.sp_import_legacy_redemptions` loads redemptions that happened after the `2026-02-08` source balance report and on or before the `2026-05-02` ledger start. These rows keep their actual redemption request date as `effective_date`/`valuation_date`, are metadata-tagged `legacy_gap`, and allocate against opening-balance lots.
- `rewards.sp_import_legacy_redemptions_with_adjustments` can also create tagged `legacy_dues_credit_adjustment` lots when a chapter-dues credit exceeds the available opening-balance lot. These adjustments are for non-cash dues credits only and should not be used for reimbursement redemptions.
- Legacy redemption payment modes distinguish chapter dues credits (`dues_credit`, no cash payment) from promotion reimbursements (`reimbursement`).
- `rewards.expirations` consumes expired point lots by creating `expire` debit transactions, writing `lot_allocations`, and setting each consumed lot's `remaining_points` to zero. Lots are still available on their `expires_on` date and first expire when `expires_on < as_of_date`.
- `rewards.tournament_awards` groups committed `ratings.tournaments` rows by host chapter plus `Reward_Event_Key`, totals rated, non-excluded games across all sections in the group, and posts only the positive top-up if a later section increases the formula result.
- The tournament-host formula is `0` points when `games <= 15`, `1,000,000` points when `games >= 700`, otherwise `1000 * ((games - 15) / (700 - 15)) ^ 0.93 * 1000`, rounded to the nearest whole point.
- If any tournament section in a grouped event is marked `Reward_Is_State_Championship`, `rewards.tournament_awards` posts a separate `200,000` point State Championship award once for that host chapter and grouped event.

## Later Layers

The following features should be implemented on top of the same transaction, point lot, and allocation model:

- Chapter-to-chapter point transfers that preserve original lot earned dates.
- Redemption requests, receipt handling, review, approval, and reimbursement.
- Manual adjustments and reversals with required notes.
- Public balance reporting and admin audit views.

## Open Questions

- Where should chapter dues/current status be sourced from?
- What should the redemption request and receipt review workflow look like?
- Should transfers preserve the original lot expiration dates exactly or create fresh receiving lots with carried expiration metadata?
