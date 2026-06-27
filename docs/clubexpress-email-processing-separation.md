# ClubExpress Email Processing Separation

## Goal

Make ClubExpress mailbox processing replayable and narrow:

`Gmail message -> archived raw artifacts -> parsed event -> membership update -> rewards source event -> reward transaction`

The mailbox poller should not directly post rewards transactions. Rewards posting should happen from normalized source events with durable idempotency keys.

## Migration Order

1. Move rewards timers out of `clubexpress-mail-app` into `chapter-rewards-automation-app` with no behavior change.
2. Extract ClubExpress parsers into a shared parser module with fixture-based tests.
3. Add or formalize durable raw-message, parsed-event, processing-attempt, and downstream-result records.
4. Convert one email type at a time to the staged flow, starting with new-member and renewal emails.
5. Add replay/preview tooling before disabling old direct stored-procedure calls.
6. Keep `aga-clubexpress-mail` as the only live Gmail poller throughout.

## Completed Steps

`chapter-rewards-automation-app/` has been added and deployed as the standalone rewards timer host.

It contains:

- rewards snapshot creation
- membership awards
- rated-game awards
- tournament awards
- point expirations
- pending chapter-renewal digest email

It does not contain:

- Gmail mailbox polling
- inbound ClubExpress message classification
- inbound message labeling
- raw artifact archiving
- membership CSV imports
- ClubExpress email parsing
- public or admin UI routes

## Cutover Guardrail

Do not run the same rewards timers in both hosts.

The production cutover was completed on 2026-06-26. These settings are disabled on `aga-clubexpress-mail`:

- `REWARDS_SNAPSHOT_ENABLED=false`
- `REWARDS_MEMBERSHIP_AWARDS_ENABLED=false`
- `REWARDS_RATED_GAME_AWARDS_ENABLED=false`
- `REWARDS_TOURNAMENT_AWARDS_ENABLED=false`
- `REWARDS_EXPIRATIONS_ENABLED=false`
- `PENDING_CHAPTER_RENEWALS_EMAIL_ENABLED=false`

Keep `CLUBEXPRESS_MAILBOX_ENABLED=true` only on `aga-clubexpress-mail`.

## Next Step

Extract the remaining parser functions from `clubexpress-mail-app/function_app.py` into parser-only modules. Parser extraction is complete for:

- new-member emails, membership renewal emails, and chapter-renewal notice emails in `clubexpress-mail-app/clubexpress_parsers.py`
- MemChap, ChapterX, and member-category CSV reports in `clubexpress-mail-app/clubexpress_csv_parsers.py`

Parsed-event staging has been added for:

- new-member signup emails
- member-renewal emails
- chapter-renewal notice emails
- E-Journal emails, including NAOL review matches
- MemChap, ChapterX, and member-category CSV emails

The SQL migration is:

`clubexpress-mail-app/sql/clubexpress_parsed_event_staging.sql`

It creates:

- `membership.clubexpress_parsed_events`
- `membership.clubexpress_parsed_event_attempts`
- `membership.sp_record_clubexpress_parsed_event`
- `membership.sp_update_clubexpress_parsed_event_status`

The mailbox app records parsed events only when `CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED=true`. Keep this false until the SQL migration has been applied in the target database.

Parsed-event preview/replay tooling has been added in:

`clubexpress-mail-app/clubexpress_replay.py`

It can:

- list recent staged events
- preview one staged event and its downstream procedure plan
- replay one staged event with `--execute --confirm-replay`
- require `--allow-processed` before replaying an event already marked `processed`

Staged new-member consumption has been added behind explicit flags:

- `CLUBEXPRESS_STAGED_NEW_MEMBER_CONSUMPTION_ENABLED`
- `CLUBEXPRESS_STAGED_NEW_MEMBER_PROCESSOR_ENABLED`

When both are enabled, the Gmail poller only archives, parses, and stages `new_member_signup` emails. The staged new-member timer consumes `new_membership` rows from `membership.clubexpress_parsed_events`, runs the downstream membership/rewards procedures from `Downstream_Payload_Json`, and writes processed/error attempts.

Staged processors have been added for:

- new-member signup emails
- member-renewal emails, including chapter-renewal confirmations
- chapter-renewal notice emails, including the summary email after reward debit decisions
- MemChap CSV imports
- ChapterX CSV imports
- member-category CSV imports
- E-Journal emails, including NAOL review matches

## Next Step

Keep watching the next real message for each staged type after deployment, then split BayRate out of the mixed `ratings-explorer-app` into its own function app.
