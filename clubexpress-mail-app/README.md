# ClubExpress Mail App

This app contains:

- Gmail mailbox polling
- ClubExpress message classification
- attachment extraction and archiving
- journal parsing
- NAOL review parsing
- automatic Chapter Rewards chapter-renewal notice processing
- optional durable parsed-event staging for parsed ClubExpress emails
- mailbox-driven import orchestration

Current migration source:

- `C:\Users\guruj\aga-functions\function_app.py`

Current trigger surface:

- timer: `poll_clubexpress_mailbox`

Chapter renewal notices:

- Gmail subject: `Membership Renewal Emails`
- rows where `Type = Chapter` are treated as chapter renewal candidates
- the `Member` column is parsed as `ChapterID`
- chapters with at least `35,000` available points receive an automatic `chapter_renewal` / `dues_credit` redemption
- every candidate is recorded in `rewards.chapter_renewal_notice_results`
- set `CHAPTER_RENEWAL_NOTICE_EMAIL_TO` to send a processing summary email
- chapter member renewal emails confirm the ClubExpress renewal side and clear matching debited chapters from the pending list
- a nightly pending-renewal digest is sent to `CHAPTER_RENEWAL_PENDING_EMAIL_TO`, or `CHAPTER_RENEWAL_NOTICE_EMAIL_TO` when the pending-specific setting is not set

Parsed-event staging:

- migration: `sql/clubexpress_parsed_event_staging.sql`
- setting: `CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED`
- keep the setting false until the migration is applied
- currently stages new-member signup, member-renewal, and chapter-renewal notice emails before the existing idempotent downstream procedures run

Parsed-event preview and replay:

- list recent staged events: `py -3 clubexpress-mail-app\clubexpress_replay.py list`
- preview one event: `py -3 clubexpress-mail-app\clubexpress_replay.py preview --event-key <event-key>`
- replay one event: `py -3 clubexpress-mail-app\clubexpress_replay.py replay --event-key <event-key> --execute --confirm-replay`
- process pending staged new-member events manually: `py -3 clubexpress-mail-app\clubexpress_replay.py process-new-members --execute --confirm-replay`
- process pending staged renewal events manually: `py -3 clubexpress-mail-app\clubexpress_replay.py process-renewals --execute --confirm-replay`
- process pending staged nightly MemChap CSV events manually: `py -3 clubexpress-mail-app\clubexpress_replay.py process-nightly-memchap --execute --confirm-replay`
- process pending staged ChapterX CSV events manually: `py -3 clubexpress-mail-app\clubexpress_replay.py process-chapterx --execute --confirm-replay`
- process pending staged member-category CSV events manually: `py -3 clubexpress-mail-app\clubexpress_replay.py process-member-categories --execute --confirm-replay`
- replaying an event already marked `processed` also requires `--allow-processed`

Staged new-member consumption:

- `CLUBEXPRESS_STAGED_NEW_MEMBER_CONSUMPTION_ENABLED=true` makes the Gmail poller stage `new_member_signup` emails without directly running downstream membership/rewards procedures
- `CLUBEXPRESS_STAGED_NEW_MEMBER_PROCESSOR_ENABLED=true` enables the timer that consumes staged `new_membership` events
- `CLUBEXPRESS_STAGED_NEW_MEMBER_PROCESSOR_SCHEDULE` defaults to every 5 minutes
- `CLUBEXPRESS_STAGED_NEW_MEMBER_PROCESSOR_BATCH_SIZE` defaults to `25`
- `CLUBEXPRESS_STAGED_RENEWAL_CONSUMPTION_ENABLED=true` makes the Gmail poller stage `member_renewal` emails without directly running downstream membership/rewards procedures
- `CLUBEXPRESS_STAGED_RENEWAL_PROCESSOR_ENABLED=true` enables the timer that consumes staged `renewal` events, including chapter-renewal confirmations
- `CLUBEXPRESS_STAGED_RENEWAL_PROCESSOR_SCHEDULE` defaults to every 5 minutes
- `CLUBEXPRESS_STAGED_RENEWAL_PROCESSOR_BATCH_SIZE` defaults to `25`
- `CLUBEXPRESS_STAGED_MEMCHAP_CONSUMPTION_ENABLED=true` makes the Gmail poller stage nightly `MemChap` CSV emails without directly importing `staging.memchap`
- `CLUBEXPRESS_STAGED_MEMCHAP_PROCESSOR_ENABLED=true` enables the timer that consumes staged `nightly_memchap_csv` events from the archived CSV attachment
- `CLUBEXPRESS_STAGED_MEMCHAP_PROCESSOR_SCHEDULE` defaults to every 5 minutes
- `CLUBEXPRESS_STAGED_MEMCHAP_PROCESSOR_BATCH_SIZE` defaults to `5`
- `CLUBEXPRESS_STAGED_CHAPTER_CONSUMPTION_ENABLED=true` makes the Gmail poller stage `ChapterX` CSV emails without directly importing `staging.chapters`
- `CLUBEXPRESS_STAGED_CHAPTER_PROCESSOR_ENABLED=true` enables the timer that consumes staged `chapter_csv` events from the archived CSV attachment
- `CLUBEXPRESS_STAGED_CHAPTER_PROCESSOR_SCHEDULE` defaults to every 5 minutes
- `CLUBEXPRESS_STAGED_CHAPTER_PROCESSOR_BATCH_SIZE` defaults to `5`
- `CLUBEXPRESS_STAGED_MEMBER_CATEGORIES_CONSUMPTION_ENABLED=true` makes the Gmail poller stage member-category CSV emails without directly importing `staging.member_categories`
- `CLUBEXPRESS_STAGED_MEMBER_CATEGORIES_PROCESSOR_ENABLED=true` enables the timer that consumes staged `nightly_member_categories_csv` events from the archived CSV attachment
- `CLUBEXPRESS_STAGED_MEMBER_CATEGORIES_PROCESSOR_SCHEDULE` defaults to every 5 minutes
- `CLUBEXPRESS_STAGED_MEMBER_CATEGORIES_PROCESSOR_BATCH_SIZE` defaults to `5`

Migration note:

- this is a first-pass split from the legacy monolith
- helper code is still duplicated locally for safety and will be reduced later
