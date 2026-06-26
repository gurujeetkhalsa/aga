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
- replaying an event already marked `processed` also requires `--allow-processed`

Migration note:

- this is a first-pass split from the legacy monolith
- helper code is still duplicated locally for safety and will be reduced later
