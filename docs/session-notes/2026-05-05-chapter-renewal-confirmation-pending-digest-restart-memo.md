# Chapter Renewal Confirmation and Pending Digest Restart Memo

Date: 2026-05-05

Branch: `codex/bayrate-clean-python`

## Context

This session continued the automatic chapter-renewal workflow after the initial May 4 processing.

The important operational correction is that a Chapter Rewards debit is not the end of the workflow. The rewards coordinator still renews the chapter in ClubExpress. When ClubExpress completes that renewal, it sends the normal `American Go Association - Member Renewal` email that the mailbox processor already handles.

Before this change, chapter renewal emails were processed and marked with the Gmail processed label, but they did not update the Chapter Rewards auto-renewal notice results because `Member Type = Chapter` is not a normal member reward event.

## Gmail Authorization

The Gmail OAuth client was updated in Google Cloud to allow:

```text
http://127.0.0.1:8765/
```

The Gmail refresh token in Azure Function App `aga-clubexpress-mail` was reauthorized and stored with both scopes:

```text
https://www.googleapis.com/auth/gmail.modify
https://www.googleapis.com/auth/gmail.send
```

Production verification showed the live token has both scopes.

The coordinator settings are:

```text
CHAPTER_RENEWAL_NOTICE_EMAIL_TO=rewards@usgo.org
CHAPTER_RENEWAL_PENDING_EMAIL_TO=rewards@usgo.org
```

A one-time May 4 summary email was sent from `clubexpress@usgo.org` to `rewards@usgo.org`.

Gmail send result:

```text
message id: 19df8632e8c020c3
thread id: 19df8632e8c020c3
```

## Code Added

Mail app changes are in:

```text
clubexpress-mail-app/function_app.py
clubexpress-mail-app/tests/test_journal_parser.py
clubexpress-mail-app/README.md
```

The mailbox processor now:

- records chapter renewal confirmation when a processed renewal email has `Member Type = Chapter`
- calls `rewards.sp_record_chapter_renewal_confirmation`
- adds timer function `send_pending_chapter_renewals_email`
- sends a nightly pending-renewal digest for chapters debited by rewards but not yet confirmed by a ClubExpress chapter renewal email
- sends the digest to `CHAPTER_RENEWAL_PENDING_EMAIL_TO`, falling back to `CHAPTER_RENEWAL_NOTICE_EMAIL_TO`

Default pending digest schedule:

```text
PENDING_CHAPTER_RENEWALS_EMAIL_SCHEDULE=0 50 5 * * *
```

That is `05:50 UTC`.

## SQL Added

SQL changes are in:

```text
rewards/sql/chapter_renewal_notice_processing.sql
```

New confirmation columns on `rewards.chapter_renewal_notice_results`:

```text
ClubExpress_Renewal_Message_ID
ClubExpress_Renewed_At
ClubExpress_Renewal_Recorded_At
ClubExpress_Renewal_Source_Payload_Json
```

New procedures:

```text
rewards.sp_record_chapter_renewal_confirmation
rewards.sp_backfill_chapter_renewal_confirmations
rewards.sp_get_pending_chapter_renewals
```

New pending index:

```text
IX_chapter_renewal_notice_results_Pending
```

Confirmation behavior:

- only applies to renewal emails where `Member Type` starts with `Chapter`
- matches by `ChapterID`
- updates the oldest posted or already-posted notice with a transaction and no existing ClubExpress confirmation
- only matches notices whose reward notice email was received before the ClubExpress renewal email
- returns `no_pending_debit` when a chapter renewal email has no open rewards debit to close

## Reports Added

Reporting changes are in:

```text
rewards/reports.py
rewards/tests/test_reports.py
```

New CLI report:

```text
py -3 -m rewards.reports pending-chapter-renewals --top 25 --json
```

The existing `chapter-renewal-notices` report now includes:

```text
ClubExpress_Renewal_Message_ID
ClubExpress_Renewed_At
ClubExpress_Renewal_Recorded_At
Pending_Days
```

## Production Work Completed

The SQL migration was applied to production Azure SQL.

Initial backfill was run:

```text
rewards.sp_backfill_chapter_renewal_confirmations
```

The first backfill query path reported two updates but did not commit because the local SQL adapter used the query fallback path. It was rerun through the committed execution path, and the updates persisted.

Backfill confirmed the two May 4 posted chapters:

| Chapter | ChapterID | ClubExpress Renewal Message ID | Renewed At |
| --- | ---: | --- | --- |
| SHPO | 14182 | 19df635d4c20f249 | 2026-05-05T03:36:51 |
| SATX | 30692 | 19df637b84311e9a | 2026-05-05T03:38:55 |

BAIN remains `insufficient_points` and is not part of the debited-but-unrenewed pending list.

Production pending report after backfill:

```json
[]
```

The updated `clubexpress-mail-app` was deployed to:

```text
aga-clubexpress-mail
```

Azure listed these deployed functions:

- `create_rewards_daily_snapshot`
- `poll_clubexpress_mailbox`
- `process_rewards_membership_awards`
- `process_rewards_point_expirations`
- `process_rewards_rated_game_awards`
- `process_rewards_tournament_awards`
- `send_pending_chapter_renewals_email`

## Verification Run

Local checks:

```text
py -3 -m py_compile clubexpress-mail-app\function_app.py rewards\reports.py
py -3 -m unittest discover clubexpress-mail-app\tests
py -3 -m unittest discover rewards\tests
git diff --check -- clubexpress-mail-app\function_app.py rewards\reports.py clubexpress-mail-app\tests\test_journal_parser.py rewards\tests\test_reports.py docs\chapter-rewards-design.md docs\deployment-memo.md clubexpress-mail-app\README.md rewards\sql\chapter_renewal_notice_processing.sql
```

Production checks:

```text
py -3 -m rewards.reports pending-chapter-renewals --top 25 --json
py -3 -m rewards.reports chapter-renewal-notices --top 10 --json
az functionapp function list --name aga-clubexpress-mail --resource-group aga-data-platform --query "[].name" -o tsv
```

Production `chapter-renewal-notices` showed SHPO and SATX with non-null ClubExpress renewal message IDs and null `Pending_Days`.

Azure CLI continues to print a local 32-bit Python cryptography warning. It did not block the work.

## Next Session Notes

- The nightly pending digest is live and should email `rewards@usgo.org` only when the timer runs. It sends an email even when no rows are pending unless disabled or changed later.
- The pending list should remain empty for the May 4 SHPO/SATX debits because their ClubExpress renewal emails were already processed and backfilled.
- Future posted chapter renewal notices will stay in `pending-chapter-renewals` until the corresponding chapter renewal email arrives and is processed.
- Do not broaden the Gmail query for old `Membership Renewal Emails` unless explicitly doing a backfill.
- The working tree contains unrelated modified and untracked files. Keep future commits scoped carefully.
