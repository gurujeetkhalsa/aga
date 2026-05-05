# Chapter Renewal Email Results Restart Memo

Date: 2026-05-05

Branch: `codex/bayrate-clean-python`

## Context

This session added processing for ClubExpress emails with subject `Membership Renewal Emails`.

The use case is automatic chapter membership renewal using Chapter Rewards points:

- parse the ClubExpress renewal-notice matrix
- keep only rows where `Type = Chapter`
- treat the first matrix column, `Member`, as the ClubExpress `ChapterID`
- check whether the chapter has at least `35,000` available points
- debit `35,000` points when enough points are available
- record both posted and not-posted decisions so the rewards coordinator can act

## Code Added

Mail processing was added in:

```text
clubexpress-mail-app/function_app.py
clubexpress-mail-app/tests/test_journal_parser.py
clubexpress-mail-app/README.md
```

The mail function now recognizes:

```text
Membership Renewal Emails
```

as `chapter_renewal_notice`.

It parses HTML or text tables, extracts chapter renewal notice rows, and calls:

```text
rewards.sp_process_chapter_renewal_notices
```

SQL support was added in:

```text
rewards/sql/chapter_renewal_notice_processing.sql
```

It creates:

```text
rewards.chapter_renewal_notice_results
rewards.sp_process_chapter_renewal_notices
```

The procedure is idempotent by `Message_ID + ChapterID`.

Posted renewals create:

- `rewards.redemption_requests` rows
- `rewards.transactions` debit rows with `Source_Type = chapter_auto_renewal`
- `rewards.lot_allocations`
- point-lot balance updates

Not-posted renewals are recorded as:

- `insufficient_points`
- `chapter_not_found`
- `already_posted`

Reporting support was added in:

```text
rewards/reports.py
rewards/tests/test_reports.py
```

New CLI report:

```text
py -3 -m rewards.reports chapter-renewal-notices --top 50
```

The current web rewards report URL is:

```text
https://aga-ratings-explorer.azurewebsites.net/api/ratings-explorer/rewards
```

That page currently shows balances/chapter detail. It does not yet expose the chapter renewal notice result table.

## Production Work Completed

The SQL file was applied to Azure SQL.

The mail function app was deployed to:

```text
aga-clubexpress-mail
```

The deployment listed these functions:

- `create_rewards_daily_snapshot`
- `poll_clubexpress_mailbox`
- `process_rewards_membership_awards`
- `process_rewards_point_expirations`
- `process_rewards_rated_game_awards`
- `process_rewards_tournament_awards`

Gmail OAuth had expired/revoked its refresh token. A new refresh token was obtained and stored in Azure as:

```text
GOOGLE_WORKSPACE_REFRESH_TOKEN
```

The refreshed token currently has only:

```text
https://www.googleapis.com/auth/gmail.modify
```

It does not have `gmail.send`.

## May 4 Renewal Email Run

The user requested that the new renewal processing run only for the email received on May 4, 2026.

The query was temporarily narrowed to:

```text
in:inbox -label:ProcessedByFunction subject:"Membership Renewal Emails" after:2026/5/4 before:2026/5/5
```

The May 4, 2026 renewal email had Gmail message id:

```text
19df31a6e7d1c1ab
```

Processing results:

| Chapter | ChapterID | Decision | Points | TransactionID |
| --- | ---: | --- | ---: | ---: |
| BAIN | 31880 | insufficient_points | 21,000 available / 35,000 required | |
| SHPO | 14182 | posted | -35,000 | 1269 |
| SATX | 30692 | posted | -35,000 | 1270 |

RunID:

```text
20
```

Notice rows remaining in production after cleanup:

- NoticeID `1`: BAIN, insufficient points
- NoticeID `2`: SHPO, posted
- NoticeID `3`: SATX, posted

## Older Email Cleanup

An earlier broad mailbox poll also processed two older `Membership Renewal Emails` messages.

Those effects were intentionally rolled back because the user asked to run only the email received on May 4, 2026.

Rolled back:

| Message_ID | NoticeID | ChapterID | RedemptionID | TransactionID | RunID |
| --- | ---: | ---: | ---: | ---: | ---: |
| 19de3a99890a56a6 | 4 | 30241 | 31 | 1271 | 21 |
| 19d86f0aaf6f1efd | 5 | 15456 | 32 | 1272 | 22 |

The rollback restored point-lot allocations and removed the auto-created notice rows, redemption rows, transaction rows, and now-empty runs for those two older messages.

Verification after rollback showed only the May 4 renewal notice results and only the two intended `chapter_auto_renewal` transactions.

## Current Mailbox Query

After the May 4-only run, the live mailbox query was restored so normal ClubExpress processing can resume, while keeping renewal notices forward-looking only:

```text
in:inbox -label:ProcessedByFunction ((subject:"New Member Signup - Payment" OR subject:"American Go Association - Member Renewal" OR subject:"American Go E - Journal" OR subject:"Weekly American Go E - Journal" OR has:attachment) OR (subject:"Membership Renewal Emails" after:2026/5/4))
```

This means:

- existing signup, renewal, journal, and attachment parsing should continue
- `Membership Renewal Emails` before May 4, 2026 should stay out of the automation path
- future `Membership Renewal Emails` can be processed

## Verification Already Run

Local checks:

```text
py -3 -m py_compile clubexpress-mail-app\function_app.py rewards\reports.py
py -3 -m unittest clubexpress-mail-app.tests.test_journal_parser rewards.tests.test_reports
py -3 -m unittest discover clubexpress-mail-app\tests
py -3 -m unittest discover rewards\tests
git diff --check -- clubexpress-mail-app\function_app.py rewards\reports.py clubexpress-mail-app\tests\test_journal_parser.py rewards\tests\test_reports.py docs\chapter-rewards-design.md docs\deployment-memo.md clubexpress-mail-app\README.md rewards\sql\chapter_renewal_notice_processing.sql
```

Production checks:

```text
py -3 -m rewards.reports chapter-renewal-notices --top 50 --json
py -3 -m rewards.reports transactions --source-type chapter_auto_renewal --top 20 --json
```

Azure CLI prints a local 32-bit Python cryptography warning. It did not block the work.

## Notification Status

No coordinator notification email was sent for the May 4 processing run.

Reason:

- the code contains a summary-email hook
- `CHAPTER_RENEWAL_NOTICE_EMAIL_TO` is not set in Azure
- the Gmail token is authorized only for `gmail.modify`, not `gmail.send`

Relevant optional app settings:

```text
CHAPTER_RENEWAL_NOTICE_EMAIL_TO
CHAPTER_RENEWAL_NOTICE_EMAIL_FROM
```

## Next Session

Primary next step: enable Gmail sends and expose/report the processing results.

Suggested steps:

1. Re-authorize Gmail with both scopes:

```text
https://www.googleapis.com/auth/gmail.modify
https://www.googleapis.com/auth/gmail.send
```

2. Store the new refresh token in `aga-clubexpress-mail`:

```text
GOOGLE_WORKSPACE_REFRESH_TOKEN
```

3. Set the coordinator recipient:

```text
CHAPTER_RENEWAL_NOTICE_EMAIL_TO=<rewards coordinator email>
```

4. Run a send-capability test, ideally without reprocessing old renewal notices.

5. Decide the result-reporting UX:

- email summary only
- web report endpoint in `aga-ratings-explorer`
- both

6. If adding the web report, expose `rewards.chapter_renewal_notice_results` in the Ratings Explorer rewards app. The current public rewards page is:

```text
https://aga-ratings-explorer.azurewebsites.net/api/ratings-explorer/rewards
```

Likely implementation files for the web report:

```text
ratings-explorer-app/function_app.py
ratings-explorer-app/rewards_report.html
```

7. Consider a coordinator-focused email format:

- posted renewals with chapter, ChapterID, points debited, and transaction ID
- insufficient-point chapters with available and required points
- chapter-not-found rows with the raw ClubExpress ChapterID
- source Gmail message id and received timestamp

## Important Cautions

- Do not broaden the `Membership Renewal Emails` query to include old messages unless the user explicitly asks for a backfill.
- Keep idempotency by `Message_ID + ChapterID`; do not rely only on email subject/date.
- Do not manually delete or relabel Gmail messages unless explicitly asked.
- The worktree contains unrelated modified and untracked files. Do not clean or revert them.
