# Workshop package verification — September 7, 2026

## September 9 welcome confirmation and OGS handles deployed

- Confirmed-place emails now use Milan's friendly personalized preparation style:
  "Hi [first name],", explicit place confirmation and scheduled date/time, guided
  Zoom/OGS format, clickable OGS registration link, request to reply with a handle,
  configured sender/signature, Zoom joining link, and calendar attachment.
- Added nullable registration_contacts.ogs_handle via idempotent migration 005
  at 18:00:44 UTC. Pre-migration database time: 18:00:43.846257 UTC. Prior package
  saved under ../_deploy/workshop-ogs-20260909/. Temporary exact-IP database rule
  removed and verified. No existing participant data was modified.
- Protected roster reads expose the handle; organizers can save or clear it.
  Updates enforce registration-management permission and the selected workshop
  and signup scope, use parameterized SQL, and create an audit entry. Handles
  are excluded from public event data and do not enqueue extra emails.
- All 50 tests passed with zero skips, including 15 SQL integration tests in
  disposable schemas. Desktop/mobile UI checks passed for save, reload, clear,
  and existing attendance/signup flows. Mobile screenshot visually inspected.
- Normal Azure remote deployment succeeded at 18:03:28 UTC, host Running.
  All five changed deployed runtime files matched tested source hashes. Public
  API returned 200; unauthenticated manager returned 401. Live signed-in browser
  showed the OGS handle field and Save OGS handle for an existing participant.
- Automatic delivery remains disabled. No live email was sent or OGS account
  created; Milan records handles when participants reply to the confirmation.

## September 9 personalized reminder greeting deployed

- User confirmed individual emails and requested each registrant's first name.
  Preparation reminders now begin "Hi [first name]," in HTML and plain text.
- All 18 existing message/policy/provider/auth tests passed on Python 3.11.9,
  with greeting and name-escaping checks added to existing reminder cases.
- Normal remote deployment completed its pipeline; downloaded package exactly
  matched tested workshop_messages.py. Public API 200 and anonymous manager 401.
  Only the greeting changed in runtime code. No email was sent; workers stay off.
- Rollback package and verification are in
  `../_deploy/workshop-personalized-20260909/`.

## September 9 Milan's preparation reminder deployed

- Applied the user's `Downloads/email24.txt` sample to reminders scheduled 24
  hours or more before a workshop. Both date references use the actual scheduled
  date/time in the event zone, including EDT/EST; no hard-coded date or "tomorrow".
- HTML links Online Go Server (OGS) to `https://online-go.com/register` and Zoom
  to the saved participant joining URL. Plain-text fallback, configured From and
  Reply-To, instructor signature, and calendar attachment are retained. One-hour
  reminders remain brief. Automatic email delivery remains disabled.
- All 18 message/policy/provider/auth tests passed on Python 3.11.9, including
  24/48-hour reminders, summer/winter date formatting, HTML escaping, links,
  attachments, and unchanged short reminders. No test emails were sent.
- The deployed package was downloaded and workshop_messages.py exactly matched
  the tested source SHA-256. It was the sole runtime source change from the prior
  production package. Public API returned 200 and anonymous manager API 401.
- Rollback package, deployment package, and readiness/verification receipts are
  in `../_deploy/workshop-reminder-20260909/`. No SQL migration or schedule change.

## September 9 compact workshop manager deployed

- Add Workshop is above the listing; each workshop starts as a compact summary
  with date/time, confirmed/capacity/waitlist counts, and a Cancel action. Selecting
  the summary opens its full editor. Settings and QR downloads are collapsed below.
- Desktop (1280px) and mobile (390px) browser tests passed, including keyboard
  expansion, save, creation, dismissed/confirmed cancellation, retained add drafts,
  roster/attendance, and no horizontal overflow. Screenshots were visually checked.
- All 46 Python tests passed with no skips, including 14 SQL tests in disposable
  schemas. The 16 workshop policy/provider/auth tests also passed on Python 3.11.9.
- Applied additive migration 004 at 16:56:48 UTC. The temporary exact-IP SQL
  firewall rule was removed and removal verified. Prior package is saved at
  `../_deploy/workshop-manager-20260909/previous-package.zip`; readiness and migration
  reports are alongside it. Pre-migration database time: 16:56:46.917426 UTC.
- Normal Azure remote build and deployment succeeded at 17:04:44 UTC; host Running
  and all 18 functions listed. An earlier forced retry was rejected by automatic
  approval review and was never executed. The successful retry used no bypass;
  Core Tools still warned about its discovered local Python 3.13 even with a real
  3.11 runtime on PATH, but Azure's remote build/validation completed successfully.
- Authenticated live browser verified all ten summaries, Add form opening above
  the list, and full details expanding. No real workshop or signup was changed.
  Public API returned 200; unauthenticated organizer API returned 401.
- Read-back confirms email and Zoom workers remain disabled. Real Zoom setup and
  end-to-end email/calendar delivery acceptance remain pending separately.

## September 9 Microsoft sign-in fully verified

- Fixed the follow-on `AADSTS650056` legacy Azure AD Graph request by changing
  Easy Auth's issuer to `https://login.microsoftonline.com/5f1a8692-caac-4040-9a1b-858e98f03e4b/v2.0`
  and explicitly requesting `openid profile email` via `additionalLoginParams`.
- Read-back matched. The client ID, secret setting reference, audiences, enabled
  authentication, token store, and application-level organizer restrictions were
  preserved. No Graph data permissions or organization-wide consent were added.
- Browser confirmed the `/oauth2/v2.0/authorize` endpoint. The existing
  `gurujeet@live.com` identity completed AGA Events' basic-profile consent and
  returned successfully to the manager. All ten workshops, their editing controls,
  and the Add a workshop form loaded. No dates were changed.
- This supersedes the incomplete callback check in the preceding repair notes.
  Full email/calendar and Zoom release acceptance remains a separate pending task.

## September 9 administrator sign-in correction

- Corrected AGA Events Entra app `cf20a3f1-6664-4bc6-9c4f-0a17473f96d3`:
  `web.implicitGrantSettings.enableIdTokenIssuance` was false, now true. Its legacy
  Easy Auth login actually requests `code id_token`, causing the reported
  `AADSTS700054` when ID tokens were disabled.
- Read-back verified the change; implicit access tokens remain disabled and
  the tenant-only audience and registered callback are unchanged.
- Fresh browser login passed through Microsoft's account picker to the callback.
  Edge then blocked the callback with `ERR_BLOCKED_BY_CLIENT`, so authenticated
  editor access still requires user verification in their browser.
- After the change, public event API returned 200 and an unauthenticated organizer
  API request returned 401. No workshop dates, roster data, or delivery flags changed.

## September 9 Zoom identity update

- User confirmed the selected host email is `milan.mladenovic.ja@gmail.com`.
- User reports the AGA Zoom invitation is still Pending. Acceptance, active
  status, and license verification remain outstanding; no Zoom API test or
  meeting creation has been performed.
- Notification sender remains `milan.mladenovic@usgo.org`.

## September 9 controlled delivery test passed

- User approved exactly one test message to `gurujeet.khalsa@usgo.org`.
- Sent through the package's `GmailSender` adapter using the workshop app's saved
  Azure credentials. Gmail accepted it at 16:16 UTC (12:16 PM Eastern).
- Verified one matching message in the recipient's Gmail **Inbox** using sender
  and exact subject: **AGA Free Beginners Workshop - email delivery test**.
- Provider message ID: `1a086f4c89af44c2`. Non-secret receipt is stored at
  `tmp/workshop-mail-test-20260909-receipt.json`; the sender script refuses repeats.
- This verifies a single plain-text message through the real sender. Calendar
  attachments, scheduled workflows, Zoom integration, and deployed registration
  flows still need their release acceptance checks. Automated delivery remains off.

## September 9 Gmail authorization completed

- User explicitly approved the existing AGA OAuth client, displayed by Google as
  **AGA ClubExpress Mailbox Reader**, for workshop sending and identity verification.
- Google consent displayed only **Send email on your behalf** for Gmail access.
  Verified the exact email and `email_verified` through Google's userinfo endpoint;
  refreshed the grant and verified the same identity again.
- Saved the client credentials, new mailbox refresh token, and sender allowlist
  only to `aga-events`; read-back matched. No source-app refresh token was reused.
- `EVENTS_ALLOWED_SENDERS=milan.mladenovic@usgo.org` and
  `EVENTS_EMAIL_ENABLED=false`. No message has been sent or delivered in this check.
- Live delivery test, Zoom setup, migration, and package deployment remain pending.
  The older absent-settings observations below describe the pre-authorization state.

## September 9 sender update

- User selected `milan.mladenovic@usgo.org`. Application and new-schema sender
  defaults, operator instructions, and test fixtures now use that mailbox.
- All 16 workshop policy/message/provider/authentication tests passed, including
  confirmation From and Reply-To checks. An initial sandbox run could not read
  existing test dependencies; the same suite passed with dependency access.
- Read-only Azure setting-name inspection confirms Gmail/Zoom credentials and
  worker flags remain absent. Live Gmail authorization and delivery are pending.
- Rebuilt the source archive. No production migration/deployment, actual email
  send, or Zoom mutation was performed for this change.

## QR download addition

- Added PNG and SVG invitation badges, the public download endpoints, and manager
  download buttons. The printed wording is “Free Beginners Online Workshop” above
  and “Learn Go in an hour - Register here.” below. The hyphen after Workshop was
  removed at the user's request. HTML entities and stray formatting in the request
  were normalized to ordinary text.
- Three focused QR tests passed: caption/vector structure, download responses and
  canonical-origin handling, and independent decoding of the full PNG at 1200,
  600 and 300 pixels. All decoded to the permanent workshop registration URL.
- The SVG was rendered in Chrome, visually inspected, and independently decoded
  to the same URL. The PNG was also visually inspected.
- Desktop/mobile manager checks passed again, including PNG/SVG button targets.
- Database workflows were unchanged by this addition; the earlier SQL results below
  still describe that verification scope. QR files are usable immediately; new manager
  buttons and endpoints require package deployment.

## Passed

- 43 Python tests: 13 original domain tests, 16 workshop policy/message/provider/
  authentication tests, and 14 real SQL Server integration tests.
- SQL tests used unique disposable schemas in the existing Azure SQL database.
  The test schemas were removed by the test runner. Production events tables were
  not modified. Migration 004 was tested for initial application and repeat application.
- Real SQL coverage: concurrent claims for the last place; waitlisting; idempotent
  retries; mismatched submissions; cancellation and promotion; event cancellation;
  organizer permissions; cross-event identifiers; attendance and past-date visibility;
  rollback when the outbox write fails; existing-signup notification backfill;
  Zoom create/update/delete state transitions; uncertain Zoom creation; confirmation
  waiting for a joining link; follow-up delivery; uncertain email delivery; and
  suppression of obsolete messages after cancellation.
- All Azure Functions entry points indexed with the Azure Functions Python library.
- Browser fixture tests at 1280px and 390px: settings updates, adding/cancelling
  signups, attendance changes, Zoom controls, and workshop cancellation. No JavaScript
  errors or horizontal page overflow. Screenshots saved under `test-results`.
- Python syntax compilation passed for all application modules and the package builder.

## Not yet verified

- Full notification/calendar workflows; the Gmail grant and one plain-text delivery are verified.
- Milan's actual licensed AGA Zoom host email and Server-to-Server OAuth credentials.
- Live Zoom meeting creation, update, cancellation and actual attendee joining.
- Production deployment, migration, Easy Auth role/scope acceptance, and controlled
  end-to-end email delivery. The existing production pilot has not been changed.

The provider tests simulate Gmail and Zoom responses. Passing them proves local
behavior and request handling; it does not prove the external accounts are configured.
Follow the release gates in `OPERATIONS.md` before activating the real workshop series.

## Read-only production readiness check

Checked September 7 after package creation, without changing the live app:

- App Service Authentication is enabled for the existing AGA Events Entra app and
  tenant. Public requests remain allowed; organizer authorization is enforced by
  the app's protected APIs. This is configuration evidence, not a new interactive
  sign-in/role acceptance test.
- The workshop organizer has one active SQL manager with both event-management
  and registration-management permissions. No permission change is needed for
  that existing full operator.
- The production database does not yet contain the outbox/settings tables or
  attendance column from migration 004.
- The Function App contains the SQL, public-origin and sign-in settings, but no
  workshop Gmail/Zoom integration settings or worker enable flags.
- Milan's email/license details are pending; the user is checking Zoom User
  Management. No live external integration test has been attempted.
