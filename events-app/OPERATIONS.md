# Free 1-hour workshop package

September 10, 2026: AGA Zoom integration is active for the existing series, with
Milan (`milan.mladenovic.ja@gmail.com`) verified as an active licensed AGA host.
Eight published dates have distinct ready meetings, all verified at 6 PM Eastern.
The two cancelled dates have no new Zoom meetings. Email delivery remains off.
See `../docs/session-notes/2026-09-10-workshop-aga-zoom.md` for verification details.
The Zoom adapter sends UTC with a literal `Z` suffix; numeric `+00:00` offsets
were interpreted by Zoom as local time and must not be used in its start_time.

Later September 10: email test mode was enabled for the separate rehearsal
series only, routing to `gurujeet.khalsa@usgo.org`. Real-series email remains
paused. See the rehearsal-delivery section below and
`../docs/session-notes/2026-09-10-workshop-test-mode.md` for the current state.

This extends the existing AGA Events registration pilot. The public URL and
workshop schedule stay in the existing database. The package adds roster and
attendance management, cancellations, queued email delivery, and Zoom scheduling.

## Operator walkthrough

1. Open `/events/manage/learn-go-in-an-hour` and sign in with Microsoft.
2. Under **Email & Zoom settings**, set the instructor and licensed AGA Zoom
   host email. Sender defaults to `milan.mladenovic@usgo.org` and is editable.
3. Use **Add Workshop** above the workshop list to open the new-date form. Dates
   use the displayed event time zone; the permanent occurrence key is generated
   automatically. Each workshop appears as a compact summary with time and signup
   counts. Click its summary (or press Enter/Space when focused) to expand the full
   details. Upcoming dates expose the editable fields and **Save changes** directly.
4. Open **Zoom meeting** to schedule or inspect a date's meeting. Each date gets
   a distinct scheduled meeting. A manual participant joining link is also supported.
5. Open **Signups & attendance** to add or cancel a signup and inspect email
   statuses. Confirmed participants occupy capacity; extra signups are waitlisted.
   Each participant also has an **OGS handle** field. When they reply with their
   handle, enter it and choose **Save OGS handle**. Existing handles are displayed
   there; saving an empty field clears it. Handles are private organizer data.
6. Cancelling a confirmed signup promotes the earliest eligible waitlisted signup
   while the workshop is still in the future. The promoted person gets a confirmation.
7. **Cancel** beside each active workshop's summary asks for confirmation, then
   removes the date from public registration and cancels its
   active signups, queues notices, and queues deletion of its managed Zoom meeting.
   Records remain available for review; this is an audited cancellation rather than
   permanent destruction of contact and attendance records. The row remains marked
   **Cancelled** and can be expanded to inspect its details. Cancellation notices
   wait until email delivery is enabled. Cancelling from the summary does not open
   the detail panel or erase an in-progress Add Workshop form.
8. After a workshop starts, mark each confirmed signup attended or absent. Past
   dates remain visible. Waitlisted/cancelled signups cannot be marked attended.
   Attendance saves automatically. Unrecorded attendance holds the follow-up;
   attended uses your edited message and absent invites them to register again.
9. In **Email & Zoom settings**, edit **Attended follow-up email** and open
   **Preview attended email** to review it, then **Save settings**. Enter the body
   only; the participant's first-name greeting and instructor signature are added.
   Changes apply to unsent follow-ups across the series, including already queued
   messages. Saving text does not enable email delivery.

## Downloadable registration QR code

The manager's **Registration QR code** panel offers PNG and SVG downloads. Both
include the wording curved around the code:

**Free Beginners Online Workshop** (above) and **Learn Go in an hour - Register here.** (below).

The permanent target is `https://aga-events.azurewebsites.net/events/learn-go-in-an-hour`.
It does not depend on the selected Zoom account or a particular workshop date.
Ready-to-use files are included in `downloads/free-beginners-workshop-qr.png` and
`downloads/free-beginners-workshop-qr.svg`. The PNG is 1200 × 1200 pixels with
300 dpi metadata; SVG remains sharp when scaled for printing. Keep the white space
around the code intact.

After deployment, the download endpoints are
`/events/learn-go-in-an-hour/workshop-qr/png` and
`/events/learn-go-in-an-hour/workshop-qr/svg`. They generate the badge using the
configured public origin. The original bare `/events/{slug}/qr.svg` endpoint remains
available. To regenerate the bundled artwork, run `python workshop_qr.py`.
Optional scan tests require `zxing-cpp`; `node tests/browser_qr.cjs` renders the SVG
for visual and independent scan verification.

## Email behavior

### Rehearsal delivery

`EVENTS_EMAIL_MODE` is `off` (default), `test`, or `live`. An invalid value blocks
delivery. In test mode, `EVENTS_EMAIL_TEST_RECIPIENT` must be one valid mailbox.
The Gmail adapter rewrites To, removes Cc/Bcc and Resent headers, prefixes the
subject with `[TEST]`, and adds the intended participant's full name and email
to both plain-text and HTML bodies. Calendar attachments and personalized body
content are preserved. Test Message-IDs differ from live Message-IDs.

Migration `006_workshop_test_mode.sql` adds a server-managed `is_test` marker to
series settings. Only rehearsal series (`is_test=1`) are processed in test mode;
only real series (`is_test=0`) are processed in live mode. Organizer settings
cannot change this classification. Real queues are untouched by rehearsal sends,
and rehearsal queues cannot later be delivered to sample participants in live mode.
Both the global `EVENTS_EMAIL_ENABLED` switch and the series email checkbox still
apply. The manager displays the current mode, recipient, and series classification.

The rehearsal manager is `/events/manage/learn-go-in-an-hour-rehearsal`. Add test
dates and sample registrations there, using sample names and `example.com` email
addresses. AGA Zoom remains a real integration: adding a rehearsal date will
create an actual AGA Zoom meeting. Test emails go to `gurujeet.khalsa@usgo.org`.
Rehearsal settings initially copy the current real-series settings; later edits
to either series are independent. Switching to live delivery is a separate launch
step after email review, attendance tests, and Zoom host/join verification.

### Manually advancing rehearsal time

New rehearsal dates must start more than 24 hours in the future and get a clock
paused at Registration. Expand the date and open **Rehearsal clock**. Complete
sample signups through the TEST QR code, then advance explicitly through:

1. **24-hour reminder** — email time becomes exactly 24 hours before the date.
2. **1-hour reminder** — email time becomes exactly one hour before the date.
3. **Attendance** — time becomes the workshop end. Record attendance under
   **Signups & attendance**, and edit the attended follow-up if desired. Follow-up
   messages remain blocked here, including if their configured delay is zero.
4. **Follow-up** — time becomes the end plus the configured follow-up delay.
   Attended/absent messages can now send; unrecorded remains held. Recording it
   later releases that participant's pending follow-up without resending others.

The clock never advances automatically. **Refresh rehearsal clock** only reads
status. The next step waits for due messages to finish or be reconciled, and
repeated requests cannot skip steps. Delivery uses the minute worker. Real retry
delays and send/claim timestamps use actual time; eligibility uses the saved
rehearsal time. This prevents retries from getting stuck when the clock is paused.
Clock controls require organizer access, a rehearsal series, and test email mode.
Date/reminder timing edits are blocked after advancing; wording edits remain
available. Use another rehearsal date for a fresh run. Existing rehearsal dates
can use **Start rehearsal clock** before testing.

- Individual confirmation emails use Milan's friendly "Hi [first name]," welcome,
  confirm the scheduled date/time, explain the guided Zoom/OGS workshop format,
  and link to OGS registration with a request to reply with the participant's handle.
- Reminders scheduled 24 hours or more ahead use Milan's preparation email from
  `email24.txt`, sent individually with "Hi [first name]," using each registrant's
  first name, and the actual workshop date/time (including EDT/EST) in both
  date references. Online Go Server (OGS) links to `https://online-go.com/register`,
  and the Zoom link uses that workshop's participant joining URL. HTML and plain
  text versions are included, along with the calendar attachment. The 1-hour
  reminder remains brief. Replies go to the configured sender mailbox.
- Confirmation with the workshop's time zone, instructor, private joining link,
  joining instructions, and calendar attachment.
- Separate waitlist acknowledgment without a joining link.
- Signup and whole-workshop cancellation notices.
- Default reminders 24 hours and 1 hour before the workshop. Change the hour list
  in settings. Late registrants do not receive reminders whose scheduled time has
  already passed. Pending reminders are suppressed after cancellation or the start.
- Default follow-up 24 hours after the end, to confirmed registrations according
  to recorded attendance. Attended receives the editable message body. Absent
  receives a separate invitation linking to this series' permanent registration
  page. Neither receives the expired Zoom link or a calendar attachment.
  Unrecorded stays pending with “Waiting for attendance to be recorded.” and is
  checked again in five minutes without consuming a delivery attempt. Other
  recipients can proceed. The worker reads attendance and the latest saved text
  immediately before sending; changing attendance after a sent follow-up does
  not queue another message. Existing delivery controls still apply.
- Reply to the configured sender to request cancellation or assistance. The package
  does not yet offer participant self-service cancellation links.
- Saving settings backfills notifications for existing upcoming pilot signups that
  have no notification history. Saving again does not resend their confirmations.
- Notifications are saved in the same transaction as a signup. The minute timer
  handles delivery. Confirmation/reminder mail waits for a ready Zoom joining link.
- A deterministic Message-ID identifies each queued email. Definite transient
  failures use bounded retries; ambiguous failures become **uncertain** and require
  checking Gmail Sent. In the roster, record the verification and mark sent or retry.
  Exactly-once delivery cannot be guaranteed by a non-idempotent mail provider;
  this workflow avoids blind retries after an uncertain outcome.

## Required server setup

Use the deployed Python 3.11 runtime on Azure Functions, the requirements file, and ODBC Driver 18 for
SQL Server. Existing production Function App: `aga-events`, resource group
`aga-data-platform`. Public origin: `https://aga-events.azurewebsites.net`.

For an existing pilot database, apply **only** `sql/004_workshop_operations.sql`
after confirming migrations 001–003 are present. Apply 001 then 003 then 004 to
a fresh database; 002 is the optional original pilot seed, not the full live
ten-date schedule. Migration 004 is transactional and idempotent. Back up the
database before release using the normal AGA deployment process.

For the OGS-handle manager feature, then apply `sql/005_registrant_ogs_handle.sql`.
This additive, idempotent migration adds one nullable contact field; existing
registrants start with no handle. It is required before deploying the new roster
reader. The migration does not create or change OGS accounts.

Configure these Azure application settings using the secret store, not source files:

| Setting | Purpose |
| --- | --- |
| `SQL_CONNECTION_STRING` | Existing AGA Events SQL connection |
| `EVENTS_PUBLIC_BASE_URL` | Canonical HTTPS origin, without a trailing slash |
| `EVENTS_EMAIL_ENABLED` | Exact value `true` activates the mail worker; absent/false disables it |
| `EVENTS_ALLOWED_SENDERS` | Comma-separated approved Gmail sender addresses; initially `milan.mladenovic@usgo.org` |
| `GOOGLE_WORKSPACE_CLIENT_ID` | Authorized Gmail OAuth client |
| `GOOGLE_WORKSPACE_CLIENT_SECRET` | Gmail OAuth secret |
| `GOOGLE_WORKSPACE_REFRESH_TOKEN` | Refresh token for the authorized sending mailbox |
| `EVENTS_ZOOM_ENABLED` | Exact value `true` activates automatic Zoom scheduling/sync |
| `EVENTS_ZOOM_ACCOUNT_ID` | AGA Zoom account ID |
| `EVENTS_ZOOM_CLIENT_ID` | AGA internal Server-to-Server OAuth app client ID |
| `EVENTS_ZOOM_CLIENT_SECRET` | AGA internal Zoom app secret |

Mail additionally requires the series-level **Enable email delivery** checkbox.
Changing sender requires authorizing the new mailbox/verified send-as address in
Gmail and adding it to the server allowlist. A UI setting alone cannot authorize a
new Gmail identity. Use an OAuth grant with Gmail send permission.

Create/activate an internal Server-to-Server OAuth app in the AGA Zoom account.
Grant the minimum user-read and meeting create/read/update/delete scopes required
by the used endpoints. The adapter verifies that the host is active, licensed
(`type=2`), and belongs to the configured AGA account. It uses scheduled meetings,
disables personal meeting IDs and automatic recording, and enables a waiting room.
Only participant `join_url` is stored; host `start_url` is discarded.

Zoom failures are visible per date. Retry a definite failure with **Schedule with
AGA Zoom** / **Sync Zoom meeting**. An uncertain operation must first be reconciled
against Zoom: enter the existing meeting ID or verify that no meeting was created.
Adoption checks the host, scheduled meeting type, and workshop identifier in its
agenda. Uncertain operations block editing/cancelling that date until reconciled.

Provider references:
[Zoom internal app authentication](https://developers.zoom.us/docs/internal-apps/s2s-oauth/),
[Zoom Meetings API](https://developers.zoom.us/docs/api/meetings/),
[Gmail message sending](https://developers.google.com/workspace/gmail/api/guides/sending).

## Authentication and permissions

The deployed legacy Easy Auth configuration requests `response_type=code id_token`
with `response_mode=form_post`. The **AGA Events** Entra app registration must have
`web.implicitGrantSettings.enableIdTokenIssuance=true` for that hybrid sign-in flow.
Leave `enableAccessTokenIssuance=false`. The client ID is
`cf20a3f1-6664-4bc6-9c4f-0a17473f96d3` and the registered Web callback is
`https://aga-events.azurewebsites.net/.auth/login/aad/callback`.
An `AADSTS700054` error indicates that the required ID-token issuance is disabled.
This setting was corrected September 9, 2026; applying `auth/authsettings.json`
alone does not configure the corresponding Entra app registration.

Use the tenant-specific Microsoft identity **v2.0** issuer from the checked-in
`auth/authsettings.json`, with `additionalLoginParams=["scope=openid profile email"]`.
The former `sts.windows.net` issuer led the legacy endpoint to request Azure AD
Graph and caused `AADSTS650056`. The v2 identity endpoint fixes that dependency
without adding Graph API data permissions. The App Service configuration schema
remains v1; this is an identity endpoint change, not an authsettingsV2 migration.
September 9 acceptance: `gurujeet@live.com` completed personal-account consent to
AGA Events, and the live manager loaded all ten workshops and date editing controls.
Organization-wide consent was not selected. Existing SQL organizer scope remains required.

Keep App Service Authentication (Easy Auth) enabled with the existing AGA Entra
tenant and application, allowing anonymous public registration. The application
checks the authenticated `Events.Organizer` or `Events.PlatformAdmin` role AND an
active SQL membership for the event's active organizer. PlatformAdmin does not
bypass SQL scope. A full workshop operator needs both `can_manage_events=1` and
`can_manage_registrations=1` in `events.organizer_memberships`.

Grant the Entra role to approved staff and provision their Entra object ID in the
appropriate organizer's SQL membership. Deactivating that row revokes access.
Retain tenant/audience restrictions in Easy Auth. Do not expose a direct backend
that bypasses Easy Auth: authenticated principal headers are trusted only because
[App Service strips externally supplied identity headers](https://learn.microsoft.com/en-us/azure/app-service/configure-authentication-user-identities).

Admin mutations require the custom admin header and reject foreign Origins.
Do not enable cross-origin admin CORS. Public contact lists and joining links are
never part of the public API. Organizer APIs return no-store responses.
Use the organization's Entra MFA/conditional-access policies. The development
principal override is restricted to `AZURE_FUNCTIONS_ENVIRONMENT=Development`;
never set that on the hosted production app.

## Verification and release gates

From `events-app` with requirements installed:

```powershell
python -m unittest discover -s tests -v
node tests/browser_admin.cjs
python -m py_compile function_app.py sql_repository.py workshop_repository.py workshop_worker.py
python build_package.py
```

The browser test requires Playwright and an installed Chrome. It intercepts all
requests and checks desktop/mobile settings, signup, cancellation, attendance,
and Zoom controls without external account mutations.

Database tests require explicitly setting `EVENTS_TEST_SQL_CONNECTION_STRING`.
They create a uniquely named `workshop_test_*` schema, redirect all application SQL
to it, and remove only that schema afterward. They do not alter production events
data. The test account needs schema/table create/drop permissions. Provider calls
are fakes; no email is sent and no Zoom meeting is created by the tests.

Before enabling live delivery, verify in the deployment environment:

1. Unauthorized visitors cannot load rosters or mutate dates/signups/settings;
   approved staff can. A staff member outside the organizer scope is denied.
2. Configure Milan's licensed AGA Zoom email and the authorized integration secrets.
   Create one test workshop and verify its distinct meeting in the AGA Zoom account.
3. Register controlled test addresses and verify actual Gmail confirmation/calendar,
   reminders, cancellation, waitlist promotion, and follow-up delivery.
4. Edit and cancel the test date; verify Zoom changes/deletion and cancellation mail.
5. Check the real manager in desktop/mobile browsers and verify attendance persists.
6. Inspect email/Zoom statuses, then enable the actual series. Remove test signups
   through the admin cancellation workflow.

Live Gmail/Zoom acceptance depends on account configuration and is separate from
the automated tests. The production app and production schema are not changed by
building this package.

## Recovery and deployment

Deploy the generated archive using the existing Azure Functions remote Python
build process so Azure installs Linux dependencies. Do not deploy `.test-deps`,
Windows wheels, local settings, test output, or other apps in this workspace.
Apply migration 004 before deploying this version; registration now writes outbox
rows and requires that schema. Keep both external worker flags disabled until
configuration and controlled acceptance tests are ready.

To stop external activity, disable both worker flags. Do not drop outbox tables
to roll back code. Claims abandoned for 15 minutes are marked uncertain for manual
reconciliation; the worker never automatically repeats such mutations.
