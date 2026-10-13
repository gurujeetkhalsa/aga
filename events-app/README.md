# AGA Events App — Repeating Event Foundation

This is the first vertical slice of the AGA-owned Events platform described in
[`docs/events-v0-design.md`](../docs/events-v0-design.md). It implements a free
event with a permanent public link and any number of future dated occurrences.

## Production pilot

Deployed August 5, 2026:

- Registration: <https://aga-events.azurewebsites.net/events/learn-go-in-an-hour>
- QR code: <https://aga-events.azurewebsites.net/events/learn-go-in-an-hour/qr.svg>
- Organizer editor: <https://aga-events.azurewebsites.net/events/manage/learn-go-in-an-hour>
- Function App: `aga-events` in resource group `aga-data-platform`
- Database: `aga-membership` on `aga-sql-server`

The first occurrence is Thursday, September 10, 2026, 6:00–7:00 PM
`America/New_York`, online via Zoom, with capacity 10 and waitlisting enabled.

The production schedule was expanded September 4, 2026 to ten workshops,
all 6:00–7:00 PM Eastern on the second and fourth Thursdays:
September 10 and 24; October 8 and 22; November 12; December 10 and 24;
January 14 and 28, 2027; and February 11, 2027.
Thanksgiving (November 26) is excluded. Each workshop has capacity 10,
waitlisting enabled. Registration choices include all workshops at least 24 hours away.
The live public page was verified to return all ten, with four initially
visible in the scrollable list.

## First use case

The stable public address is:

```text
https://aga-events.azurewebsites.net/events/learn-go-in-an-hour
```

Its QR code is always:

```text
https://aga-events.azurewebsites.net/events/learn-go-in-an-hour/qr.svg
```

Adding an occurrence changes the list at the first URL; it does not change the
URL or QR code. A single-occurrence event uses the same schema and simply has
one row in `events.event_occurrences`.

The public page supports:

- one or multiple selectable occurrences, controlled by event policy;
- all published dates at least 24 hours away in a scrollable list, initially sized to the next
  four dates (the existing `public_occurrence_limit` setting controls viewport
  size only; it no longer limits API results);
- confirmed count and maximum capacity for every occurrence;
- automatic waitlisting when capacity is full;
- free registration requiring first name, last name, and email;
- optional city, state/province, country, and ZIP/postal code;
- an idempotency key so a retried submission does not register twice; and
- responsive standalone and ClubExpress-iframe presentation.

## Original API

- `GET /events/{slug}` — permanent public registration page
- `GET /events/{slug}/qr.svg` — QR code for the permanent page
- `GET /events/manage/{slug}` — protected workshop occurrence editor
- `GET /api/events/{slug}` — public event and live occurrence availability
- `POST /api/events/{slug}/registrations` — atomic free registration
- `POST /api/events/manage/{event_id}/occurrences` — append an occurrence
- `GET /api/events/manage/{event_id}/occurrences` — list manageable occurrences
- `PATCH /api/events/manage/{event_id}/occurrences/{occurrence_id}` — edit an occurrence
- `GET /api/events/health` — process health

The organizer endpoint requires an authenticated Entra principal with the
`Events.Organizer` or `Events.PlatformAdmin` application role and an active SQL
membership with `can_manage_events = 1` for the event's organizer.

The production Entra application is `AGA Events`. Public registration remains
anonymous; organizer APIs enforce both its application role and SQL organizer
scope. Occurrence creation and editing are written to `events.audit_log`.

Example organizer payload:

```json
{
  "occurrence_key": "sep-2026",
  "title": "September Self-Pair",
  "starts_at": "2026-09-12T14:00:00-04:00",
  "ends_at": "2026-09-12T18:00:00-04:00",
  "registration_opens_at": "2026-08-15T09:00:00-04:00",
  "capacity": 32,
  "waitlist_enabled": true,
  "location_name": "Online"
}
```

## Database setup

1. Review and apply `sql/001_repeating_event_foundation.sql` to the shared Azure
   SQL database.
2. Apply `sql/002_beginner_online_workshop_seed.sql` to create the first
   production series and its September 10, 2026 occurrence. The seed is
   idempotent and can be rerun safely.
3. Set `SQL_CONNECTION_STRING` on the Function App.
4. Set `EVENTS_PUBLIC_BASE_URL` to the canonical origin. The QR endpoint derives
   the current origin when this setting is absent, but production should set it
   explicitly.

The initial production occurrence is configured for 6:00–7:00 PM
`America/New_York`, capacity 10, free registration, and waitlisting.

## Capacity correctness

Registration locks the stable event row and all selected occurrence rows in
ascending identifier order within one SQL transaction. It recounts confirmed
assignments while holding those locks, then inserts each choice as `confirmed`
or `waitlisted`. This prevents two concurrent requests from claiming the same
last spot, provided every capacity-changing path follows the same lock order.

The event-level registration status can be `mixed` when a multi-select request
receives a confirmed place in one occurrence and a waitlist place in another.

## Local verification

From this directory:

```powershell
py -3 -m unittest discover -s tests -v
py -3 -m py_compile event_domain.py sql_repository.py function_app.py
```

For local Azure Functions execution, create an untracked `local.settings.json`
with `SQL_CONNECTION_STRING`, then run `func start`.

## Workshop operations package

The manager places **Add Workshop** above compact workshop summaries. Each summary
shows the date/time and signup counts, expands to full details when selected, and
offers **Cancel** separately. Cancelled workshops retain their history and status.
Email/Zoom settings and QR downloads are grouped in expandable sections below.

The workshop manager now includes signups, attendance, audited cancellation,
waitlist promotion, email status/reconciliation, sender/reminder/follow-up settings,
and per-date Zoom scheduling/status. See [OPERATIONS.md](OPERATIONS.md) for setup,
permissions, operator instructions, integration settings, testing, and release gates.

Apply migration 004 before deploying this version. Both email and Zoom workers
are disabled by default. The source package does not change the deployed pilot.

The notification sender selected September 9, 2026 is
`milan.mladenovic@usgo.org`. From, Reply-To, and the assistance/cancellation
address use this mailbox. Gmail authorization was verified and saved to the
workshop Azure app September 9. One authorized delivery test arrived in
`gurujeet.khalsa@usgo.org`'s Inbox. Automated delivery remains disabled pending
Zoom setup and release acceptance.

The selected Zoom host signs in as `milan.mladenovic.ja@gmail.com`. On September 9
the user reported his AGA Zoom invitation is still Pending. After acceptance,
verify active status and an assigned license before configuring managed meetings.

## Simplified workshop scheduling � September 11, 2026

Add Workshop defaults to **Free Beginners 1-Hour Workshop**, prefixed with
**Test - ** for rehearsal series. Date and Time (Eastern) are separate; time
defaults to 6 PM and the server supplies a one-hour duration. The Manager hides
end time and registration-open/close fields. Public selection and registration
submission both use a 24-hour lead time (exactly 24 hours is allowed), including
existing dates regardless of their legacy registration-window fields. All eligible
dates remain scrollable. In email test mode, the rehearsal's simulated clock also
applies to this cutoff; advancing to the one-hour reminder hides the date.

Zoom Link is read-only and loaded from the authorized meeting endpoint; it is
never copied into the publicly visible location label. After adding a workshop,
it checks for the automatically prepared meeting and offers Refresh Zoom Link.
Existing titles and meeting links are retained.
