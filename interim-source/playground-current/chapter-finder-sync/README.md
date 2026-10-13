<!-- Copyright 2026, American Go Association, All rights reserved -->

# Chapter Finder Sync

This utility captures the current public ClubExpress Chapter Finder map data
from usgo.org and stores it in Azure SQL under the existing `clubexpress`
schema.

Chapter administrator assignments are separate, private ClubExpress data. An
authorized Chrome session can extract them from each chapter's
`Administrators` action. They are stored in private relationship tables and
are not added to the public Chapter Finder rows or views.

The first goal is to maintain an auditable SQL copy of the information that
appears on the Find a Club map. Later, chapter-update forms can prefill from
this data, collect proposed changes, and apply approved updates back into
ClubExpress.

## Source

- Map page: `https://usgo.org/content.aspx?page_id=225&club_id=454497`
- Detail endpoint: `https://usgo.org/basic_modules/organization/cs.aspx?c={id}`

The detail endpoint must be called with the same web session created by loading
the map page. Direct unauthenticated calls return a session-expired fragment.

## Tables

- `clubexpress.chapter_finder_sync_runs`
- `clubexpress.chapter_finder_entries`
- `clubexpress.chapter_finder_entry_versions`
- `clubexpress.chapter_finder_administrator_sync_runs`
- `clubexpress.chapter_finder_administrator_assignments`
- `clubexpress.chapter_finder_administrator_assignment_versions`
- `clubexpress.chapter_finder_update_requests`
- `clubexpress.chapter_finder_update_request_fields`
- `clubexpress.chapter_finder_update_push_attempts`

`chapter_finder_entries` holds the current known state. A new
`chapter_finder_entry_versions` row is written only when a chapter is first seen
or its normalized public data changes.

The update-request tables are the editing workflow. `chapter_finder_entries`
continues to mean "what ClubExpress currently shows publicly." Update requests
hold proposed desired changes. Approval moves a request onto the push queue.
The ClubExpress push worker will read the queue, apply the approved SQL payload
to ClubExpress through an authorized Chrome session, and then a sync run
verifies the public page. See [CLUBEXPRESS_PUSH.md](CLUBEXPRESS_PUSH.md) for the
mapped controls and guarded procedure.

`chapter_finder_administrator_assignments` is a many-to-many private mapping.
One chapter may have zero or more assigned administrators. The import retains
the ClubExpress display name and only fills `Membership_AGAID` when the current
membership database gives exactly one conservative name match. Ambiguous and
unmatched names remain explicit review states.

## Run

Set `SQL_CONNECTION_STRING`, or pass `--connection-string`.

```powershell
py -3 aga\chapter-finder-sync\sync_chapter_finder.py --apply-schema
```

Useful options:

```powershell
py -3 aga\chapter-finder-sync\sync_chapter_finder.py --dry-run
py -3 aga\chapter-finder-sync\sync_chapter_finder.py --limit 5
py -3 aga\chapter-finder-sync\sync_chapter_finder.py --no-mark-missing-inactive
```

## Private Chapter Administrators

Apply the private assignment schema:

```powershell
py -3 aga\chapter-finder-sync\sync_chapter_administrators.py apply-schema
```

After extracting assignments through an authorized Chrome admin session,
validate the JSON without changing SQL:

```powershell
py -3 aga\chapter-finder-sync\sync_chapter_administrators.py import `
  --input .\chapter-administrator-assignments.json `
  --dry-run
```

Remove `--dry-run` to synchronize current assignments and append history. The
extraction file contains private names and should be treated as ephemeral. It
does not contain credentials, cookies, MFA codes, or browser session data.

Show aggregate status without displaying private names:

```powershell
py -3 aga\chapter-finder-sync\sync_chapter_administrators.py status
```

## SQL-first Editing Workflow

Apply the update workflow schema:

```powershell
py -3 aga\chapter-finder-sync\chapter_finder_update_workflow.py apply-schema
```

Create an update request from a JSON object containing one or more editable
fields:

```powershell
py -3 aga\chapter-finder-sync\chapter_finder_update_workflow.py propose `
  --clubexpress-chapter-id 5518 `
  --proposed-json .\austin-update.json `
  --submitted-by-name "Chapter Rep" `
  --submitted-by-email "rep@example.org"
```

Editable field keys are:

- `chapter_name`
- `address_text`
- `postal_text`
- `city`
- `state_province`
- `postal_code`
- `contact_name`
- `contact_email`
- `contact_phone`
- `website_url`
- `website_label`
- `description_text`

Approve a request and place it on the ClubExpress push queue:

```powershell
py -3 aga\chapter-finder-sync\chapter_finder_update_workflow.py approve 123 --approved-by "AGA Editor"
```

Inspect the queue:

```powershell
py -3 aga\chapter-finder-sync\chapter_finder_update_workflow.py queue
```

Preview an approved request's exact ClubExpress controls without claiming it:

```powershell
py -3 aga\chapter-finder-sync\chapter_finder_update_workflow.py plan-push 123
```

Claim it immediately before the authorized browser edit:

```powershell
py -3 aga\chapter-finder-sync\chapter_finder_update_workflow.py start-push 123 --operator "AGA Editor"
```

The final ClubExpress Save remains browser-assisted and requires action-time
confirmation. No ClubExpress credentials or browser sessions are stored.
