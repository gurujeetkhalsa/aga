# Baseline verification

All 16 active deployment entry points were compared against authenticated live
read-only VFS responses and matched byte for byte. Flex packages were selected by
the successful active deployment ID/UploadedPackageName in Kudu state, with an
ETag-protected download and active-ID recheck. The classic app was recovered from
its current SCM SquashFS archive; no source was synthesized.

Static checks validated every source file against its manifest hash, parsed all
Python files (also using each app's recorded Python language version), followed
reachable local imports, checked declared third-party dependencies and created/read
source-only ZIPs with CRC validation. All passed. This does not certify native
dependency rebuilds or end-to-end behavior on the future VM.

All captured HTML/JS/CSS, WGo scripts/wood image, news card images and workshop QR
artwork are present. Route-looking filenames and runtime blob/CSV/snapshot names
were reviewed separately from static assets. The chart's two private CSV datasets
are intentionally excluded and documented as data migration prerequisites.
Original package `.ostype` markers and installed/runtime output are excluded.

42 existing workshop/domain/QR tests ran with application credentials removed,
network connections blocked and SQL connection creation blocked. No real Gmail,
Zoom, database, queue or mutation route was contacted. QR tests passed, including
decoding the current registration destination. The run used Python 3.14 on Windows;
the deployed workshop runtime is Python 3.11. Target-version syntax checks passed.

Three assertions failed in two **unchanged, captured** email-format test methods:

- `test_preparation_html_escapes_values_and_short_reminder_stays_brief` expects no
  HTML part for a short reminder; the deployed code produces an HTML part.
- `test_preparation_reminder_uses_actual_date_links_and_calendar` expects two copies
  of a formatted date; deployed messages contain one, in both tested months.

These are recorded source/test expectation differences, not recovery failures.
Do not change deployed message behavior merely to make old bundled tests pass.
Resolve expectations with the email owner during migration implementation.
Real SQL integration and browser tests were not run against production.

[Machine-readable static checks](azure-capture/verification.json) and
[offline results](azure-capture/offline-tests.json) accompany this report.
The public staging review excludes credentials, signed URLs, database rows,
mailbox content, receipts, environments and generated data. Gitleaks findings
were reviewed as SHA-256 manifest strings and the existing public reward event
identifier `nyga-circuit-20260501`. GitHub push protection subsequently identified
three hard-coded Azure Function URL keys in the older `original-functions` local
snapshot that Gitleaks missed. The rejected push was not published. Those values
were replaced with secure-supply placeholders before publishing the rewritten,
previously unpushed commit. The snapshot manifest records original and published
hashes plus the redaction. A repository-wide scan for those exact keys and long
credential URL parameters found no remaining occurrences. No deployed source
required redaction, and no production credentials were rotated or changed.

The deployed packages and all original local worktrees remain behaviorally unchanged.
