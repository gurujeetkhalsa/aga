# Reconcile later Azure changes into the VM stack

Start with [the service/source map](azure-source-map.md). `PREMIGRATION` is an
immutable source/schema reference, **not a database backup or a VM deployment**.
Read its capture status and unresolved items before assuming completeness.
Never retag it. Follow-up captures are new commits with their own timestamps.

## Capture without changing Azure

Required access: the AGA tenant `5f1a8692-caac-4040-9a1b-858e98f03e4b`, subscription
`4dcce4a0-c902-4012-91fe-6dbbddfd6fb7`, Microsoft.Web read/list configuration and
host-key permissions, storage read plus account-key-list permission, and SQL
connect/VIEW DEFINITION. No SQL DML/DDL or Azure write operation is used.
The personal Azure subscription is not the target for this AGA task.

Install Python 3.11+, Azure CLI, Microsoft ODBC Driver 18 for SQL Server, and
`python -m pip install requests azure-storage-blob dissect.squashfs pyodbc` in an isolated environment.
Use `az login --tenant 5f1a8692-caac-4040-9a1b-858e98f03e4b` if not signed in.
The helper explicitly supplies the subscription and checks the tenant; it never
changes the CLI default. It reads existing app SQL credentials in memory and
connects with them. A firewall or permission failure requires administrator access;
do not modify firewall rules, secrets or application settings to make capture work.

From the repository on Windows PowerShell:

```powershell
python scripts/capture_azure.py --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --output C:\AGA-captures\capture-20261020
```

On Linux:

```sh
python scripts/capture_azure.py --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --output /srv/aga-captures/capture-20261020
```

Choose a NEW private directory outside the checkout. Output must not already
exist. The command leaves baseline/development files unchanged. It uses only
management reads (including Azure's POST-shaped list operations), blob downloads,
authenticated GETs of source files, and SELECTs against SQL system catalogs.
It does not invoke application functions, peek/consume queues, or read database rows.
Stop on failed capture, changed active deployment, or runtime hash conflict.

The output contains `inventory.json`, per-app `manifests/`, `config/`,
`azure-baseline/<app>/` and `sql/azure-baseline/`. A successful process exit alone
does not mean every app was recovered: inspect each manifest and SQL export status.
Review excluded-file lists and supply an explicit safe asset allowlist if needed.
Review secret candidates and all added files for production data before Git staging.
The exporter defaults to credential placeholders; privately review placeholders
marked as behavior settings and record approved nonsecret values before publishing.
Never commit raw app settings, storage keys, signed URLs, connection strings,
database rows, mail, receipts, runtime snapshots or downloaded dependency trees.

Flex capture resolves `ActiveDeploymentId` in `kudu-state.json`, requires a successful
completed deployment, downloads its `UploadedPackageName`, verifies the blob ETag,
rechecks the active ID, and compares the Python entry point to live read-only VFS.
It does not choose the newest ZIP. Classic Consumption checks the configured package
URL or `scm-releases/scm-latest-<app>.zip`; require live VFS confirmation and investigate
any mismatch. An unavailable interface is not permission to synthesize replacement code.
Capture deployment slots separately if slots are introduced; do not silently promote
a slot's source to the production baseline.

SQL is exported directly from live catalog metadata, never from a mirror. Each
object has readable `.sql` and a `.metadata.json` companion; `_database/` records
types, indexes, constraints, permissions, dependencies and other catalog details.
Use both files for specialized SQL Server features. Exported SQL is evidence, not
a turnkey SQL Server restore script. The index distinguishes application objects
from SQL tooling/replication. Missing definitions and encryption are blockers.
The paused historical restore database is not the live application contract; do
not resume it as part of capture. If apps start using another database, capture it
into its own directory, update the map and resolve the helper's database separation
guard before continuing. Do not silently combine same-schema objects across databases.

## Three-way reconciliation

1. Fetch GitHub, inspect existing work and save it without discarding changes.
   Resolve `PREMIGRATION^{commit}` and consult the baseline manifest. Never move it.
   Use a separate branch/worktree for reconciliation.
2. Compare **A**: app and SQL definitions at PREMIGRATION, **B**: the new Azure capture,
   and **C**: the current implementation adapted for the single VM/PostgreSQL.
   Compare hashes/file contents, not ZIP timestamps alone. Maintain a change ledger
   for added/removed apps, routes, assets, shared modules, settings and database objects.
3. Classify Aâ†’B changes as intended behavior, bug fixes, operational settings,
   schema changes, or retirement proposals. Check `interim-source/` and Azure developer
   branches separately: unshipped work is not an Azure production change. Ask about
   genuine conflicting business rules; resolve routine layout/import conflicts directly.
4. Reapply each intended change into C's architecture. **Do not replace migrated
   code with an entire Azure directory.** Retain Azure evidence in a dated capture
   commit and link each adapted change to its source and tests.
5. Translate SQL changes into explicit ordered PostgreSQL migrations. Order roles,
   schemas/types/extensions, tables/columns, backfills/data transformations,
   constraints/indexes, functions/views, triggers, grants, then dependent application
   changes. Use catalog dependency edges and inspect dynamic SQL manually. Plan
   conversions for identity/sequence values, computed columns, defaults, collation,
   datetime/timezone, decimals, binary values, NULL rules, SQL Server procedures and
   transaction/isolation semantics. Separate dbrepl_meta/SQL tooling from application
   behavior. Keep the matching Azure definitions alongside migration notes.
6. Verify in an isolated disposable database and environment with outbound network
   integrations blocked and timers/queue workers disabled. Validate route/auth
   behavior, assets, imports, SQL result semantics and migration rollback/recovery.
   Do not apply PostgreSQL migrations to production during source reconciliation.

## Authentication and side-effect ownership

Preserve each route's platform and application authorization, including EasyAuth,
Entra roles, function keys, ClubExpress SSO/token validation, CSRF/session behavior,
admin scopes and public routes. An Azure ANONYMOUS trigger does not establish that
the application is unauthenticated. Inspect its handler and auth helpers.
Keep workshop QR URLs/redirect destinations and their existing artwork unchanged
unless a separate routing decision explicitly authorizes a change.

Map every timer and queue trigger to exactly one owner during cutover. Record cron
timezone, enablement flags, overlap/misfire policy, retries, poison/dead-letter
handling, leases and deduplication keys. Azure and VM must never both send the same
mail, create Zoom meetings, post rewards, push ClubExpress updates, import ratings,
poll the same mailbox or process the same queue item. Reconcile idempotency and
external operation ledgers before enabling the VM. Keep secrets in the deployment
secret store, not Git. Reauthorize integrations only during an approved cutover.

## Production data is a separate migration stream

A source diff does not capture member changes, registrations, rewards, game records,
mailbox checkpoints, queue contents, receipts or blob changes. Before migration,
design and rehearse an initial consistent SQL data export/import and blob transfer
using encrypted private storage. Inventory every live table, including
`integration.news_archive_article_member_match`, rather than assuming an old mirror
is complete. Preserve IDs, FKs, timestamps, binary assets and operation ledgers.

Define a high-water mark or a supported change-capture mechanism for each mutable
table/blob and test deletion handling. At final cutover, explicitly schedule a
write freeze or transactionally consistent delta window; stop Azure side-effect
owners only under separate authorization. Drain or transfer queues with ownership
and deduplication records, apply final inserts/updates/deletes and blob deltas, then
verify row counts, checksums, referential integrity, balances, business totals and
external-operation checkpoints. Reseed PostgreSQL sequences above imported IDs.
Record acceptance criteria, freeze duration, rollback decision point and the handling
of VM writes if reverting. Only after acceptance may routing and worker ownership
switch. This source-capture task authorizes none of those production actions.

## Completion evidence

Commit reviewed source/config/schema changes, a reconciliation ledger, ordered
migrations and isolated test results. Link unresolved dependencies explicitly.
Record capture timestamps, release/package hashes and Git revisions. Preserve
PREMIGRATION and all original branches. Publish a PR if protection blocks merging;
never report pending recovery or pending merges as a complete baseline.
