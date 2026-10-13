# aga-events

Entra/EasyAuth plus application role checks; SQL event/registration ledgers; Gmail messages; Zoom meeting creation; workshop timers and QR downloads. Preserve exactly one message/meeting worker owner and idempotency.

- Repository: [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga). Resource group: `aga-data-platform`.
- Exact captured source: [azure-baseline/aga-events/](../../azure-baseline/aga-events/). Development: [events-app/](../../events-app/).
- Host: `aga-events.azurewebsites.net`; state `Running`; runtime `{"name": "python", "version": "3.11"}`.
- Capture: `2026-10-13T17:13:04.132693+00:00`; source status: **recovered active deployment**.
- Active package: `https://agafunc0522.blob.core.windows.net/app-package-agaevents-7822871/released-package.zip`. SHA-256 `bff80da9fdd316d83e55f2f41cb7b623cc7c14f1929f0b0bc0f27917b45605cb`.
- Release metadata and individual file hashes: [manifest](../azure-capture/manifests/aga-events.json). Runtime `function_app.py` byte hash matches captured source.

## Shared and application modules

| Captured module | Maintained development copy |
|---|---|
| [build_package.py](../../azure-baseline/aga-events/build_package.py) | [events-app/build_package.py](../../events-app/build_package.py) |
| [event_domain.py](../../azure-baseline/aga-events/event_domain.py) | [events-app/event_domain.py](../../events-app/event_domain.py) |
| [sql_repository.py](../../azure-baseline/aga-events/sql_repository.py) | [events-app/sql_repository.py](../../events-app/sql_repository.py) |
| [workshop_domain.py](../../azure-baseline/aga-events/workshop_domain.py) | [events-app/workshop_domain.py](../../events-app/workshop_domain.py) |
| [workshop_messages.py](../../azure-baseline/aga-events/workshop_messages.py) | [events-app/workshop_messages.py](../../events-app/workshop_messages.py) |
| [workshop_providers.py](../../azure-baseline/aga-events/workshop_providers.py) | [events-app/workshop_providers.py](../../events-app/workshop_providers.py) |
| [workshop_qr.py](../../azure-baseline/aga-events/workshop_qr.py) | [events-app/workshop_qr.py](../../events-app/workshop_qr.py) |
| [workshop_repository.py](../../azure-baseline/aga-events/workshop_repository.py) | [events-app/workshop_repository.py](../../events-app/workshop_repository.py) |
| [workshop_worker.py](../../azure-baseline/aga-events/workshop_worker.py) | [events-app/workshop_worker.py](../../events-app/workshop_worker.py) |

These are package-local copies, not links to the repository root. Their versions may differ between apps. Never deduplicate merely by filename. Third-party imports are declared in [requirements.txt](../../azure-baseline/aga-events/requirements.txt); the [static verification report](../azure-capture/verification.json) records the reachable module graph.

## Database

| SQL definition | Relationship | Classification |
|---|---|---|
| [events.audit_log](../../sql/azure-baseline/events/audit_log.sql) | literal reference in packaged source | application |
| [events.email_outbox](../../sql/azure-baseline/events/email_outbox.sql) | literal reference in packaged source | application |
| [events.event_occurrences](../../sql/azure-baseline/events/event_occurrences.sql) | literal reference in packaged source | application |
| [events.event_versions](../../sql/azure-baseline/events/event_versions.sql) | literal reference in packaged source | application |
| [events.events](../../sql/azure-baseline/events/events.sql) | literal reference in packaged source | application |
| [events.organizer_memberships](../../sql/azure-baseline/events/organizer_memberships.sql) | literal reference in packaged source | application |
| [events.organizers](../../sql/azure-baseline/events/organizers.sql) | literal reference in packaged source | application |
| [events.registration_contacts](../../sql/azure-baseline/events/registration_contacts.sql) | literal reference in packaged source | application |
| [events.registration_occurrences](../../sql/azure-baseline/events/registration_occurrences.sql) | literal reference in packaged source | application |
| [events.registrations](../../sql/azure-baseline/events/registrations.sql) | literal reference in packaged source | application |
| [events.rehearsal_clocks](../../sql/azure-baseline/events/rehearsal_clocks.sql) | literal reference in packaged source | application |
| [events.workshop_meetings](../../sql/azure-baseline/events/workshop_meetings.sql) | literal reference in packaged source | application |
| [events.workshop_settings](../../sql/azure-baseline/events/workshop_settings.sql) | literal reference in packaged source | application |
| [membership.chapters](../../sql/azure-baseline/membership/chapters.sql) | literal reference in packaged source | application |

Every SQL file has a metadata companion. [Database permissions](../../sql/azure-baseline/_database/permissions.sql), [principals](../../sql/azure-baseline/_database/principals.json), [schemas](../../sql/azure-baseline/_database/schemas.sql), [full-text definitions](../../sql/azure-baseline/_database/fulltext.sql) and [all dependency edges](../../sql/azure-baseline/_database/dependencies.json) are part of the contract.

**Dependency analysis limit:** dynamic SQL targets and external database definitions are unknown until each constructed SQL path is checked. Literal matching plus catalog closure is evidence, not proof that all runtime branches execute. Triggers classified as replication support must be evaluated separately from application behavior.

## Configuration and deployment

[Sanitized live settings, EasyAuth, route bindings, schedules and enablement](../azure-capture/config/aga-events.json). Values marked `<supplied securely>` come from the existing Azure app setting/identity and must be provisioned through the future VM secret store. No secret values are in Git.

[Storage container/queue inventory](../azure-capture/storage-contract.json). App-specific setting names and binding `connection`/`queueName`/`schedule` fields identify its resources. Dynamically constructed blob prefixes remain in the source. Do not infer timer ownership from app names alone.

[Captured host configuration](../../azure-baseline/aga-events/host.json). [Captured dependency declaration](../../azure-baseline/aga-events/requirements.txt). [Original packaged operations notes](../../azure-baseline/aga-events/README.md).

Build a reviewed source ZIP without executing code or contacting Azure:

```powershell
python scripts/package_baseline.py aga-events --output ../packages/aga-events-source.zip
```

For a separately authorized future Azure deployment, install the declared dependencies using the recorded Python version or use the Azure remote build. The historical deployment interface is:

```powershell
az functionapp deployment source config-zip --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --resource-group aga-data-platform --name aga-events --src ../packages/aga-events-source.zip --build-remote true
```

**Do not execute that deployment command during baseline or reconciliation work.** Source ZIPs omit installed dependencies and production data. Deployment scripts embedded in the package are preserved; existing repository scripts are under [scripts/](../../scripts/). This command records the interface and is not a deployment performed by this task.

## Routes, authentication and schedules

| Function | Trigger contract | Enabled |
|---|---|---|
| `EventsHealth` | type=httpTrigger; route=api/events/health; methods=['GET']; authLevel=ANONYMOUS | True |
| `EventsManageAddOccurrence` | type=httpTrigger; route=api/events/manage/{event_id:int}/occurrences; methods=['POST']; authLevel=ANONYMOUS | True |
| `EventsManageListOccurrences` | type=httpTrigger; route=api/events/manage/{event_id:int}/occurrences; methods=['GET']; authLevel=ANONYMOUS | True |
| `EventsManagePage` | type=httpTrigger; route=events/manage/{slug}; methods=['GET']; authLevel=ANONYMOUS | True |
| `EventsManageUpdateOccurrence` | type=httpTrigger; route=api/events/manage/{event_id:int}/occurrences/{occurrence_id:int}; methods=['PATCH']; authLevel=ANONYMOUS | True |
| `EventsPublicData` | type=httpTrigger; route=api/events/{slug}; methods=['GET']; authLevel=ANONYMOUS | True |
| `EventsPublicPage` | type=httpTrigger; route=events/{slug}; methods=['GET']; authLevel=ANONYMOUS | True |
| `EventsQrCode` | type=httpTrigger; route=events/{slug}/qr.svg; methods=['GET']; authLevel=ANONYMOUS | True |
| `EventsRegisterFree` | type=httpTrigger; route=api/events/{slug}/registrations; methods=['POST']; authLevel=ANONYMOUS | True |
| `WorkshopAdminScript` | type=httpTrigger; route=events-assets/workshop-admin.js; methods=['GET']; authLevel=ANONYMOUS | True |
| `WorkshopCancelDate` | type=httpTrigger; route=api/events/manage/{event_id:int}/occurrences/{occurrence_id:int}; methods=['DELETE']; authLevel=ANONYMOUS | True |
| `WorkshopChapterQrPage` | type=httpTrigger; route=events/{slug}/chapter-qr; methods=['GET']; authLevel=ANONYMOUS | True |
| `WorkshopEmailReconcile` | type=httpTrigger; route=api/events/manage/{event_id:int}/emails/{email_id:int}; methods=['PATCH']; authLevel=ANONYMOUS | True |
| `WorkshopMeeting` | type=httpTrigger; route=api/events/manage/{event_id:int}/occurrences/{occurrence_id:int}/meeting; methods=['GET', 'PUT', 'PATCH']; authLevel=ANONYMOUS | True |
| `WorkshopNotificationsTimer` | type=timerTrigger; schedule=0 * * * * * | True |
| `WorkshopQrCard` | type=httpTrigger; route=events/{slug}/workshop-qr; methods=['GET']; authLevel=ANONYMOUS | True |
| `WorkshopQrDownload` | type=httpTrigger; route=events/{slug}/workshop-qr/{format}; methods=['GET']; authLevel=ANONYMOUS | True |
| `WorkshopQrShareScript` | type=httpTrigger; route=events-assets/workshop-qr-share.js; methods=['GET']; authLevel=ANONYMOUS | True |
| `WorkshopReferralChapters` | type=httpTrigger; route=api/workshop-chapters; methods=['GET']; authLevel=ANONYMOUS | True |
| `WorkshopRegistrationStatistics` | type=httpTrigger; route=api/events/manage/{event_id:int}/statistics; methods=['GET']; authLevel=ANONYMOUS | True |
| `WorkshopRehearsalClock` | type=httpTrigger; route=api/events/manage/{event_id:int}/occurrences/{occurrence_id:int}/rehearsal-clock; methods=['GET', 'POST']; authLevel=ANONYMOUS | True |
| `WorkshopRoster` | type=httpTrigger; route=api/events/manage/{event_id:int}/occurrences/{occurrence_id:int}/roster; methods=['GET', 'POST']; authLevel=ANONYMOUS | True |
| `WorkshopSettings` | type=httpTrigger; route=api/events/manage/{event_id:int}/settings; methods=['GET', 'PUT']; authLevel=ANONYMOUS | True |
| `WorkshopSignup` | type=httpTrigger; route=api/events/manage/{event_id:int}/occurrences/{occurrence_id:int}/roster/{selection_id:int}; methods=['PATCH', 'DELETE']; authLevel=ANONYMOUS | True |

Trigger ANONYMOUS does not bypass handler/EasyAuth authorization. Preserve the checks in the entry point and imported auth modules. Azure settings may disable a trigger independently; consult both settings and binding metadata.

## Differences and verification

43 captured source/assets files. Static syntax, reachable imports, per-file hashes and ZIP CRC checks passed. No handler was invoked against production. [Per-file deployed/development/local comparison](../azure-capture/source-comparison.json) distinguishes the original GitHub development layout and preserved local work.

The normal development folder may lag or diverge from the deployment. Future VM work starts from this appâ€™s captured behavior; do not overwrite newer development work. Use a separate `vm-stack/` implementation when migration begins.

Workshop QR assets and their current target `https://aga-events.azurewebsites.net/events/learn-go-in-an-hour` are retained unchanged. Rehearsal and chapter referral targets are constructed by the captured source. No redirect/routing changes were made.
