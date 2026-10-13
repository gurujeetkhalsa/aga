# aga-chapter-finder-update

Chapter update request/review workflow, Entra/admin authorization and ClubExpress push-attempt ledger. Approval/push routes have production side effects.

- Repository: [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga). Resource group: `aga-data-platform`.
- Exact captured source: [azure-baseline/aga-chapter-finder-update/](../../azure-baseline/aga-chapter-finder-update/). Development: [chapter-finder-update-app/](../../chapter-finder-update-app/).
- Host: `aga-chapter-finder-update.azurewebsites.net`; state `Running`; runtime `{"name": "python", "version": "3.13"}`.
- Capture: `2026-10-13T17:10:19.376779+00:00`; source status: **recovered active deployment**.
- Active package: `https://agafunc0522.blob.core.windows.net/app-package-agachapterfinderupdate-8306138/released-package.zip`. SHA-256 `6d02a1cb46bc426b7e9e396caceddf67549df6e73cccdbd4c7bb70abd440cae6`.
- Release metadata and individual file hashes: [manifest](../azure-capture/manifests/aga-chapter-finder-update.json). Runtime `function_app.py` byte hash matches captured source.

## Shared and application modules

| Captured module | Maintained development copy |
|---|---|
| [admin_auth.py](../../azure-baseline/aga-chapter-finder-update/admin_auth.py) | [chapter-finder-update-app/admin_auth.py](../../chapter-finder-update-app/admin_auth.py) |
| [chapter_finder_update_support.py](../../azure-baseline/aga-chapter-finder-update/chapter_finder_update_support.py) | [chapter-finder-update-app/chapter_finder_update_support.py](../../chapter-finder-update-app/chapter_finder_update_support.py) |
| [sql_adapter.py](../../azure-baseline/aga-chapter-finder-update/sql_adapter.py) | [chapter-finder-update-app/sql_adapter.py](../../chapter-finder-update-app/sql_adapter.py) |

These are package-local copies, not links to the repository root. Their versions may differ between apps. Never deduplicate merely by filename. Third-party imports are declared in [requirements.txt](../../azure-baseline/aga-chapter-finder-update/requirements.txt); the [static verification report](../azure-capture/verification.json) records the reachable module graph.

## Database

| SQL definition | Relationship | Classification |
|---|---|---|
| [clubexpress.chapter_finder_administrator_assignments](../../sql/azure-baseline/clubexpress/chapter_finder_administrator_assignments.sql) | literal reference in packaged source | application |
| [clubexpress.chapter_finder_entries](../../sql/azure-baseline/clubexpress/chapter_finder_entries.sql) | literal reference in packaged source | application |
| [clubexpress.chapter_finder_update_access_links](../../sql/azure-baseline/clubexpress/chapter_finder_update_access_links.sql) | literal reference in packaged source | application |
| [clubexpress.chapter_finder_update_request_fields](../../sql/azure-baseline/clubexpress/chapter_finder_update_request_fields.sql) | literal reference in packaged source | application |
| [clubexpress.chapter_finder_update_requests](../../sql/azure-baseline/clubexpress/chapter_finder_update_requests.sql) | literal reference in packaged source | application |
| [clubexpress.sp_approve_chapter_finder_update_request](../../sql/azure-baseline/clubexpress/sp_approve_chapter_finder_update_request.sql) | literal reference in packaged source | application |
| [clubexpress.sp_reject_chapter_finder_update_request](../../sql/azure-baseline/clubexpress/sp_reject_chapter_finder_update_request.sql) | literal reference in packaged source | application |
| [clubexpress.v_chapter_finder_update_portal_review](../../sql/azure-baseline/clubexpress/v_chapter_finder_update_portal_review.sql) | literal reference in packaged source | application |
| [clubexpress.v_chapter_finder_update_push_queue](../../sql/azure-baseline/clubexpress/v_chapter_finder_update_push_queue.sql) | catalog dependency or attached trigger | application |
| [membership.members](../../sql/azure-baseline/membership/members.sql) | literal reference in packaged source | application |
| [ratings.admin_permissions](../../sql/azure-baseline/ratings/admin_permissions.sql) | literal reference in packaged source | application |

Every SQL file has a metadata companion. [Database permissions](../../sql/azure-baseline/_database/permissions.sql), [principals](../../sql/azure-baseline/_database/principals.json), [schemas](../../sql/azure-baseline/_database/schemas.sql), [full-text definitions](../../sql/azure-baseline/_database/fulltext.sql) and [all dependency edges](../../sql/azure-baseline/_database/dependencies.json) are part of the contract.

**Dependency analysis limit:** dynamic SQL targets and external database definitions are unknown until each constructed SQL path is checked. Literal matching plus catalog closure is evidence, not proof that all runtime branches execute. Triggers classified as replication support must be evaluated separately from application behavior.

## Configuration and deployment

[Sanitized live settings, EasyAuth, route bindings, schedules and enablement](../azure-capture/config/aga-chapter-finder-update.json). Values marked `<supplied securely>` come from the existing Azure app setting/identity and must be provisioned through the future VM secret store. No secret values are in Git.

[Storage container/queue inventory](../azure-capture/storage-contract.json). App-specific setting names and binding `connection`/`queueName`/`schedule` fields identify its resources. Dynamically constructed blob prefixes remain in the source. Do not infer timer ownership from app names alone.

[Captured host configuration](../../azure-baseline/aga-chapter-finder-update/host.json). [Captured dependency declaration](../../azure-baseline/aga-chapter-finder-update/requirements.txt). [Original packaged operations notes](../../azure-baseline/aga-chapter-finder-update/README.md).

Build a reviewed source ZIP without executing code or contacting Azure:

```powershell
python scripts/package_baseline.py aga-chapter-finder-update --output ../packages/aga-chapter-finder-update-source.zip
```

For a separately authorized future Azure deployment, install the declared dependencies using the recorded Python version or use the Azure remote build. The historical deployment interface is:

```powershell
az functionapp deployment source config-zip --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --resource-group aga-data-platform --name aga-chapter-finder-update --src ../packages/aga-chapter-finder-update-source.zip --build-remote true
```

**Do not execute that deployment command during baseline or reconciliation work.** Source ZIPs omit installed dependencies and production data. Deployment scripts embedded in the package are preserved; existing repository scripts are under [scripts/](../../scripts/). This command records the interface and is not a deployment performed by this task.

## Routes, authentication and schedules

| Function | Trigger contract | Enabled |
|---|---|---|
| `ChapterFinderReviewApprove` | type=httpTrigger; route=chapter-finder/review/approve; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterFinderReviewMe` | type=httpTrigger; route=chapter-finder/review/me; methods=['GET', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterFinderReviewPage` | type=httpTrigger; route=chapter-finder/review; methods=['GET']; authLevel=ANONYMOUS | True |
| `ChapterFinderReviewReject` | type=httpTrigger; route=chapter-finder/review/reject; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterFinderReviewRequest` | type=httpTrigger; route=chapter-finder/review/request; methods=['GET', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterFinderReviewRequests` | type=httpTrigger; route=chapter-finder/review/requests; methods=['GET', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterFinderUpdateContext` | type=httpTrigger; route=chapter-finder/update/context; methods=['GET', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterFinderUpdateMember` | type=httpTrigger; route=chapter-finder/update/member; methods=['GET', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterFinderUpdatePage` | type=httpTrigger; route=chapter-finder/update; methods=['GET']; authLevel=ANONYMOUS | True |
| `ChapterFinderUpdateSubmit` | type=httpTrigger; route=chapter-finder/update/submit; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |

Trigger ANONYMOUS does not bypass handler/EasyAuth authorization. Preserve the checks in the entry point and imported auth modules. Azure settings may disable a trigger independently; consult both settings and binding metadata.

## Differences and verification

10 captured source/assets files. Static syntax, reachable imports, per-file hashes and ZIP CRC checks passed. No handler was invoked against production. [Per-file deployed/development/local comparison](../azure-capture/source-comparison.json) distinguishes the original GitHub development layout and preserved local work.

The normal development folder may lag or diverge from the deployment. Future VM work starts from this appâ€™s captured behavior; do not overwrite newer development work. Use a separate `vm-stack/` implementation when migration begins.
