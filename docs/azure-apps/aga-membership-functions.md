# aga-membership-functions

Member/chapter imports, lookup and TD-list routes; legacy mailbox/MySQL paths coexist. Settings/enablement determine which timers own work. aga-lookup-app and tdlists-app are reference folders, not extra deployments. External MySQL definitions require separate assessment.

- Repository: [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga). Resource group: `aga-data-platform`.
- Exact captured source: [azure-baseline/aga-membership-functions/](../../azure-baseline/aga-membership-functions/). Development: [membership-data-app/](../../membership-data-app/).
- Host: `aga-membership-functions-fmgchkbxa3hxd8h0.westus-01.azurewebsites.net`; state `Running`; runtime `{"name": "python", "version": "3.13"}`.
- Capture: `2026-10-13T17:13:20.138725+00:00`; source status: **recovered active deployment**.
- Active package: `https://agafunc0522.blob.core.windows.net/app-package-aga-membership-functions-6cf36d2/released-package.zip`. SHA-256 `aceeeb18f5bc92ed9a63e392132c020e6c15652f7a266486abefb58508138119`.
- Release metadata and individual file hashes: [manifest](../azure-capture/manifests/aga-membership-functions.json). Runtime `function_app.py` byte hash matches captured source.

## Shared and application modules

| Captured module | Maintained development copy |
|---|---|
| Entry point is self-contained application code | See `function_app.py` |

These are package-local copies, not links to the repository root. Their versions may differ between apps. Never deduplicate merely by filename. Third-party imports are declared in [requirements.txt](../../azure-baseline/aga-membership-functions/requirements.txt); the [static verification report](../azure-capture/verification.json) records the reachable module graph.

## Database

| SQL definition | Relationship | Classification |
|---|---|---|
| [api.sp_lookup_members](../../sql/azure-baseline/api/sp_lookup_members.sql) | literal reference in packaged source | application |
| [api.v_current_ratings](../../sql/azure-baseline/api/v_current_ratings.sql) | catalog dependency or attached trigger | application |
| [api.v_member_lookup](../../sql/azure-baseline/api/v_member_lookup.sql) | catalog dependency or attached trigger | application |
| [dbrepl_meta.generations](../../sql/azure-baseline/dbrepl_meta/generations.sql) | catalog dependency or attached trigger | tooling/replication |
| [integration.clubexpress_email_log](../../sql/azure-baseline/integration/clubexpress_email_log.sql) | catalog dependency or attached trigger | application |
| [integration.journal_article_member_match](../../sql/azure-baseline/integration/journal_article_member_match.sql) | catalog dependency or attached trigger | application |
| [integration.journal_review_member_match](../../sql/azure-baseline/integration/journal_review_member_match.sql) | catalog dependency or attached trigger | application |
| [membership.categories](../../sql/azure-baseline/membership/categories.sql) | catalog dependency or attached trigger | application |
| [membership.chapter_membership_history](../../sql/azure-baseline/membership/chapter_membership_history.sql) | catalog dependency or attached trigger | application |
| [membership.chapters](../../sql/azure-baseline/membership/chapters.sql) | literal reference in packaged source | application |
| [membership.member_categories](../../sql/azure-baseline/membership/member_categories.sql) | catalog dependency or attached trigger | application |
| [membership.members](../../sql/azure-baseline/membership/members.sql) | literal reference in packaged source | application |
| [membership.membership_events](../../sql/azure-baseline/membership/membership_events.sql) | catalog dependency or attached trigger | application |
| [membership.sp_import_chapters](../../sql/azure-baseline/membership/sp_import_chapters.sql) | literal reference in packaged source | application |
| [membership.sp_import_member_categories](../../sql/azure-baseline/membership/sp_import_member_categories.sql) | literal reference in packaged source | application |
| [membership.sp_import_memchap](../../sql/azure-baseline/membership/sp_import_memchap.sql) | literal reference in packaged source | application |
| [membership.sp_log_clubexpress_email](../../sql/azure-baseline/membership/sp_log_clubexpress_email.sql) | literal reference in packaged source | application |
| [membership.sp_process_journal_news_email](../../sql/azure-baseline/membership/sp_process_journal_news_email.sql) | literal reference in packaged source | application |
| [membership.sp_process_membership_renewal](../../sql/azure-baseline/membership/sp_process_membership_renewal.sql) | literal reference in packaged source | application |
| [membership.sp_process_new_member_email](../../sql/azure-baseline/membership/sp_process_new_member_email.sql) | literal reference in packaged source | application |
| [ratings.dbrepl_dirty_1250103494](../../sql/azure-baseline/ratings/dbrepl_dirty_1250103494.sql) | catalog dependency or attached trigger | tooling/replication |
| [ratings.ratings](../../sql/azure-baseline/ratings/ratings.sql) | literal reference in packaged source | application |
| [rewards.membership_events](../../sql/azure-baseline/rewards/membership_events.sql) | catalog dependency or attached trigger | application |
| [rewards.sp_record_membership_event](../../sql/azure-baseline/rewards/sp_record_membership_event.sql) | literal reference in packaged source | application |
| [staging.chapters](../../sql/azure-baseline/staging/chapters.sql) | literal reference in packaged source | application |
| [staging.dbrepl_dirty_1202103323](../../sql/azure-baseline/staging/dbrepl_dirty_1202103323.sql) | catalog dependency or attached trigger | tooling/replication |
| [staging.dbrepl_dirty_18099105](../../sql/azure-baseline/staging/dbrepl_dirty_18099105.sql) | catalog dependency or attached trigger | tooling/replication |
| [staging.dbrepl_dirty_887674210](../../sql/azure-baseline/staging/dbrepl_dirty_887674210.sql) | catalog dependency or attached trigger | tooling/replication |
| [staging.member_categories](../../sql/azure-baseline/staging/member_categories.sql) | literal reference in packaged source | application |
| [staging.memchap](../../sql/azure-baseline/staging/memchap.sql) | literal reference in packaged source | application |

Every SQL file has a metadata companion. [Database permissions](../../sql/azure-baseline/_database/permissions.sql), [principals](../../sql/azure-baseline/_database/principals.json), [schemas](../../sql/azure-baseline/_database/schemas.sql), [full-text definitions](../../sql/azure-baseline/_database/fulltext.sql) and [all dependency edges](../../sql/azure-baseline/_database/dependencies.json) are part of the contract.

**Dependency analysis limit:** dynamic SQL targets and external database definitions are unknown until each constructed SQL path is checked. Literal matching plus catalog closure is evidence, not proof that all runtime branches execute. Triggers classified as replication support must be evaluated separately from application behavior.

## Configuration and deployment

[Sanitized live settings, EasyAuth, route bindings, schedules and enablement](../azure-capture/config/aga-membership-functions.json). Values marked `<supplied securely>` come from the existing Azure app setting/identity and must be provisioned through the future VM secret store. No secret values are in Git.

[Storage container/queue inventory](../azure-capture/storage-contract.json). App-specific setting names and binding `connection`/`queueName`/`schedule` fields identify its resources. Dynamically constructed blob prefixes remain in the source. Do not infer timer ownership from app names alone.

[Captured host configuration](../../azure-baseline/aga-membership-functions/host.json). [Captured dependency declaration](../../azure-baseline/aga-membership-functions/requirements.txt). [Original packaged operations notes](../../azure-baseline/aga-membership-functions/README.md).

Build a reviewed source ZIP without executing code or contacting Azure:

```powershell
python scripts/package_baseline.py aga-membership-functions --output ../packages/aga-membership-functions-source.zip
```

For a separately authorized future Azure deployment, install the declared dependencies using the recorded Python version or use the Azure remote build. The historical deployment interface is:

```powershell
az functionapp deployment source config-zip --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --resource-group aga-data-platform --name aga-membership-functions --src ../packages/aga-membership-functions-source.zip --build-remote true
```

**Do not execute that deployment command during baseline or reconciliation work.** Source ZIPs omit installed dependencies and production data. Deployment scripts embedded in the package are preserved; existing repository scripts are under [scripts/](../../scripts/). This command records the interface and is not a deployment performed by this task.

## Routes, authentication and schedules

| Function | Trigger contract | Enabled |
|---|---|---|
| `AGALookup` | type=httpTrigger; route=AGALookup; methods=['GET']; authLevel=ANONYMOUS | True |
| `GenerateTDListA` | type=httpTrigger; route=GenerateTDListA; methods=['GET']; authLevel=ANONYMOUS | True |
| `GenerateTDListB` | type=httpTrigger; route=GenerateTDListB; methods=['GET']; authLevel=ANONYMOUS | True |
| `GenerateTDListN` | type=httpTrigger; route=GenerateTDListN; methods=['GET']; authLevel=ANONYMOUS | True |
| `import_memchap` | type=httpTrigger; route=import_memchap; methods=['POST']; authLevel=FUNCTION | True |
| `LookupMembers` | type=httpTrigger; route=lookup-members; methods=['GET']; authLevel=ANONYMOUS | True |
| `TDListShortA` | type=httpTrigger; route=tda; methods=['GET']; authLevel=ANONYMOUS | True |
| `TDListShortB` | type=httpTrigger; route=tdb; methods=['GET']; authLevel=ANONYMOUS | True |
| `TDListShortN` | type=httpTrigger; route=tdn; methods=['GET']; authLevel=ANONYMOUS | True |

Trigger ANONYMOUS does not bypass handler/EasyAuth authorization. Preserve the checks in the entry point and imported auth modules. Azure settings may disable a trigger independently; consult both settings and binding metadata.

## Differences and verification

6 captured source/assets files. Static syntax, reachable imports, per-file hashes and ZIP CRC checks passed. No handler was invoked against production. [Per-file deployed/development/local comparison](../azure-capture/source-comparison.json) distinguishes the original GitHub development layout and preserved local work.

The normal development folder may lag or diverge from the deployment. Future VM work starts from this appâ€™s captured behavior; do not overwrite newer development work. Use a separate `vm-stack/` implementation when migration begins.
