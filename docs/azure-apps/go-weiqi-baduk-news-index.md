# go-weiqi-baduk-news-index

External news sites, translation service, spaCy name indexing, blob-backed index/history, timer and queue processing. Preserve translation budget/pacing and queue deduplication.

- Repository: [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga). Resource group: `aga-data-platform`.
- Exact captured source: [azure-baseline/go-weiqi-baduk-news-index/](../../azure-baseline/go-weiqi-baduk-news-index/). Development: [go-weiqi-baduk-news-index-app/](../../go-weiqi-baduk-news-index-app/).
- Host: `go-weiqi-baduk-news-index.azurewebsites.net`; state `Running`; runtime `{"name": "python", "version": "3.11"}`.
- Capture: `2026-10-13T17:17:38.401701+00:00`; source status: **recovered active deployment**.
- Active package: `https://agafunc0522.blob.core.windows.net/go-news-index-deployment/released-package.zip`. SHA-256 `54ff2e136b39c88b3da3bf325b250380d6968ef1a3a06b065c252a30bebc17a9`.
- Release metadata and individual file hashes: [manifest](../azure-capture/manifests/go-weiqi-baduk-news-index.json). Runtime `function_app.py` byte hash matches captured source.

## Shared and application modules

| Captured module | Maintained development copy |
|---|---|
| [additional_sources.py](../../azure-baseline/go-weiqi-baduk-news-index/additional_sources.py) | [go-weiqi-baduk-news-index-app/additional_sources.py](../../go-weiqi-baduk-news-index-app/additional_sources.py) |
| [archive_discovery.py](../../azure-baseline/go-weiqi-baduk-news-index/archive_discovery.py) | [go-weiqi-baduk-news-index-app/archive_discovery.py](../../go-weiqi-baduk-news-index-app/archive_discovery.py) |
| [history_backfill.py](../../azure-baseline/go-weiqi-baduk-news-index/history_backfill.py) | [go-weiqi-baduk-news-index-app/history_backfill.py](../../go-weiqi-baduk-news-index-app/history_backfill.py) |
| [index_core.py](../../azure-baseline/go-weiqi-baduk-news-index/index_core.py) | [go-weiqi-baduk-news-index-app/index_core.py](../../go-weiqi-baduk-news-index-app/index_core.py) |
| [legacy_sources.py](../../azure-baseline/go-weiqi-baduk-news-index/legacy_sources.py) | [go-weiqi-baduk-news-index-app/legacy_sources.py](../../go-weiqi-baduk-news-index-app/legacy_sources.py) |
| [name_index.py](../../azure-baseline/go-weiqi-baduk-news-index/name_index.py) | [go-weiqi-baduk-news-index-app/name_index.py](../../go-weiqi-baduk-news-index-app/name_index.py) |
| [national_sources.py](../../azure-baseline/go-weiqi-baduk-news-index/national_sources.py) | [go-weiqi-baduk-news-index-app/national_sources.py](../../go-weiqi-baduk-news-index-app/national_sources.py) |
| [source_discovery.py](../../azure-baseline/go-weiqi-baduk-news-index/source_discovery.py) | [go-weiqi-baduk-news-index-app/source_discovery.py](../../go-weiqi-baduk-news-index-app/source_discovery.py) |
| [tournament_index.py](../../azure-baseline/go-weiqi-baduk-news-index/tournament_index.py) | [go-weiqi-baduk-news-index-app/tournament_index.py](../../go-weiqi-baduk-news-index-app/tournament_index.py) |
| [translation_budget.py](../../azure-baseline/go-weiqi-baduk-news-index/translation_budget.py) | [go-weiqi-baduk-news-index-app/translation_budget.py](../../go-weiqi-baduk-news-index-app/translation_budget.py) |
| [translation_pacing.py](../../azure-baseline/go-weiqi-baduk-news-index/translation_pacing.py) | [go-weiqi-baduk-news-index-app/translation_pacing.py](../../go-weiqi-baduk-news-index-app/translation_pacing.py) |

These are package-local copies, not links to the repository root. Their versions may differ between apps. Never deduplicate merely by filename. Third-party imports are declared in [requirements.txt](../../azure-baseline/go-weiqi-baduk-news-index/requirements.txt); the [static verification report](../azure-capture/verification.json) records the reachable module graph.

## Database

| SQL definition | Relationship | Classification |
|---|---|---|
| No Azure SQL literal references found | Inspect config and dynamic paths | N/A |

Every SQL file has a metadata companion. [Database permissions](../../sql/azure-baseline/_database/permissions.sql), [principals](../../sql/azure-baseline/_database/principals.json), [schemas](../../sql/azure-baseline/_database/schemas.sql), [full-text definitions](../../sql/azure-baseline/_database/fulltext.sql) and [all dependency edges](../../sql/azure-baseline/_database/dependencies.json) are part of the contract.

**Dependency analysis limit:** dynamic SQL targets and external database definitions are unknown until each constructed SQL path is checked. Literal matching plus catalog closure is evidence, not proof that all runtime branches execute. Triggers classified as replication support must be evaluated separately from application behavior.

## Configuration and deployment

[Sanitized live settings, EasyAuth, route bindings, schedules and enablement](../azure-capture/config/go-weiqi-baduk-news-index.json). Values marked `<supplied securely>` come from the existing Azure app setting/identity and must be provisioned through the future VM secret store. No secret values are in Git.

[Storage container/queue inventory](../azure-capture/storage-contract.json). App-specific setting names and binding `connection`/`queueName`/`schedule` fields identify its resources. Dynamically constructed blob prefixes remain in the source. Do not infer timer ownership from app names alone.

[Captured host configuration](../../azure-baseline/go-weiqi-baduk-news-index/host.json). [Captured dependency declaration](../../azure-baseline/go-weiqi-baduk-news-index/requirements.txt). No original deployment script was embedded in the release; use the reproducible source-packaging command below.

Build a reviewed source ZIP without executing code or contacting Azure:

```powershell
python scripts/package_baseline.py go-weiqi-baduk-news-index --output ../packages/go-weiqi-baduk-news-index-source.zip
```

For a separately authorized future Azure deployment, install the declared dependencies using the recorded Python version or use the Azure remote build. The historical deployment interface is:

```powershell
az functionapp deployment source config-zip --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --resource-group aga-data-platform --name go-weiqi-baduk-news-index --src ../packages/go-weiqi-baduk-news-index-source.zip --build-remote true
```

**Do not execute that deployment command during baseline or reconciliation work.** Source ZIPs omit installed dependencies and production data. Deployment scripts embedded in the package are preserved; existing repository scripts are under [scripts/](../../scripts/). This command records the interface and is not a deployment performed by this task.

## Routes, authentication and schedules

| Function | Trigger contract | Enabled |
|---|---|---|
| `collect_source` | type=queueTrigger; queueName=go-news-index-jobs; connection=AzureWebJobsStorage | False |
| `daily_collection` | type=timerTrigger; schedule=0 0 11 * * * | False |
| `daily_history` | type=timerTrigger; schedule=0 0 * * * * | False |
| `names` | type=httpTrigger; route=news/names; authLevel=ANONYMOUS | True |
| `news` | type=httpTrigger; route=news; authLevel=ANONYMOUS | True |
| `search` | type=httpTrigger; route=news/search; authLevel=ANONYMOUS | True |
| `status` | type=httpTrigger; route=news/status; authLevel=ANONYMOUS | True |
| `tournaments` | type=httpTrigger; route=news/tournaments; authLevel=ANONYMOUS | True |

Trigger ANONYMOUS does not bypass handler/EasyAuth authorization. Preserve the checks in the entry point and imported auth modules. Azure settings may disable a trigger independently; consult both settings and binding metadata.

## Differences and verification

16 captured source/assets files. Static syntax, reachable imports, per-file hashes and ZIP CRC checks passed. No handler was invoked against production. [Per-file deployed/development/local comparison](../azure-capture/source-comparison.json) distinguishes the original GitHub development layout and preserved local work.

The normal development folder may lag or diverge from the deployment. Future VM work starts from this appâ€™s captured behavior; do not overwrite newer development work. Use a separate `vm-stack/` implementation when migration begins.
