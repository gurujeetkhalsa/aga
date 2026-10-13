# aga-ratings-sgf-upload

Authenticated SGF archive/import matching, SQL game records, blob upload and snapshot refresh. Preserve import authorization and replacement rules.

- Repository: [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga). Resource group: `aga-data-platform`.
- Exact captured source: [azure-baseline/aga-ratings-sgf-upload/](../../azure-baseline/aga-ratings-sgf-upload/). Development: [ratings-sgf-upload-app/](../../ratings-sgf-upload-app/).
- Host: `aga-ratings-sgf-upload.azurewebsites.net`; state `Running`; runtime `{"name": "python", "version": "3.13"}`.
- Capture: `2026-10-13T17:17:12.636637+00:00`; source status: **recovered active deployment**.
- Active package: `https://agafunc0522.blob.core.windows.net/app-package-agaratingssgfupload-0967613/released-package.zip`. SHA-256 `f3e74443971408e25878dc2aa515be66a5b28fab8e30824b524f8f13e6be9cea`.
- Release metadata and individual file hashes: [manifest](../azure-capture/manifests/aga-ratings-sgf-upload.json). Runtime `function_app.py` byte hash matches captured source.

## Shared and application modules

| Captured module | Maintained development copy |
|---|---|
| [auth.py](../../azure-baseline/aga-ratings-sgf-upload/auth.py) | [ratings-sgf-upload-app/auth.py](../../ratings-sgf-upload-app/auth.py) |
| [importer.py](../../azure-baseline/aga-ratings-sgf-upload/importer.py) | [ratings-sgf-upload-app/importer.py](../../ratings-sgf-upload-app/importer.py) |
| [storage.py](../../azure-baseline/aga-ratings-sgf-upload/storage.py) | [ratings-sgf-upload-app/storage.py](../../ratings-sgf-upload-app/storage.py) |

These are package-local copies, not links to the repository root. Their versions may differ between apps. Never deduplicate merely by filename. Third-party imports are declared in [requirements.txt](../../azure-baseline/aga-ratings-sgf-upload/requirements.txt); the [static verification report](../azure-capture/verification.json) records the reachable module graph.

## Database

| SQL definition | Relationship | Classification |
|---|---|---|
| [dbrepl_meta.generations](../../sql/azure-baseline/dbrepl_meta/generations.sql) | catalog dependency or attached trigger | tooling/replication |
| [ratings.admin_permissions](../../sql/azure-baseline/ratings/admin_permissions.sql) | literal reference in packaged source | application |
| [ratings.dbrepl_dirty_1266103551](../../sql/azure-baseline/ratings/dbrepl_dirty_1266103551.sql) | catalog dependency or attached trigger | tooling/replication |
| [ratings.games](../../sql/azure-baseline/ratings/games.sql) | literal reference in packaged source | application |

Every SQL file has a metadata companion. [Database permissions](../../sql/azure-baseline/_database/permissions.sql), [principals](../../sql/azure-baseline/_database/principals.json), [schemas](../../sql/azure-baseline/_database/schemas.sql), [full-text definitions](../../sql/azure-baseline/_database/fulltext.sql) and [all dependency edges](../../sql/azure-baseline/_database/dependencies.json) are part of the contract.

**Dependency analysis limit:** dynamic SQL targets and external database definitions are unknown until each constructed SQL path is checked. Literal matching plus catalog closure is evidence, not proof that all runtime branches execute. Triggers classified as replication support must be evaluated separately from application behavior.

## Configuration and deployment

[Sanitized live settings, EasyAuth, route bindings, schedules and enablement](../azure-capture/config/aga-ratings-sgf-upload.json). Values marked `<supplied securely>` come from the existing Azure app setting/identity and must be provisioned through the future VM secret store. No secret values are in Git.

[Storage container/queue inventory](../azure-capture/storage-contract.json). App-specific setting names and binding `connection`/`queueName`/`schedule` fields identify its resources. Dynamically constructed blob prefixes remain in the source. Do not infer timer ownership from app names alone.

[Captured host configuration](../../azure-baseline/aga-ratings-sgf-upload/host.json). [Captured dependency declaration](../../azure-baseline/aga-ratings-sgf-upload/requirements.txt). No original deployment script was embedded in the release; use the reproducible source-packaging command below.

Build a reviewed source ZIP without executing code or contacting Azure:

```powershell
python scripts/package_baseline.py aga-ratings-sgf-upload --output ../packages/aga-ratings-sgf-upload-source.zip
```

For a separately authorized future Azure deployment, install the declared dependencies using the recorded Python version or use the Azure remote build. The historical deployment interface is:

```powershell
az functionapp deployment source config-zip --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --resource-group aga-data-platform --name aga-ratings-sgf-upload --src ../packages/aga-ratings-sgf-upload-source.zip --build-remote true
```

**Do not execute that deployment command during baseline or reconciliation work.** Source ZIPs omit installed dependencies and production data. Deployment scripts embedded in the package are preserved; existing repository scripts are under [scripts/](../../scripts/). This command records the interface and is not a deployment performed by this task.

## Routes, authentication and schedules

| Function | Trigger contract | Enabled |
|---|---|---|
| `SgfUploadApply` | type=httpTrigger; route=sgf-upload/apply; methods=['POST']; authLevel=ANONYMOUS | True |
| `SgfUploadHealth` | type=httpTrigger; route=sgf-upload/health; methods=['GET']; authLevel=ANONYMOUS | True |
| `SgfUploadMe` | type=httpTrigger; route=sgf-upload/me; methods=['GET']; authLevel=ANONYMOUS | True |
| `SgfUploadPage` | type=httpTrigger; route=sgf-upload; methods=['GET']; authLevel=ANONYMOUS | True |
| `SgfUploadPreview` | type=httpTrigger; route=sgf-upload/preview; methods=['POST']; authLevel=ANONYMOUS | True |
| `SgfUploadSqlHealth` | type=httpTrigger; route=sgf-upload/health/sql; methods=['GET']; authLevel=FUNCTION | True |

Trigger ANONYMOUS does not bypass handler/EasyAuth authorization. Preserve the checks in the entry point and imported auth modules. Azure settings may disable a trigger independently; consult both settings and binding metadata.

## Differences and verification

9 captured source/assets files. Static syntax, reachable imports, per-file hashes and ZIP CRC checks passed. No handler was invoked against production. [Per-file deployed/development/local comparison](../azure-capture/source-comparison.json) distinguishes the original GitHub development layout and preserved local work.

The normal development folder may lag or diverge from the deployment. Future VM work starts from this appâ€™s captured behavior; do not overwrite newer development work. Use a separate `vm-stack/` implementation when migration begins.
