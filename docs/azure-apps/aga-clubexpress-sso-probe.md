# aga-clubexpress-sso-probe

ClubExpress SSO callback/token investigation and capture storage. Preserve callback/token validation; do not publish captured user tokens.

- Repository: [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga). Resource group: `aga-data-platform`.
- Exact captured source: [azure-baseline/aga-clubexpress-sso-probe/](../../azure-baseline/aga-clubexpress-sso-probe/). Development: [clubexpress-sso-probe-app/](../../clubexpress-sso-probe-app/).
- Host: `aga-clubexpress-sso-probe.azurewebsites.net`; state `Running`; runtime `{"name": "python", "version": "3.13"}`.
- Capture: `2026-10-13T17:12:48.357629+00:00`; source status: **recovered active deployment**.
- Active package: `https://agafunc0522.blob.core.windows.net/app-package-agaclubexpressssoprobe-9551475/released-package.zip`. SHA-256 `cbea1dbc360351ff25547522cc2862f276398e7a8ec60c7819f9dbd92d609a12`.
- Release metadata and individual file hashes: [manifest](../azure-capture/manifests/aga-clubexpress-sso-probe.json). Runtime `function_app.py` byte hash matches captured source.

## Shared and application modules

| Captured module | Maintained development copy |
|---|---|
| Entry point is self-contained application code | See `function_app.py` |

These are package-local copies, not links to the repository root. Their versions may differ between apps. Never deduplicate merely by filename. Third-party imports are declared in [requirements.txt](../../azure-baseline/aga-clubexpress-sso-probe/requirements.txt); the [static verification report](../azure-capture/verification.json) records the reachable module graph.

## Database

| SQL definition | Relationship | Classification |
|---|---|---|
| No Azure SQL literal references found | Inspect config and dynamic paths | N/A |

Every SQL file has a metadata companion. [Database permissions](../../sql/azure-baseline/_database/permissions.sql), [principals](../../sql/azure-baseline/_database/principals.json), [schemas](../../sql/azure-baseline/_database/schemas.sql), [full-text definitions](../../sql/azure-baseline/_database/fulltext.sql) and [all dependency edges](../../sql/azure-baseline/_database/dependencies.json) are part of the contract.

**Dependency analysis limit:** dynamic SQL targets and external database definitions are unknown until each constructed SQL path is checked. Literal matching plus catalog closure is evidence, not proof that all runtime branches execute. Triggers classified as replication support must be evaluated separately from application behavior.

## Configuration and deployment

[Sanitized live settings, EasyAuth, route bindings, schedules and enablement](../azure-capture/config/aga-clubexpress-sso-probe.json). Values marked `<supplied securely>` come from the existing Azure app setting/identity and must be provisioned through the future VM secret store. No secret values are in Git.

[Storage container/queue inventory](../azure-capture/storage-contract.json). App-specific setting names and binding `connection`/`queueName`/`schedule` fields identify its resources. Dynamically constructed blob prefixes remain in the source. Do not infer timer ownership from app names alone.

[Captured host configuration](../../azure-baseline/aga-clubexpress-sso-probe/host.json). [Captured dependency declaration](../../azure-baseline/aga-clubexpress-sso-probe/requirements.txt). [Original packaged operations notes](../../azure-baseline/aga-clubexpress-sso-probe/README.md).

Build a reviewed source ZIP without executing code or contacting Azure:

```powershell
python scripts/package_baseline.py aga-clubexpress-sso-probe --output ../packages/aga-clubexpress-sso-probe-source.zip
```

For a separately authorized future Azure deployment, install the declared dependencies using the recorded Python version or use the Azure remote build. The historical deployment interface is:

```powershell
az functionapp deployment source config-zip --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --resource-group aga-data-platform --name aga-clubexpress-sso-probe --src ../packages/aga-clubexpress-sso-probe-source.zip --build-remote true
```

**Do not execute that deployment command during baseline or reconciliation work.** Source ZIPs omit installed dependencies and production data. Deployment scripts embedded in the package are preserved; existing repository scripts are under [scripts/](../../scripts/). This command records the interface and is not a deployment performed by this task.

## Routes, authentication and schedules

| Function | Trigger contract | Enabled |
|---|---|---|
| `clubexpress_sso_probe` | type=httpTrigger; route=clubexpress-sso-probe/{probe_token}; methods=['GET', 'POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `clubexpress_sso_probe_last` | type=httpTrigger; route=clubexpress-sso-probe-last/{probe_token}; methods=['GET']; authLevel=ANONYMOUS | True |
| `clubexpress_sso_probe_last_short` | type=httpTrigger; route=l/{probe_token}; methods=['GET']; authLevel=ANONYMOUS | True |
| `clubexpress_sso_probe_short` | type=httpTrigger; route=c/{probe_token}; methods=['GET', 'POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `clubexpress_sso_probe_start` | type=httpTrigger; route=clubexpress-sso-probe-start/{probe_token}; methods=['GET']; authLevel=ANONYMOUS | True |
| `clubexpress_sso_probe_start_short` | type=httpTrigger; route=s/{probe_token}; methods=['GET']; authLevel=ANONYMOUS | True |

Trigger ANONYMOUS does not bypass handler/EasyAuth authorization. Preserve the checks in the entry point and imported auth modules. Azure settings may disable a trigger independently; consult both settings and binding metadata.

## Differences and verification

5 captured source/assets files. Static syntax, reachable imports, per-file hashes and ZIP CRC checks passed. No handler was invoked against production. [Per-file deployed/development/local comparison](../azure-capture/source-comparison.json) distinguishes the original GitHub development layout and preserved local work.

The normal development folder may lag or diverge from the deployment. Future VM work starts from this appâ€™s captured behavior; do not overwrite newer development work. Use a separate `vm-stack/` implementation when migration begins.
