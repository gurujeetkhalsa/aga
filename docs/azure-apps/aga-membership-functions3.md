# aga-membership-functions3

Legacy rating history HTML/SVG using SQL member names and two packaged CSV datasets. Data files are deliberately excluded from public Git; a private cutover transfer is required.

- Repository: [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga). Resource group: `aga-data-platform`.
- Exact captured source: [azure-baseline/aga-membership-functions3/](../../azure-baseline/aga-membership-functions3/). Development: [membership-functions3-app/](../../membership-functions3-app/).
- Host: `aga-membership-functions3.azurewebsites.net`; state `Running`; runtime `{"linuxFxVersion": "Python|3.11"}`.
- Capture: `2026-10-13T17:15:24.682695+00:00`; source status: **recovered current SCM SquashFS; runtime entry verified**.
- Active package: `https://agafunc0522.blob.core.windows.net/scm-releases/scm-latest-aga-membership-functions3.zip`. SHA-256 `217f5480e2b9bec79a70ddc69121d6d6e8dde494a7b0b5d6101224a45614dc0a`.
- Release metadata and individual file hashes: [manifest](../azure-capture/manifests/aga-membership-functions3.json). Runtime `function_app.py` byte hash matches captured source.

## Shared and application modules

| Captured module | Maintained development copy |
|---|---|
| [__init__.py](../../azure-baseline/aga-membership-functions3/__init__.py) | [membership-functions3-app/__init__.py](../../membership-functions3-app/__init__.py) |
| [export_legacy_event_ratings.py](../../azure-baseline/aga-membership-functions3/export_legacy_event_ratings.py) | [membership-functions3-app/export_legacy_event_ratings.py](../../membership-functions3-app/export_legacy_event_ratings.py) |
| [extract_legacy_player_history.py](../../azure-baseline/aga-membership-functions3/extract_legacy_player_history.py) | [membership-functions3-app/extract_legacy_player_history.py](../../membership-functions3-app/extract_legacy_player_history.py) |
| [extract_legacy_player_history_from_table.py](../../azure-baseline/aga-membership-functions3/extract_legacy_player_history_from_table.py) | [membership-functions3-app/extract_legacy_player_history_from_table.py](../../membership-functions3-app/extract_legacy_player_history_from_table.py) |
| [legacy_bayrate.py](../../azure-baseline/aga-membership-functions3/legacy_bayrate.py) | [membership-functions3-app/legacy_bayrate.py](../../membership-functions3-app/legacy_bayrate.py) |
| [plot_legacy_player_history.py](../../azure-baseline/aga-membership-functions3/plot_legacy_player_history.py) | [membership-functions3-app/plot_legacy_player_history.py](../../membership-functions3-app/plot_legacy_player_history.py) |
| [run_legacy_bayrate.py](../../azure-baseline/aga-membership-functions3/run_legacy_bayrate.py) | [membership-functions3-app/run_legacy_bayrate.py](../../membership-functions3-app/run_legacy_bayrate.py) |
| [sweep_legacy_bayrate.py](../../azure-baseline/aga-membership-functions3/sweep_legacy_bayrate.py) | [membership-functions3-app/sweep_legacy_bayrate.py](../../membership-functions3-app/sweep_legacy_bayrate.py) |
| [sweep_legacy_bayrate_performance.py](../../azure-baseline/aga-membership-functions3/sweep_legacy_bayrate_performance.py) | [membership-functions3-app/sweep_legacy_bayrate_performance.py](../../membership-functions3-app/sweep_legacy_bayrate_performance.py) |

These are package-local copies, not links to the repository root. Their versions may differ between apps. Never deduplicate merely by filename. Third-party imports are declared in [requirements.txt](../../azure-baseline/aga-membership-functions3/requirements.txt); the [static verification report](../azure-capture/verification.json) records the reachable module graph.

## Database

| SQL definition | Relationship | Classification |
|---|---|---|
| [membership.members](../../sql/azure-baseline/membership/members.sql) | literal reference in packaged source | application |

Every SQL file has a metadata companion. [Database permissions](../../sql/azure-baseline/_database/permissions.sql), [principals](../../sql/azure-baseline/_database/principals.json), [schemas](../../sql/azure-baseline/_database/schemas.sql), [full-text definitions](../../sql/azure-baseline/_database/fulltext.sql) and [all dependency edges](../../sql/azure-baseline/_database/dependencies.json) are part of the contract.

**Dependency analysis limit:** dynamic SQL targets and external database definitions are unknown until each constructed SQL path is checked. Literal matching plus catalog closure is evidence, not proof that all runtime branches execute. Triggers classified as replication support must be evaluated separately from application behavior.

## Configuration and deployment

[Sanitized live settings, EasyAuth, route bindings, schedules and enablement](../azure-capture/config/aga-membership-functions3.json). Values marked `<supplied securely>` come from the existing Azure app setting/identity and must be provisioned through the future VM secret store. No secret values are in Git.

[Storage container/queue inventory](../azure-capture/storage-contract.json). App-specific setting names and binding `connection`/`queueName`/`schedule` fields identify its resources. Dynamically constructed blob prefixes remain in the source. Do not infer timer ownership from app names alone.

[Captured host configuration](../../azure-baseline/aga-membership-functions3/host.json). [Captured dependency declaration](../../azure-baseline/aga-membership-functions3/requirements.txt). No original deployment script was embedded in the release; use the reproducible source-packaging command below.

Build a reviewed source ZIP without executing code or contacting Azure:

```powershell
python scripts/package_baseline.py aga-membership-functions3 --output ../packages/aga-membership-functions3-source.zip
```

For a separately authorized future Azure deployment, install the declared dependencies using the recorded Python version or use the Azure remote build. The historical deployment interface is:

```powershell
az functionapp deployment source config-zip --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --resource-group aga-data-platform --name aga-membership-functions3 --src ../packages/aga-membership-functions3-source.zip --build-remote true
```

**Do not execute that deployment command during baseline or reconciliation work.** Source ZIPs omit installed dependencies and production data. Deployment scripts embedded in the package are preserved; existing repository scripts are under [scripts/](../../scripts/). This command records the interface and is not a deployment performed by this task.

## Routes, authentication and schedules

| Function | Trigger contract | Enabled |
|---|---|---|
| `LegacyRatingHistoryChart` | type=httpTrigger; route=legacy-rating-history-chart; methods=['GET']; authLevel=ANONYMOUS | True |
| `LegacyRatingHistoryChartSvg` | type=httpTrigger; route=legacy-rating-history-chart.svg; methods=['GET']; authLevel=ANONYMOUS | True |

Trigger ANONYMOUS does not bypass handler/EasyAuth authorization. Preserve the checks in the entry point and imported auth modules. Azure settings may disable a trigger independently; consult both settings and binding metadata.

## Differences and verification

13 captured source/assets files. Static syntax, reachable imports, per-file hashes and ZIP CRC checks passed. No handler was invoked against production. [Per-file deployed/development/local comparison](../azure-capture/source-comparison.json) distinguishes the original GitHub development layout and preserved local work.

The normal development folder may lag or diverge from the deployment. Future VM work starts from this appâ€™s captured behavior; do not overwrite newer development work. Use a separate `vm-stack/` implementation when migration begins.

## Private data prerequisites

The original SCM archive is SquashFS despite its `.zip` suffix. It was decoded with `dissect.squashfs`; live VFS returned HTTP 200 and matched its entry point. No replacement code was invented.

`data/aga-ratings.csv` and `data/legacy-bayrate-best-event-ratings.csv` are required production datasets, excluded from public Git under the userâ€™s data restriction. Generated `legacy_bayrate_*.json` experiments are also excluded. Transfer the CSVs privately from the current SCM package during the separately authorized data migration, or use the `LEGACY_CHART_<FILENAME_NORMALIZED>` environment overrides documented by `_resolve_data_file`. Their absence from Git is intentional; this source package alone cannot serve actual rating history without those datasets.
