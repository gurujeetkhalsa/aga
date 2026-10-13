# aga-bayrate

Azure SQL rating/reward writes and snapshot refresh requests. Preserve operator authorization, rerun reconciliation and snapshot ownership.

- Repository: [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga). Resource group: `aga-data-platform`.
- Exact captured source: [azure-baseline/aga-bayrate/](../../azure-baseline/aga-bayrate/). Development: [bayrate-app/](../../bayrate-app/).
- Host: `aga-bayrate.azurewebsites.net`; state `Running`; runtime `{"name": "python", "version": "3.13"}`.
- Capture: `2026-10-13T17:10:03.455374+00:00`; source status: **recovered active deployment**.
- Active package: `https://agafunc0522.blob.core.windows.net/app-package-agabayrate-4644553/released-package.zip`. SHA-256 `4b786bb0a273334764016caaf5d2f5449a88f2fc247ce1bbb65ef975dcd0ec9d`.
- Release metadata and individual file hashes: [manifest](../azure-capture/manifests/aga-bayrate.json). Runtime `function_app.py` byte hash matches captured source.

## Shared and application modules

| Captured module | Maintained development copy |
|---|---|
| [bayrate/__init__.py](../../azure-baseline/aga-bayrate/bayrate/__init__.py) | [bayrate/__init__.py](../../bayrate/__init__.py) |
| [bayrate/auth.py](../../azure-baseline/aga-bayrate/bayrate/auth.py) | [bayrate/auth.py](../../bayrate/auth.py) |
| [bayrate/commit_staged_run.py](../../azure-baseline/aga-bayrate/bayrate/commit_staged_run.py) | [bayrate/commit_staged_run.py](../../bayrate/commit_staged_run.py) |
| [bayrate/core.py](../../azure-baseline/aga-bayrate/bayrate/core.py) | [bayrate/core.py](../../bayrate/core.py) |
| [bayrate/replay_staged_run.py](../../azure-baseline/aga-bayrate/bayrate/replay_staged_run.py) | [bayrate/replay_staged_run.py](../../bayrate/replay_staged_run.py) |
| [bayrate/report_parser.py](../../azure-baseline/aga-bayrate/bayrate/report_parser.py) | [bayrate/report_parser.py](../../bayrate/report_parser.py) |
| [bayrate/reward_reconciliation.py](../../azure-baseline/aga-bayrate/bayrate/reward_reconciliation.py) | [bayrate/reward_reconciliation.py](../../bayrate/reward_reconciliation.py) |
| [bayrate/snapshot_refresh.py](../../azure-baseline/aga-bayrate/bayrate/snapshot_refresh.py) | [bayrate/snapshot_refresh.py](../../bayrate/snapshot_refresh.py) |
| [bayrate/sql_adapter.py](../../azure-baseline/aga-bayrate/bayrate/sql_adapter.py) | [bayrate/sql_adapter.py](../../bayrate/sql_adapter.py) |
| [bayrate/stage_reports.py](../../azure-baseline/aga-bayrate/bayrate/stage_reports.py) | [bayrate/stage_reports.py](../../bayrate/stage_reports.py) |

These are package-local copies, not links to the repository root. Their versions may differ between apps. Never deduplicate merely by filename. Third-party imports are declared in [requirements.txt](../../azure-baseline/aga-bayrate/requirements.txt); the [static verification report](../azure-capture/verification.json) records the reachable module graph.

## Database

| SQL definition | Relationship | Classification |
|---|---|---|
| [dbrepl_meta.generations](../../sql/azure-baseline/dbrepl_meta/generations.sql) | catalog dependency or attached trigger | tooling/replication |
| [membership.chapters](../../sql/azure-baseline/membership/chapters.sql) | literal reference in packaged source | application |
| [membership.members](../../sql/azure-baseline/membership/members.sql) | literal reference in packaged source | application |
| [ratings.admin_permissions](../../sql/azure-baseline/ratings/admin_permissions.sql) | literal reference in packaged source | application |
| [ratings.bayrate_reward_reconciliations](../../sql/azure-baseline/ratings/bayrate_reward_reconciliations.sql) | literal reference in packaged source | application |
| [ratings.bayrate_run_id_seq](../../sql/azure-baseline/ratings/bayrate_run_id_seq.sql) | literal reference in packaged source | application |
| [ratings.bayrate_runs](../../sql/azure-baseline/ratings/bayrate_runs.sql) | literal reference in packaged source | application |
| [ratings.bayrate_staged_games](../../sql/azure-baseline/ratings/bayrate_staged_games.sql) | literal reference in packaged source | application |
| [ratings.bayrate_staged_ratings](../../sql/azure-baseline/ratings/bayrate_staged_ratings.sql) | literal reference in packaged source | application |
| [ratings.bayrate_staged_tournaments](../../sql/azure-baseline/ratings/bayrate_staged_tournaments.sql) | literal reference in packaged source | application |
| [ratings.dbrepl_dirty_1234103437](../../sql/azure-baseline/ratings/dbrepl_dirty_1234103437.sql) | catalog dependency or attached trigger | tooling/replication |
| [ratings.dbrepl_dirty_1250103494](../../sql/azure-baseline/ratings/dbrepl_dirty_1250103494.sql) | catalog dependency or attached trigger | tooling/replication |
| [ratings.dbrepl_dirty_1266103551](../../sql/azure-baseline/ratings/dbrepl_dirty_1266103551.sql) | catalog dependency or attached trigger | tooling/replication |
| [ratings.games](../../sql/azure-baseline/ratings/games.sql) | literal reference in packaged source | application |
| [ratings.ratings](../../sql/azure-baseline/ratings/ratings.sql) | literal reference in packaged source | application |
| [ratings.tournaments](../../sql/azure-baseline/ratings/tournaments.sql) | literal reference in packaged source | application |
| [rewards.chapter_daily_snapshot](../../sql/azure-baseline/rewards/chapter_daily_snapshot.sql) | literal reference in packaged source | application |
| [rewards.member_daily_snapshot](../../sql/azure-baseline/rewards/member_daily_snapshot.sql) | literal reference in packaged source | application |
| [rewards.point_lots](../../sql/azure-baseline/rewards/point_lots.sql) | catalog dependency or attached trigger | application |
| [rewards.reward_runs](../../sql/azure-baseline/rewards/reward_runs.sql) | catalog dependency or attached trigger | application |
| [rewards.sp_process_tournament_awards](../../sql/azure-baseline/rewards/sp_process_tournament_awards.sql) | literal reference in packaged source | application |
| [rewards.transactions](../../sql/azure-baseline/rewards/transactions.sql) | catalog dependency or attached trigger | application |

Every SQL file has a metadata companion. [Database permissions](../../sql/azure-baseline/_database/permissions.sql), [principals](../../sql/azure-baseline/_database/principals.json), [schemas](../../sql/azure-baseline/_database/schemas.sql), [full-text definitions](../../sql/azure-baseline/_database/fulltext.sql) and [all dependency edges](../../sql/azure-baseline/_database/dependencies.json) are part of the contract.

**Dependency analysis limit:** dynamic SQL targets and external database definitions are unknown until each constructed SQL path is checked. Literal matching plus catalog closure is evidence, not proof that all runtime branches execute. Triggers classified as replication support must be evaluated separately from application behavior.

## Configuration and deployment

[Sanitized live settings, EasyAuth, route bindings, schedules and enablement](../azure-capture/config/aga-bayrate.json). Values marked `<supplied securely>` come from the existing Azure app setting/identity and must be provisioned through the future VM secret store. No secret values are in Git.

[Storage container/queue inventory](../azure-capture/storage-contract.json). App-specific setting names and binding `connection`/`queueName`/`schedule` fields identify its resources. Dynamically constructed blob prefixes remain in the source. Do not infer timer ownership from app names alone.

[Captured host configuration](../../azure-baseline/aga-bayrate/host.json). [Captured dependency declaration](../../azure-baseline/aga-bayrate/requirements.txt). [Original packaged operations notes](../../azure-baseline/aga-bayrate/README.md).

Build a reviewed source ZIP without executing code or contacting Azure:

```powershell
python scripts/package_baseline.py aga-bayrate --output ../packages/aga-bayrate-source.zip
```

For a separately authorized future Azure deployment, install the declared dependencies using the recorded Python version or use the Azure remote build. The historical deployment interface is:

```powershell
az functionapp deployment source config-zip --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --resource-group aga-data-platform --name aga-bayrate --src ../packages/aga-bayrate-source.zip --build-remote true
```

**Do not execute that deployment command during baseline or reconciliation work.** Source ZIPs omit installed dependencies and production data. Deployment scripts embedded in the package are preserved; existing repository scripts are under [scripts/](../../scripts/). This command records the interface and is not a deployment performed by this task.

## Routes, authentication and schedules

| Function | Trigger contract | Enabled |
|---|---|---|
| `BayRateMetadataOptions` | type=httpTrigger; route=bayrate/metadata-options; methods=['GET', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateStagingCommit` | type=httpTrigger; route=bayrate/commit; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateStagingCommitPreview` | type=httpTrigger; route=bayrate/commit-preview; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateStagingPage` | type=httpTrigger; route=bayrate; methods=['GET']; authLevel=ANONYMOUS | True |
| `BayRateStagingPreview` | type=httpTrigger; route=bayrate/preview; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateStagingReplay` | type=httpTrigger; route=bayrate/replay; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateStagingReview` | type=httpTrigger; route=bayrate/review; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateStagingRun` | type=httpTrigger; route=bayrate/run; methods=['GET', 'POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateStagingWrite` | type=httpTrigger; route=bayrate/stage; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |

Trigger ANONYMOUS does not bypass handler/EasyAuth authorization. Preserve the checks in the entry point and imported auth modules. Azure settings may disable a trigger independently; consult both settings and binding metadata.

## Differences and verification

18 captured source/assets files. Static syntax, reachable imports, per-file hashes and ZIP CRC checks passed. No handler was invoked against production. [Per-file deployed/development/local comparison](../azure-capture/source-comparison.json) distinguishes the original GitHub development layout and preserved local work.

The normal development folder may lag or diverge from the deployment. Future VM work starts from this appâ€™s captured behavior; do not overwrite newer development work. Use a separate `vm-stack/` implementation when migration begins.
