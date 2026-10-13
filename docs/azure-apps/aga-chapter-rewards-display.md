# aga-chapter-rewards-display

Public chapter balances and reward reporting from Azure SQL; preserve read-only display behavior.

- Repository: [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga). Resource group: `aga-data-platform`.
- Exact captured source: [azure-baseline/aga-chapter-rewards-display/](../../azure-baseline/aga-chapter-rewards-display/). Development: [chapter-rewards-display-app/](../../chapter-rewards-display-app/).
- Host: `aga-chapter-rewards-display.azurewebsites.net`; state `Running`; runtime `{"name": "python", "version": "3.13"}`.
- Capture: `2026-10-13T17:10:50.538422+00:00`; source status: **recovered active deployment**.
- Active package: `https://agafunc0522.blob.core.windows.net/app-package-agachapterrewardsdisplay-5880534/released-package.zip`. SHA-256 `f69b8237d6364bc819f570ade8f7b5ddaed6f545ef892c5789d4766316c20fd0`.
- Release metadata and individual file hashes: [manifest](../azure-capture/manifests/aga-chapter-rewards-display.json). Runtime `function_app.py` byte hash matches captured source.

## Shared and application modules

| Captured module | Maintained development copy |
|---|---|
| [chapter_rewards_support.py](../../azure-baseline/aga-chapter-rewards-display/chapter_rewards_support.py) | [chapter-rewards-display-app/chapter_rewards_support.py](../../chapter-rewards-display-app/chapter_rewards_support.py) |

These are package-local copies, not links to the repository root. Their versions may differ between apps. Never deduplicate merely by filename. Third-party imports are declared in [requirements.txt](../../azure-baseline/aga-chapter-rewards-display/requirements.txt); the [static verification report](../azure-capture/verification.json) records the reachable module graph.

## Database

| SQL definition | Relationship | Classification |
|---|---|---|
| [membership.chapters](../../sql/azure-baseline/membership/chapters.sql) | literal reference in packaged source | application |
| [rewards.chapter_daily_snapshot](../../sql/azure-baseline/rewards/chapter_daily_snapshot.sql) | catalog dependency or attached trigger | application |
| [rewards.lot_allocations](../../sql/azure-baseline/rewards/lot_allocations.sql) | catalog dependency or attached trigger | application |
| [rewards.point_lots](../../sql/azure-baseline/rewards/point_lots.sql) | catalog dependency or attached trigger | application |
| [rewards.redemption_receipts](../../sql/azure-baseline/rewards/redemption_receipts.sql) | literal reference in packaged source | application |
| [rewards.redemption_requests](../../sql/azure-baseline/rewards/redemption_requests.sql) | literal reference in packaged source | application |
| [rewards.reward_runs](../../sql/azure-baseline/rewards/reward_runs.sql) | catalog dependency or attached trigger | application |
| [rewards.transactions](../../sql/azure-baseline/rewards/transactions.sql) | literal reference in packaged source | application |
| [rewards.v_chapter_balances](../../sql/azure-baseline/rewards/v_chapter_balances.sql) | literal reference in packaged source | application |
| [rewards.v_chapter_transaction_history](../../sql/azure-baseline/rewards/v_chapter_transaction_history.sql) | literal reference in packaged source | application |
| [rewards.v_point_lot_aging](../../sql/azure-baseline/rewards/v_point_lot_aging.sql) | literal reference in packaged source | application |

Every SQL file has a metadata companion. [Database permissions](../../sql/azure-baseline/_database/permissions.sql), [principals](../../sql/azure-baseline/_database/principals.json), [schemas](../../sql/azure-baseline/_database/schemas.sql), [full-text definitions](../../sql/azure-baseline/_database/fulltext.sql) and [all dependency edges](../../sql/azure-baseline/_database/dependencies.json) are part of the contract.

**Dependency analysis limit:** dynamic SQL targets and external database definitions are unknown until each constructed SQL path is checked. Literal matching plus catalog closure is evidence, not proof that all runtime branches execute. Triggers classified as replication support must be evaluated separately from application behavior.

## Configuration and deployment

[Sanitized live settings, EasyAuth, route bindings, schedules and enablement](../azure-capture/config/aga-chapter-rewards-display.json). Values marked `<supplied securely>` come from the existing Azure app setting/identity and must be provisioned through the future VM secret store. No secret values are in Git.

[Storage container/queue inventory](../azure-capture/storage-contract.json). App-specific setting names and binding `connection`/`queueName`/`schedule` fields identify its resources. Dynamically constructed blob prefixes remain in the source. Do not infer timer ownership from app names alone.

[Captured host configuration](../../azure-baseline/aga-chapter-rewards-display/host.json). [Captured dependency declaration](../../azure-baseline/aga-chapter-rewards-display/requirements.txt). [Original packaged operations notes](../../azure-baseline/aga-chapter-rewards-display/README.md).

Build a reviewed source ZIP without executing code or contacting Azure:

```powershell
python scripts/package_baseline.py aga-chapter-rewards-display --output ../packages/aga-chapter-rewards-display-source.zip
```

For a separately authorized future Azure deployment, install the declared dependencies using the recorded Python version or use the Azure remote build. The historical deployment interface is:

```powershell
az functionapp deployment source config-zip --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --resource-group aga-data-platform --name aga-chapter-rewards-display --src ../packages/aga-chapter-rewards-display-source.zip --build-remote true
```

**Do not execute that deployment command during baseline or reconciliation work.** Source ZIPs omit installed dependencies and production data. Deployment scripts embedded in the package are preserved; existing repository scripts are under [scripts/](../../scripts/). This command records the interface and is not a deployment performed by this task.

## Routes, authentication and schedules

| Function | Trigger contract | Enabled |
|---|---|---|
| `ChapterRewardsBalances` | type=httpTrigger; route=chapter-rewards/balances; methods=['GET']; authLevel=ANONYMOUS | True |
| `ChapterRewardsChapter` | type=httpTrigger; route=chapter-rewards/chapter; methods=['GET']; authLevel=ANONYMOUS | True |
| `ChapterRewardsDisplayPage` | type=httpTrigger; route=chapter-rewards; methods=['GET']; authLevel=ANONYMOUS | True |

Trigger ANONYMOUS does not bypass handler/EasyAuth authorization. Preserve the checks in the entry point and imported auth modules. Azure settings may disable a trigger independently; consult both settings and binding metadata.

## Differences and verification

7 captured source/assets files. Static syntax, reachable imports, per-file hashes and ZIP CRC checks passed. No handler was invoked against production. [Per-file deployed/development/local comparison](../azure-capture/source-comparison.json) distinguishes the original GitHub development layout and preserved local work.

The normal development folder may lag or diverge from the deployment. Future VM work starts from this appâ€™s captured behavior; do not overwrite newer development work. Use a separate `vm-stack/` implementation when migration begins.
