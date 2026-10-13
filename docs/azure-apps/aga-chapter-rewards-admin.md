# aga-chapter-rewards-admin

Chapter balances, redemptions, transfers, admin scopes and receipt blob storage. Preserve approval and authorization rules.

- Repository: [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga). Resource group: `aga-data-platform`.
- Exact captured source: [azure-baseline/aga-chapter-rewards-admin/](../../azure-baseline/aga-chapter-rewards-admin/). Development: [chapter-rewards-admin-app/](../../chapter-rewards-admin-app/).
- Host: `aga-chapter-rewards-admin.azurewebsites.net`; state `Running`; runtime `{"name": "python", "version": "3.13"}`.
- Capture: `2026-10-13T17:10:27.001426+00:00`; source status: **recovered active deployment**.
- Active package: `https://agafunc0522.blob.core.windows.net/app-package-agachapterrewardsadmin-6000233/released-package.zip`. SHA-256 `7fe58260ff7c1c2b3b5f99f940d7a397152fe4a73c5c113fe43e7c1f0723680d`.
- Release metadata and individual file hashes: [manifest](../azure-capture/manifests/aga-chapter-rewards-admin.json). Runtime `function_app.py` byte hash matches captured source.

## Shared and application modules

| Captured module | Maintained development copy |
|---|---|
| [admin_auth.py](../../azure-baseline/aga-chapter-rewards-admin/admin_auth.py) | [chapter-rewards-admin-app/admin_auth.py](../../chapter-rewards-admin-app/admin_auth.py) |
| [chapter_rewards_admin_support.py](../../azure-baseline/aga-chapter-rewards-admin/chapter_rewards_admin_support.py) | [chapter-rewards-admin-app/chapter_rewards_admin_support.py](../../chapter-rewards-admin-app/chapter_rewards_admin_support.py) |
| [sql_adapter.py](../../azure-baseline/aga-chapter-rewards-admin/sql_adapter.py) | [chapter-rewards-admin-app/sql_adapter.py](../../chapter-rewards-admin-app/sql_adapter.py) |

These are package-local copies, not links to the repository root. Their versions may differ between apps. Never deduplicate merely by filename. Third-party imports are declared in [requirements.txt](../../azure-baseline/aga-chapter-rewards-admin/requirements.txt); the [static verification report](../azure-capture/verification.json) records the reachable module graph.

## Database

| SQL definition | Relationship | Classification |
|---|---|---|
| [membership.chapters](../../sql/azure-baseline/membership/chapters.sql) | literal reference in packaged source | application |
| [ratings.admin_permissions](../../sql/azure-baseline/ratings/admin_permissions.sql) | literal reference in packaged source | application |
| [rewards.chapter_daily_snapshot](../../sql/azure-baseline/rewards/chapter_daily_snapshot.sql) | catalog dependency or attached trigger | application |
| [rewards.chapter_transfers](../../sql/azure-baseline/rewards/chapter_transfers.sql) | literal reference in packaged source | application |
| [rewards.lot_allocations](../../sql/azure-baseline/rewards/lot_allocations.sql) | literal reference in packaged source | application |
| [rewards.point_lots](../../sql/azure-baseline/rewards/point_lots.sql) | literal reference in packaged source | application |
| [rewards.redemption_receipts](../../sql/azure-baseline/rewards/redemption_receipts.sql) | literal reference in packaged source | application |
| [rewards.redemption_requests](../../sql/azure-baseline/rewards/redemption_requests.sql) | literal reference in packaged source | application |
| [rewards.reward_runs](../../sql/azure-baseline/rewards/reward_runs.sql) | catalog dependency or attached trigger | application |
| [rewards.sp_add_redemption_receipt](../../sql/azure-baseline/rewards/sp_add_redemption_receipt.sql) | literal reference in packaged source | application |
| [rewards.sp_delete_redemption_receipt](../../sql/azure-baseline/rewards/sp_delete_redemption_receipt.sql) | literal reference in packaged source | application |
| [rewards.sp_post_chapter_transfer](../../sql/azure-baseline/rewards/sp_post_chapter_transfer.sql) | literal reference in packaged source | application |
| [rewards.sp_post_manual_redemption](../../sql/azure-baseline/rewards/sp_post_manual_redemption.sql) | literal reference in packaged source | application |
| [rewards.sp_update_redemption_notes](../../sql/azure-baseline/rewards/sp_update_redemption_notes.sql) | literal reference in packaged source | application |
| [rewards.transactions](../../sql/azure-baseline/rewards/transactions.sql) | literal reference in packaged source | application |
| [rewards.v_chapter_balances](../../sql/azure-baseline/rewards/v_chapter_balances.sql) | literal reference in packaged source | application |
| [rewards.v_chapter_transaction_history](../../sql/azure-baseline/rewards/v_chapter_transaction_history.sql) | literal reference in packaged source | application |
| [rewards.v_point_lot_aging](../../sql/azure-baseline/rewards/v_point_lot_aging.sql) | literal reference in packaged source | application |

Every SQL file has a metadata companion. [Database permissions](../../sql/azure-baseline/_database/permissions.sql), [principals](../../sql/azure-baseline/_database/principals.json), [schemas](../../sql/azure-baseline/_database/schemas.sql), [full-text definitions](../../sql/azure-baseline/_database/fulltext.sql) and [all dependency edges](../../sql/azure-baseline/_database/dependencies.json) are part of the contract.

**Dependency analysis limit:** dynamic SQL targets and external database definitions are unknown until each constructed SQL path is checked. Literal matching plus catalog closure is evidence, not proof that all runtime branches execute. Triggers classified as replication support must be evaluated separately from application behavior.

## Configuration and deployment

[Sanitized live settings, EasyAuth, route bindings, schedules and enablement](../azure-capture/config/aga-chapter-rewards-admin.json). Values marked `<supplied securely>` come from the existing Azure app setting/identity and must be provisioned through the future VM secret store. No secret values are in Git.

[Storage container/queue inventory](../azure-capture/storage-contract.json). App-specific setting names and binding `connection`/`queueName`/`schedule` fields identify its resources. Dynamically constructed blob prefixes remain in the source. Do not infer timer ownership from app names alone.

[Captured host configuration](../../azure-baseline/aga-chapter-rewards-admin/host.json). [Captured dependency declaration](../../azure-baseline/aga-chapter-rewards-admin/requirements.txt). [Original packaged operations notes](../../azure-baseline/aga-chapter-rewards-admin/README.md).

Build a reviewed source ZIP without executing code or contacting Azure:

```powershell
python scripts/package_baseline.py aga-chapter-rewards-admin --output ../packages/aga-chapter-rewards-admin-source.zip
```

For a separately authorized future Azure deployment, install the declared dependencies using the recorded Python version or use the Azure remote build. The historical deployment interface is:

```powershell
az functionapp deployment source config-zip --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --resource-group aga-data-platform --name aga-chapter-rewards-admin --src ../packages/aga-chapter-rewards-admin-source.zip --build-remote true
```

**Do not execute that deployment command during baseline or reconciliation work.** Source ZIPs omit installed dependencies and production data. Deployment scripts embedded in the package are preserved; existing repository scripts are under [scripts/](../../scripts/). This command records the interface and is not a deployment performed by this task.

## Routes, authentication and schedules

| Function | Trigger contract | Enabled |
|---|---|---|
| `ChapterRewardsAdminBalances` | type=httpTrigger; route=chapter-rewards/balances; methods=['GET', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterRewardsAdminChapter` | type=httpTrigger; route=chapter-rewards/chapter; methods=['GET', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterRewardsAdminDebitPost` | type=httpTrigger; route=chapter-rewards/admin/debit; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterRewardsAdminDebitPreview` | type=httpTrigger; route=chapter-rewards/admin/debit-preview; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterRewardsAdminMe` | type=httpTrigger; route=chapter-rewards/admin/me; methods=['GET', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterRewardsAdminOptions` | type=httpTrigger; route=chapter-rewards/admin/options; methods=['GET', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterRewardsAdminPage` | type=httpTrigger; route=chapter-rewards/admin; methods=['GET']; authLevel=ANONYMOUS | True |
| `ChapterRewardsAdminReceiptDelete` | type=httpTrigger; route=chapter-rewards/admin/receipt-delete; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterRewardsAdminReceiptFile` | type=httpTrigger; route=chapter-rewards/admin/receipts/{receipt_id}; methods=['GET']; authLevel=ANONYMOUS | True |
| `ChapterRewardsAdminReceiptUpload` | type=httpTrigger; route=chapter-rewards/admin/receipt-upload; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterRewardsAdminRedemption` | type=httpTrigger; route=chapter-rewards/admin/redemption; methods=['GET', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterRewardsAdminRedemptionNotes` | type=httpTrigger; route=chapter-rewards/admin/redemption-notes; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterRewardsAdminTransferPost` | type=httpTrigger; route=chapter-rewards/admin/transfer; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `ChapterRewardsAdminTransferPreview` | type=httpTrigger; route=chapter-rewards/admin/transfer-preview; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |

Trigger ANONYMOUS does not bypass handler/EasyAuth authorization. Preserve the checks in the entry point and imported auth modules. Azure settings may disable a trigger independently; consult both settings and binding metadata.

## Differences and verification

10 captured source/assets files. Static syntax, reachable imports, per-file hashes and ZIP CRC checks passed. No handler was invoked against production. [Per-file deployed/development/local comparison](../azure-capture/source-comparison.json) distinguishes the original GitHub development layout and preserved local work.

The normal development folder may lag or diverge from the deployment. Future VM work starts from this appâ€™s captured behavior; do not overwrite newer development work. Use a separate `vm-stack/` implementation when migration begins.
