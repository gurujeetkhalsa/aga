# aga-chapter-rewards-automation

Rewards SQL procedures, award/expiry/snapshot timers and Gmail renewal digest. One scheduler/mail owner only.

- Repository: [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga). Resource group: `aga-data-platform`.
- Exact captured source: [azure-baseline/aga-chapter-rewards-automation/](../../azure-baseline/aga-chapter-rewards-automation/). Development: [chapter-rewards-automation-app/](../../chapter-rewards-automation-app/).
- Host: `aga-chapter-rewards-automation.azurewebsites.net`; state `Running`; runtime `{"name": "python", "version": "3.13"}`.
- Capture: `2026-10-13T17:10:43.086398+00:00`; source status: **recovered active deployment**.
- Active package: `https://agafunc0522.blob.core.windows.net/app-package-agachapterrewardsautomation-4113466/released-package.zip`. SHA-256 `3b92c41675b8da5217771487513c072f12161234e4b16293dc7e73f47d908b92`.
- Release metadata and individual file hashes: [manifest](../azure-capture/manifests/aga-chapter-rewards-automation.json). Runtime `function_app.py` byte hash matches captured source.

## Shared and application modules

| Captured module | Maintained development copy |
|---|---|
| Entry point is self-contained application code | See `function_app.py` |

These are package-local copies, not links to the repository root. Their versions may differ between apps. Never deduplicate merely by filename. Third-party imports are declared in [requirements.txt](../../azure-baseline/aga-chapter-rewards-automation/requirements.txt); the [static verification report](../azure-capture/verification.json) records the reachable module graph.

## Database

| SQL definition | Relationship | Classification |
|---|---|---|
| [dbrepl_meta.generations](../../sql/azure-baseline/dbrepl_meta/generations.sql) | catalog dependency or attached trigger | tooling/replication |
| [membership.chapters](../../sql/azure-baseline/membership/chapters.sql) | catalog dependency or attached trigger | application |
| [membership.members](../../sql/azure-baseline/membership/members.sql) | catalog dependency or attached trigger | application |
| [ratings.bayrate_reward_reconciliations](../../sql/azure-baseline/ratings/bayrate_reward_reconciliations.sql) | catalog dependency or attached trigger | application |
| [ratings.dbrepl_dirty_1234103437](../../sql/azure-baseline/ratings/dbrepl_dirty_1234103437.sql) | catalog dependency or attached trigger | tooling/replication |
| [ratings.dbrepl_dirty_1266103551](../../sql/azure-baseline/ratings/dbrepl_dirty_1266103551.sql) | catalog dependency or attached trigger | tooling/replication |
| [ratings.games](../../sql/azure-baseline/ratings/games.sql) | catalog dependency or attached trigger | application |
| [ratings.tournaments](../../sql/azure-baseline/ratings/tournaments.sql) | catalog dependency or attached trigger | application |
| [rewards.chapter_daily_snapshot](../../sql/azure-baseline/rewards/chapter_daily_snapshot.sql) | catalog dependency or attached trigger | application |
| [rewards.chapter_eligibility_periods](../../sql/azure-baseline/rewards/chapter_eligibility_periods.sql) | catalog dependency or attached trigger | application |
| [rewards.chapter_renewal_notice_results](../../sql/azure-baseline/rewards/chapter_renewal_notice_results.sql) | catalog dependency or attached trigger | application |
| [rewards.lot_allocations](../../sql/azure-baseline/rewards/lot_allocations.sql) | catalog dependency or attached trigger | application |
| [rewards.member_daily_snapshot](../../sql/azure-baseline/rewards/member_daily_snapshot.sql) | catalog dependency or attached trigger | application |
| [rewards.membership_events](../../sql/azure-baseline/rewards/membership_events.sql) | catalog dependency or attached trigger | application |
| [rewards.point_lots](../../sql/azure-baseline/rewards/point_lots.sql) | catalog dependency or attached trigger | application |
| [rewards.reward_runs](../../sql/azure-baseline/rewards/reward_runs.sql) | catalog dependency or attached trigger | application |
| [rewards.sp_create_daily_snapshot](../../sql/azure-baseline/rewards/sp_create_daily_snapshot.sql) | literal reference in packaged source | application |
| [rewards.sp_get_pending_chapter_renewals](../../sql/azure-baseline/rewards/sp_get_pending_chapter_renewals.sql) | literal reference in packaged source | application |
| [rewards.sp_process_membership_awards](../../sql/azure-baseline/rewards/sp_process_membership_awards.sql) | literal reference in packaged source | application |
| [rewards.sp_process_point_expirations](../../sql/azure-baseline/rewards/sp_process_point_expirations.sql) | literal reference in packaged source | application |
| [rewards.sp_process_rated_game_awards](../../sql/azure-baseline/rewards/sp_process_rated_game_awards.sql) | literal reference in packaged source | application |
| [rewards.sp_process_tournament_awards](../../sql/azure-baseline/rewards/sp_process_tournament_awards.sql) | literal reference in packaged source | application |
| [rewards.transactions](../../sql/azure-baseline/rewards/transactions.sql) | catalog dependency or attached trigger | application |

Every SQL file has a metadata companion. [Database permissions](../../sql/azure-baseline/_database/permissions.sql), [principals](../../sql/azure-baseline/_database/principals.json), [schemas](../../sql/azure-baseline/_database/schemas.sql), [full-text definitions](../../sql/azure-baseline/_database/fulltext.sql) and [all dependency edges](../../sql/azure-baseline/_database/dependencies.json) are part of the contract.

**Dependency analysis limit:** dynamic SQL targets and external database definitions are unknown until each constructed SQL path is checked. Literal matching plus catalog closure is evidence, not proof that all runtime branches execute. Triggers classified as replication support must be evaluated separately from application behavior.

## Configuration and deployment

[Sanitized live settings, EasyAuth, route bindings, schedules and enablement](../azure-capture/config/aga-chapter-rewards-automation.json). Values marked `<supplied securely>` come from the existing Azure app setting/identity and must be provisioned through the future VM secret store. No secret values are in Git.

[Storage container/queue inventory](../azure-capture/storage-contract.json). App-specific setting names and binding `connection`/`queueName`/`schedule` fields identify its resources. Dynamically constructed blob prefixes remain in the source. Do not infer timer ownership from app names alone.

[Captured host configuration](../../azure-baseline/aga-chapter-rewards-automation/host.json). [Captured dependency declaration](../../azure-baseline/aga-chapter-rewards-automation/requirements.txt). [Original packaged operations notes](../../azure-baseline/aga-chapter-rewards-automation/README.md).

Build a reviewed source ZIP without executing code or contacting Azure:

```powershell
python scripts/package_baseline.py aga-chapter-rewards-automation --output ../packages/aga-chapter-rewards-automation-source.zip
```

For a separately authorized future Azure deployment, install the declared dependencies using the recorded Python version or use the Azure remote build. The historical deployment interface is:

```powershell
az functionapp deployment source config-zip --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --resource-group aga-data-platform --name aga-chapter-rewards-automation --src ../packages/aga-chapter-rewards-automation-source.zip --build-remote true
```

**Do not execute that deployment command during baseline or reconciliation work.** Source ZIPs omit installed dependencies and production data. Deployment scripts embedded in the package are preserved; existing repository scripts are under [scripts/](../../scripts/). This command records the interface and is not a deployment performed by this task.

## Routes, authentication and schedules

| Function | Trigger contract | Enabled |
|---|---|---|
| `create_rewards_daily_snapshot` | type=timerTrigger; schedule=0 10 5 * * * | True |
| `process_rewards_membership_awards` | type=timerTrigger; schedule=0 20 5 * * * | True |
| `process_rewards_point_expirations` | type=timerTrigger; schedule=0 40 5 * * * | True |
| `process_rewards_rated_game_awards` | type=timerTrigger; schedule=0 30 5 * * * | True |
| `process_rewards_tournament_awards` | type=timerTrigger; schedule=0 35 5 * * * | True |
| `send_pending_chapter_renewals_email` | type=timerTrigger; schedule=0 50 5 * * * | True |

Trigger ANONYMOUS does not bypass handler/EasyAuth authorization. Preserve the checks in the entry point and imported auth modules. Azure settings may disable a trigger independently; consult both settings and binding metadata.

## Differences and verification

6 captured source/assets files. Static syntax, reachable imports, per-file hashes and ZIP CRC checks passed. No handler was invoked against production. [Per-file deployed/development/local comparison](../azure-capture/source-comparison.json) distinguishes the original GitHub development layout and preserved local work.

The normal development folder may lag or diverge from the deployment. Future VM work starts from this appâ€™s captured behavior; do not overwrite newer development work. Use a separate `vm-stack/` implementation when migration begins.
