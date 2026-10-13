# aga-ratings-explorer-sgf-20260407t2105

Older running mixed staging variant with its own source and configuration. Do not overwrite it with aga-ratings-explorer source or assume it is disposable.

- Repository: [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga). Resource group: `aga-data-platform`.
- Exact captured source: [azure-baseline/aga-ratings-explorer-sgf-20260407t2105/](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/). Development: [ratings-explorer-app/](../../ratings-explorer-app/).
- Host: `aga-ratings-explorer-sgf-20260407t2105.azurewebsites.net`; state `Running`; runtime `{"name": "python", "version": "3.13"}`.
- Capture: `2026-10-13T17:16:54.317725+00:00`; source status: **recovered active deployment**.
- Active package: `https://agafunc0522.blob.core.windows.net/app-package-agaratingsexplorersgf20260407t21-0706146/released-package.zip`. SHA-256 `427d28c74dfb7bc19dcfb7a59e53c40e4be8094bdbbaed922ea9ba5c0e8e80ca`.
- Release metadata and individual file hashes: [manifest](../azure-capture/manifests/aga-ratings-explorer-sgf-20260407t2105.json). Runtime `function_app.py` byte hash matches captured source.

## Shared and application modules

| Captured module | Maintained development copy |
|---|---|
| [bayrate/__init__.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/bayrate/__init__.py) | [bayrate/__init__.py](../../bayrate/__init__.py) |
| [bayrate/auth.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/bayrate/auth.py) | [bayrate/auth.py](../../bayrate/auth.py) |
| [bayrate/commit_staged_run.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/bayrate/commit_staged_run.py) | [bayrate/commit_staged_run.py](../../bayrate/commit_staged_run.py) |
| [bayrate/compare_replay_to_production.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/bayrate/compare_replay_to_production.py) | [bayrate/compare_replay_to_production.py](../../bayrate/compare_replay_to_production.py) |
| [bayrate/core.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/bayrate/core.py) | [bayrate/core.py](../../bayrate/core.py) |
| [bayrate/merge_member_ratings.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/bayrate/merge_member_ratings.py) | **Unknown**: no unambiguous maintained counterpart; use captured source and establish ownership before edits |
| [bayrate/operator.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/bayrate/operator.py) | [bayrate/operator.py](../../bayrate/operator.py) |
| [bayrate/replay_staged_run.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/bayrate/replay_staged_run.py) | [bayrate/replay_staged_run.py](../../bayrate/replay_staged_run.py) |
| [bayrate/report_parser.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/bayrate/report_parser.py) | [bayrate/report_parser.py](../../bayrate/report_parser.py) |
| [bayrate/run_bayrate.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/bayrate/run_bayrate.py) | [bayrate/run_bayrate.py](../../bayrate/run_bayrate.py) |
| [bayrate/sql_adapter.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/bayrate/sql_adapter.py) | [bayrate/sql_adapter.py](../../bayrate/sql_adapter.py) |
| [bayrate/stage_reports.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/bayrate/stage_reports.py) | [bayrate/stage_reports.py](../../bayrate/stage_reports.py) |
| [bulk_upload_sgf_csv.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/bulk_upload_sgf_csv.py) | [ratings-explorer-app/bulk_upload_sgf_csv.py](../../ratings-explorer-app/bulk_upload_sgf_csv.py) |
| [leago_sgf_import/__init__.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/leago_sgf_import/__init__.py) | **Unknown**: no unambiguous maintained counterpart; use captured source and establish ownership before edits |
| [leago_sgf_import/build_leago_sgf_manifest.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/leago_sgf_import/build_leago_sgf_manifest.py) | **Unknown**: no unambiguous maintained counterpart; use captured source and establish ownership before edits |
| [leago_sgf_import/download_leago_aga_sgfs.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/leago_sgf_import/download_leago_aga_sgfs.py) | **Unknown**: no unambiguous maintained counterpart; use captured source and establish ownership before edits |
| [leago_sgf_import/workflow.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/leago_sgf_import/workflow.py) | **Unknown**: no unambiguous maintained counterpart; use captured source and establish ownership before edits |
| [ratings_explorer_support.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/ratings_explorer_support.py) | [ratings-explorer-app/ratings_explorer_support.py](../../ratings-explorer-app/ratings_explorer_support.py) |
| [wgo_embedded_assets.py](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/wgo_embedded_assets.py) | [ratings-explorer-app/wgo_embedded_assets.py](../../ratings-explorer-app/wgo_embedded_assets.py) |

These are package-local copies, not links to the repository root. Their versions may differ between apps. Never deduplicate merely by filename. Third-party imports are declared in [requirements.txt](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/requirements.txt); the [static verification report](../azure-capture/verification.json) records the reachable module graph.

## Database

| SQL definition | Relationship | Classification |
|---|---|---|
| [dbrepl_meta.generations](../../sql/azure-baseline/dbrepl_meta/generations.sql) | catalog dependency or attached trigger | tooling/replication |
| [integration.journal_article_member_match](../../sql/azure-baseline/integration/journal_article_member_match.sql) | literal reference in packaged source | application |
| [integration.journal_review_member_match](../../sql/azure-baseline/integration/journal_review_member_match.sql) | literal reference in packaged source | application |
| [membership.chapters](../../sql/azure-baseline/membership/chapters.sql) | literal reference in packaged source | application |
| [membership.members](../../sql/azure-baseline/membership/members.sql) | literal reference in packaged source | application |
| [ratings.admin_permissions](../../sql/azure-baseline/ratings/admin_permissions.sql) | literal reference in packaged source | application |
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
| [rewards.chapter_daily_snapshot](../../sql/azure-baseline/rewards/chapter_daily_snapshot.sql) | catalog dependency or attached trigger | application |
| [rewards.lot_allocations](../../sql/azure-baseline/rewards/lot_allocations.sql) | catalog dependency or attached trigger | application |
| [rewards.point_lots](../../sql/azure-baseline/rewards/point_lots.sql) | catalog dependency or attached trigger | application |
| [rewards.redemption_receipts](../../sql/azure-baseline/rewards/redemption_receipts.sql) | literal reference in packaged source | application |
| [rewards.redemption_requests](../../sql/azure-baseline/rewards/redemption_requests.sql) | literal reference in packaged source | application |
| [rewards.reward_runs](../../sql/azure-baseline/rewards/reward_runs.sql) | catalog dependency or attached trigger | application |
| [rewards.sp_add_redemption_receipt](../../sql/azure-baseline/rewards/sp_add_redemption_receipt.sql) | literal reference in packaged source | application |
| [rewards.sp_delete_redemption_receipt](../../sql/azure-baseline/rewards/sp_delete_redemption_receipt.sql) | literal reference in packaged source | application |
| [rewards.sp_post_manual_redemption](../../sql/azure-baseline/rewards/sp_post_manual_redemption.sql) | literal reference in packaged source | application |
| [rewards.sp_update_redemption_notes](../../sql/azure-baseline/rewards/sp_update_redemption_notes.sql) | literal reference in packaged source | application |
| [rewards.transactions](../../sql/azure-baseline/rewards/transactions.sql) | literal reference in packaged source | application |
| [rewards.v_chapter_balances](../../sql/azure-baseline/rewards/v_chapter_balances.sql) | literal reference in packaged source | application |
| [rewards.v_chapter_transaction_history](../../sql/azure-baseline/rewards/v_chapter_transaction_history.sql) | literal reference in packaged source | application |
| [rewards.v_point_lot_aging](../../sql/azure-baseline/rewards/v_point_lot_aging.sql) | literal reference in packaged source | application |

Every SQL file has a metadata companion. [Database permissions](../../sql/azure-baseline/_database/permissions.sql), [principals](../../sql/azure-baseline/_database/principals.json), [schemas](../../sql/azure-baseline/_database/schemas.sql), [full-text definitions](../../sql/azure-baseline/_database/fulltext.sql) and [all dependency edges](../../sql/azure-baseline/_database/dependencies.json) are part of the contract.

**Dependency analysis limit:** dynamic SQL targets and external database definitions are unknown until each constructed SQL path is checked. Literal matching plus catalog closure is evidence, not proof that all runtime branches execute. Triggers classified as replication support must be evaluated separately from application behavior.

## Configuration and deployment

[Sanitized live settings, EasyAuth, route bindings, schedules and enablement](../azure-capture/config/aga-ratings-explorer-sgf-20260407t2105.json). Values marked `<supplied securely>` come from the existing Azure app setting/identity and must be provisioned through the future VM secret store. No secret values are in Git.

[Storage container/queue inventory](../azure-capture/storage-contract.json). App-specific setting names and binding `connection`/`queueName`/`schedule` fields identify its resources. Dynamically constructed blob prefixes remain in the source. Do not infer timer ownership from app names alone.

[Captured host configuration](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/host.json). [Captured dependency declaration](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/requirements.txt). [Original packaged operations notes](../../azure-baseline/aga-ratings-explorer-sgf-20260407t2105/README.md).

Build a reviewed source ZIP without executing code or contacting Azure:

```powershell
python scripts/package_baseline.py aga-ratings-explorer-sgf-20260407t2105 --output ../packages/aga-ratings-explorer-sgf-20260407t2105-source.zip
```

For a separately authorized future Azure deployment, install the declared dependencies using the recorded Python version or use the Azure remote build. The historical deployment interface is:

```powershell
az functionapp deployment source config-zip --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --resource-group aga-data-platform --name aga-ratings-explorer-sgf-20260407t2105 --src ../packages/aga-ratings-explorer-sgf-20260407t2105-source.zip --build-remote true
```

**Do not execute that deployment command during baseline or reconciliation work.** Source ZIPs omit installed dependencies and production data. Deployment scripts embedded in the package are preserved; existing repository scripts are under [scripts/](../../scripts/). This command records the interface and is not a deployment performed by this task.

## Routes, authentication and schedules

| Function | Trigger contract | Enabled |
|---|---|---|
| `BayRateMemberMerge` | type=httpTrigger; route=ratings-explorer/bayrate/member-merge; methods=['GET', 'POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateMemberMergePreview` | type=httpTrigger; route=ratings-explorer/bayrate/member-merge-preview; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateMetadataOptions` | type=httpTrigger; route=ratings-explorer/bayrate/metadata-options; methods=['GET', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateStagingCommit` | type=httpTrigger; route=ratings-explorer/bayrate/commit; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateStagingCommitPreview` | type=httpTrigger; route=ratings-explorer/bayrate/commit-preview; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateStagingPage` | type=httpTrigger; route=ratings-explorer/bayrate; methods=['GET']; authLevel=ANONYMOUS | True |
| `BayRateStagingPreview` | type=httpTrigger; route=ratings-explorer/bayrate/preview; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateStagingReplay` | type=httpTrigger; route=ratings-explorer/bayrate/replay; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateStagingReview` | type=httpTrigger; route=ratings-explorer/bayrate/review; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateStagingRun` | type=httpTrigger; route=ratings-explorer/bayrate/run; methods=['GET', 'POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `BayRateStagingWrite` | type=httpTrigger; route=ratings-explorer/bayrate/stage; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `RatingsExplorerAsset` | type=httpTrigger; route=ratings-explorer/assets/{*asset_path}; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerFilterOptions` | type=httpTrigger; route=ratings-explorer/filter-options; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerGameSgf` | type=httpTrigger; route=ratings-explorer/game-sgf; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerGameSgfViewer` | type=httpTrigger; route=ratings-explorer/game-sgf-viewer; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerMobilePage` | type=httpTrigger; route=ratings-explorer/mobile; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerNightlySnapshot` | type=timerTrigger; schedule=0 15 6 * * * | True |
| `RatingsExplorerPage` | type=httpTrigger; route=ratings-explorer; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerPendingSnapshotRefresh` | type=timerTrigger; schedule=0 */5 * * * * | True |
| `RatingsExplorerPlayer` | type=httpTrigger; route=ratings-explorer/player; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerPlayerContext` | type=httpTrigger; route=ratings-explorer/player-context; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerPlayerHistorySvg` | type=httpTrigger; route=ratings-explorer/player-history.svg; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerPlayers` | type=httpTrigger; route=ratings-explorer/players; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerPlayersStartup` | type=httpTrigger; route=ratings-explorer/players-startup; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerSgfImportApply` | type=httpTrigger; route=ratings-explorer/sgf-import/apply; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `RatingsExplorerSgfImportPage` | type=httpTrigger; route=ratings-explorer/sgf-import; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerSgfImportPreview` | type=httpTrigger; route=ratings-explorer/sgf-import/preview; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `RatingsExplorerSgfPopupTest` | type=httpTrigger; route=ratings-explorer/sgf-popup-test; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerSnapshotRefresh` | type=httpTrigger; route=ratings-explorer/snapshot-refresh; methods=['POST']; authLevel=FUNCTION | True |
| `RatingsExplorerSnapshotStatus` | type=httpTrigger; route=ratings-explorer/snapshot-status; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerSnapshotWarm` | type=httpTrigger; route=ratings-explorer/snapshot-warm; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerTournament` | type=httpTrigger; route=ratings-explorer/tournament; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerTournaments` | type=httpTrigger; route=ratings-explorer/tournaments; methods=['GET']; authLevel=ANONYMOUS | True |
| `RewardsAdminDebitPost` | type=httpTrigger; route=ratings-explorer/rewards/admin/debit; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `RewardsAdminDebitPreview` | type=httpTrigger; route=ratings-explorer/rewards/admin/debit-preview; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `RewardsAdminMe` | type=httpTrigger; route=ratings-explorer/rewards/admin/me; methods=['GET', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `RewardsAdminOptions` | type=httpTrigger; route=ratings-explorer/rewards/admin/options; methods=['GET', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `RewardsAdminReceiptDelete` | type=httpTrigger; route=ratings-explorer/rewards/admin/receipt-delete; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `RewardsAdminReceiptFile` | type=httpTrigger; route=ratings-explorer/rewards/admin/receipts/{receipt_id}; methods=['GET']; authLevel=ANONYMOUS | True |
| `RewardsAdminReceiptUpload` | type=httpTrigger; route=ratings-explorer/rewards/admin/receipt-upload; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `RewardsAdminRedemption` | type=httpTrigger; route=ratings-explorer/rewards/admin/redemption; methods=['GET', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `RewardsAdminRedemptionNotes` | type=httpTrigger; route=ratings-explorer/rewards/admin/redemption-notes; methods=['POST', 'OPTIONS']; authLevel=ANONYMOUS | True |
| `RewardsAdminReportPage` | type=httpTrigger; route=ratings-explorer/rewards/admin; methods=['GET']; authLevel=ANONYMOUS | True |
| `RewardsPublicBalances` | type=httpTrigger; route=ratings-explorer/rewards/balances; methods=['GET']; authLevel=ANONYMOUS | True |
| `RewardsPublicChapter` | type=httpTrigger; route=ratings-explorer/rewards/chapter; methods=['GET']; authLevel=ANONYMOUS | True |
| `RewardsPublicReportPage` | type=httpTrigger; route=ratings-explorer/rewards; methods=['GET']; authLevel=ANONYMOUS | True |

Trigger ANONYMOUS does not bypass handler/EasyAuth authorization. Preserve the checks in the entry point and imported auth modules. Azure settings may disable a trigger independently; consult both settings and binding metadata.

## Differences and verification

37 captured source/assets files. Static syntax, reachable imports, per-file hashes and ZIP CRC checks passed. No handler was invoked against production. [Per-file deployed/development/local comparison](../azure-capture/source-comparison.json) distinguishes the original GitHub development layout and preserved local work.

The normal development folder may lag or diverge from the deployment. Future VM work starts from this appâ€™s captured behavior; do not overwrite newer development work. Use a separate `vm-stack/` implementation when migration begins.
