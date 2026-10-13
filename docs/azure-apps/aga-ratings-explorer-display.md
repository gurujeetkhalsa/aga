# aga-ratings-explorer-display

Public Ratings Explorer, SVG/SGF viewer and snapshot timers/queue workers. Snapshot blobs are runtime data; WGo scripts/artwork are committed assets.

- Repository: [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga). Resource group: `aga-data-platform`.
- Exact captured source: [azure-baseline/aga-ratings-explorer-display/](../../azure-baseline/aga-ratings-explorer-display/). Development: [ratings-explorer-display-app/](../../ratings-explorer-display-app/).
- Host: `aga-ratings-explorer-display.azurewebsites.net`; state `Running`; runtime `{"name": "python", "version": "3.13"}`.
- Capture: `2026-10-13T17:16:37.067894+00:00`; source status: **recovered active deployment**.
- Active package: `https://agafunc0522.blob.core.windows.net/app-package-agaratingsexplorerdisplay-8238950/released-package.zip`. SHA-256 `16abc67950b046138b650a68ea04b8e52830e0beebbee85aa9649017cadd635c`.
- Release metadata and individual file hashes: [manifest](../azure-capture/manifests/aga-ratings-explorer-display.json). Runtime `function_app.py` byte hash matches captured source.

## Shared and application modules

| Captured module | Maintained development copy |
|---|---|
| [ratings_explorer_display_support.py](../../azure-baseline/aga-ratings-explorer-display/ratings_explorer_display_support.py) | [ratings-explorer-display-app/ratings_explorer_display_support.py](../../ratings-explorer-display-app/ratings_explorer_display_support.py) |
| [wgo_embedded_assets.py](../../azure-baseline/aga-ratings-explorer-display/wgo_embedded_assets.py) | [ratings-explorer-display-app/wgo_embedded_assets.py](../../ratings-explorer-display-app/wgo_embedded_assets.py) |

These are package-local copies, not links to the repository root. Their versions may differ between apps. Never deduplicate merely by filename. Third-party imports are declared in [requirements.txt](../../azure-baseline/aga-ratings-explorer-display/requirements.txt); the [static verification report](../azure-capture/verification.json) records the reachable module graph.

## Database

| SQL definition | Relationship | Classification |
|---|---|---|
| [dbrepl_meta.generations](../../sql/azure-baseline/dbrepl_meta/generations.sql) | catalog dependency or attached trigger | tooling/replication |
| [integration.journal_article_member_match](../../sql/azure-baseline/integration/journal_article_member_match.sql) | literal reference in packaged source | application |
| [integration.journal_review_member_match](../../sql/azure-baseline/integration/journal_review_member_match.sql) | literal reference in packaged source | application |
| [membership.chapters](../../sql/azure-baseline/membership/chapters.sql) | literal reference in packaged source | application |
| [membership.members](../../sql/azure-baseline/membership/members.sql) | literal reference in packaged source | application |
| [ratings.dbrepl_dirty_1234103437](../../sql/azure-baseline/ratings/dbrepl_dirty_1234103437.sql) | catalog dependency or attached trigger | tooling/replication |
| [ratings.dbrepl_dirty_1250103494](../../sql/azure-baseline/ratings/dbrepl_dirty_1250103494.sql) | catalog dependency or attached trigger | tooling/replication |
| [ratings.dbrepl_dirty_1266103551](../../sql/azure-baseline/ratings/dbrepl_dirty_1266103551.sql) | catalog dependency or attached trigger | tooling/replication |
| [ratings.games](../../sql/azure-baseline/ratings/games.sql) | literal reference in packaged source | application |
| [ratings.ratings](../../sql/azure-baseline/ratings/ratings.sql) | literal reference in packaged source | application |
| [ratings.tournaments](../../sql/azure-baseline/ratings/tournaments.sql) | literal reference in packaged source | application |

Every SQL file has a metadata companion. [Database permissions](../../sql/azure-baseline/_database/permissions.sql), [principals](../../sql/azure-baseline/_database/principals.json), [schemas](../../sql/azure-baseline/_database/schemas.sql), [full-text definitions](../../sql/azure-baseline/_database/fulltext.sql) and [all dependency edges](../../sql/azure-baseline/_database/dependencies.json) are part of the contract.

**Dependency analysis limit:** dynamic SQL targets and external database definitions are unknown until each constructed SQL path is checked. Literal matching plus catalog closure is evidence, not proof that all runtime branches execute. Triggers classified as replication support must be evaluated separately from application behavior.

## Configuration and deployment

[Sanitized live settings, EasyAuth, route bindings, schedules and enablement](../azure-capture/config/aga-ratings-explorer-display.json). Values marked `<supplied securely>` come from the existing Azure app setting/identity and must be provisioned through the future VM secret store. No secret values are in Git.

[Storage container/queue inventory](../azure-capture/storage-contract.json). App-specific setting names and binding `connection`/`queueName`/`schedule` fields identify its resources. Dynamically constructed blob prefixes remain in the source. Do not infer timer ownership from app names alone.

[Captured host configuration](../../azure-baseline/aga-ratings-explorer-display/host.json). [Captured dependency declaration](../../azure-baseline/aga-ratings-explorer-display/requirements.txt). [Original packaged operations notes](../../azure-baseline/aga-ratings-explorer-display/README.md).

Build a reviewed source ZIP without executing code or contacting Azure:

```powershell
python scripts/package_baseline.py aga-ratings-explorer-display --output ../packages/aga-ratings-explorer-display-source.zip
```

For a separately authorized future Azure deployment, install the declared dependencies using the recorded Python version or use the Azure remote build. The historical deployment interface is:

```powershell
az functionapp deployment source config-zip --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --resource-group aga-data-platform --name aga-ratings-explorer-display --src ../packages/aga-ratings-explorer-display-source.zip --build-remote true
```

**Do not execute that deployment command during baseline or reconciliation work.** Source ZIPs omit installed dependencies and production data. Deployment scripts embedded in the package are preserved; existing repository scripts are under [scripts/](../../scripts/). This command records the interface and is not a deployment performed by this task.

## Routes, authentication and schedules

| Function | Trigger contract | Enabled |
|---|---|---|
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
| `RatingsExplorerSnapshotRefresh` | type=httpTrigger; route=ratings-explorer/snapshot-refresh; methods=['POST']; authLevel=FUNCTION | True |
| `RatingsExplorerSnapshotStatus` | type=httpTrigger; route=ratings-explorer/snapshot-status; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerSnapshotWarm` | type=httpTrigger; route=ratings-explorer/snapshot-warm; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerTournament` | type=httpTrigger; route=ratings-explorer/tournament; methods=['GET']; authLevel=ANONYMOUS | True |
| `RatingsExplorerTournaments` | type=httpTrigger; route=ratings-explorer/tournaments; methods=['GET']; authLevel=ANONYMOUS | True |

Trigger ANONYMOUS does not bypass handler/EasyAuth authorization. Preserve the checks in the entry point and imported auth modules. Azure settings may disable a trigger independently; consult both settings and binding metadata.

## Differences and verification

14 captured source/assets files. Static syntax, reachable imports, per-file hashes and ZIP CRC checks passed. No handler was invoked against production. [Per-file deployed/development/local comparison](../azure-capture/source-comparison.json) distinguishes the original GitHub development layout and preserved local work.

The normal development folder may lag or diverge from the deployment. Future VM work starts from this appâ€™s captured behavior; do not overwrite newer development work. Use a separate `vm-stack/` implementation when migration begins.
