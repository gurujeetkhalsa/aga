# aga-news-archive-viewer

SQL and blob-backed archived articles, assets/member matches, external news synchronization and public display. Article bodies/mailbox content are production data, not source assets.

- Repository: [gurujeetkhalsa/aga](https://github.com/gurujeetkhalsa/aga). Resource group: `aga-data-platform`.
- Exact captured source: [azure-baseline/aga-news-archive-viewer/](../../azure-baseline/aga-news-archive-viewer/). Development: [news-article-archive-viewer-app/](../../news-article-archive-viewer-app/).
- Host: `aga-news-archive-viewer.azurewebsites.net`; state `Running`; runtime `{"name": "python", "version": "3.11"}`.
- Capture: `2026-10-13T17:15:43.951873+00:00`; source status: **recovered active deployment**.
- Active package: `https://agafunc0522.blob.core.windows.net/app-package-aganewsarchiveviewer-5060998/released-package.zip`. SHA-256 `c264479298719b9996356a36166886a66255702633e0e66d2cbd1dd48b6250e5`.
- Release metadata and individual file hashes: [manifest](../azure-capture/manifests/aga-news-archive-viewer.json). Runtime `function_app.py` byte hash matches captured source.

## Shared and application modules

| Captured module | Maintained development copy |
|---|---|
| [external_go_news_sources.py](../../azure-baseline/aga-news-archive-viewer/external_go_news_sources.py) | [news-article-archive-viewer-app/external_go_news_sources.py](../../news-article-archive-viewer-app/external_go_news_sources.py) |
| [news_archive_external_sync.py](../../azure-baseline/aga-news-archive-viewer/news_archive_external_sync.py) | [news-article-archive-viewer-app/news_archive_external_sync.py](../../news-article-archive-viewer-app/news_archive_external_sync.py) |
| [news_archive_player_context_sync.py](../../azure-baseline/aga-news-archive-viewer/news_archive_player_context_sync.py) | [news-article-archive-viewer-app/news_archive_player_context_sync.py](../../news-article-archive-viewer-app/news_archive_player_context_sync.py) |
| [news_archive_public_sync.py](../../azure-baseline/aga-news-archive-viewer/news_archive_public_sync.py) | [news-article-archive-viewer-app/news_archive_public_sync.py](../../news-article-archive-viewer-app/news_archive_public_sync.py) |

These are package-local copies, not links to the repository root. Their versions may differ between apps. Never deduplicate merely by filename. Third-party imports are declared in [requirements.txt](../../azure-baseline/aga-news-archive-viewer/requirements.txt); the [static verification report](../azure-capture/verification.json) records the reachable module graph.

## Database

| SQL definition | Relationship | Classification |
|---|---|---|
| [clubexpress.external_news_source_items](../../sql/azure-baseline/clubexpress/external_news_source_items.sql) | literal reference in packaged source | application |
| [clubexpress.external_news_translation_cache](../../sql/azure-baseline/clubexpress/external_news_translation_cache.sql) | literal reference in packaged source | application |
| [clubexpress.news_archive_runs](../../sql/azure-baseline/clubexpress/news_archive_runs.sql) | literal reference in packaged source | application |
| [clubexpress.news_article_assets](../../sql/azure-baseline/clubexpress/news_article_assets.sql) | literal reference in packaged source | application |
| [clubexpress.news_article_links](../../sql/azure-baseline/clubexpress/news_article_links.sql) | literal reference in packaged source | application |
| [clubexpress.news_article_snapshots](../../sql/azure-baseline/clubexpress/news_article_snapshots.sql) | literal reference in packaged source | application |
| [clubexpress.news_articles](../../sql/azure-baseline/clubexpress/news_articles.sql) | literal reference in packaged source | application |
| [dbrepl_meta.generations](../../sql/azure-baseline/dbrepl_meta/generations.sql) | catalog dependency or attached trigger | tooling/replication |
| [integration.news_archive_article_member_match](../../sql/azure-baseline/integration/news_archive_article_member_match.sql) | literal reference in packaged source | application |
| [membership.members](../../sql/azure-baseline/membership/members.sql) | literal reference in packaged source | application |
| [ratings.dbrepl_dirty_1250103494](../../sql/azure-baseline/ratings/dbrepl_dirty_1250103494.sql) | catalog dependency or attached trigger | tooling/replication |
| [ratings.ratings](../../sql/azure-baseline/ratings/ratings.sql) | literal reference in packaged source | application |

Every SQL file has a metadata companion. [Database permissions](../../sql/azure-baseline/_database/permissions.sql), [principals](../../sql/azure-baseline/_database/principals.json), [schemas](../../sql/azure-baseline/_database/schemas.sql), [full-text definitions](../../sql/azure-baseline/_database/fulltext.sql) and [all dependency edges](../../sql/azure-baseline/_database/dependencies.json) are part of the contract.

**Dependency analysis limit:** dynamic SQL targets and external database definitions are unknown until each constructed SQL path is checked. Literal matching plus catalog closure is evidence, not proof that all runtime branches execute. Triggers classified as replication support must be evaluated separately from application behavior.

## Configuration and deployment

[Sanitized live settings, EasyAuth, route bindings, schedules and enablement](../azure-capture/config/aga-news-archive-viewer.json). Values marked `<supplied securely>` come from the existing Azure app setting/identity and must be provisioned through the future VM secret store. No secret values are in Git.

[Storage container/queue inventory](../azure-capture/storage-contract.json). App-specific setting names and binding `connection`/`queueName`/`schedule` fields identify its resources. Dynamically constructed blob prefixes remain in the source. Do not infer timer ownership from app names alone.

[Captured host configuration](../../azure-baseline/aga-news-archive-viewer/host.json). [Captured dependency declaration](../../azure-baseline/aga-news-archive-viewer/requirements.txt). [Original packaged operations notes](../../azure-baseline/aga-news-archive-viewer/README.md).

Build a reviewed source ZIP without executing code or contacting Azure:

```powershell
python scripts/package_baseline.py aga-news-archive-viewer --output ../packages/aga-news-archive-viewer-source.zip
```

For a separately authorized future Azure deployment, install the declared dependencies using the recorded Python version or use the Azure remote build. The historical deployment interface is:

```powershell
az functionapp deployment source config-zip --subscription 4dcce4a0-c902-4012-91fe-6dbbddfd6fb7 --resource-group aga-data-platform --name aga-news-archive-viewer --src ../packages/aga-news-archive-viewer-source.zip --build-remote true
```

**Do not execute that deployment command during baseline or reconciliation work.** Source ZIPs omit installed dependencies and production data. Deployment scripts embedded in the package are preserved; existing repository scripts are under [scripts/](../../scripts/). This command records the interface and is not a deployment performed by this task.

## Routes, authentication and schedules

| Function | Trigger contract | Enabled |
|---|---|---|
| `NewsArchiveDefaultCardImage` | type=httpTrigger; route=news-archive/static/default-card-image; methods=['GET']; authLevel=ANONYMOUS | True |
| `NewsArchiveDigestCardImage` | type=httpTrigger; route=news-archive/static/digest-card-image; methods=['GET']; authLevel=ANONYMOUS | True |
| `NewsArchiveExternalSyncTimer` | type=timerTrigger; schedule=0 35 10 * * * | True |
| `NewsArchiveHomeCards` | type=httpTrigger; route=news-archive/home-cards; methods=['GET']; authLevel=ANONYMOUS | True |
| `NewsArchivePlayerContextSyncTimer` | type=timerTrigger; schedule=0 30 * * * * | True |
| `NewsArchivePublicSyncTimer` | type=timerTrigger; schedule=0 20 * * * * | True |
| `NewsArchiveViewerArticle` | type=httpTrigger; route=news-archive/article/{snapshot_id}; methods=['GET']; authLevel=ANONYMOUS | True |
| `NewsArchiveViewerArticleEmailHtml` | type=httpTrigger; route=news-archive/article/{snapshot_id}/email-html; methods=['GET']; authLevel=ANONYMOUS | True |
| `NewsArchiveViewerArticles` | type=httpTrigger; route=news-archive/articles; methods=['GET']; authLevel=ANONYMOUS | True |
| `NewsArchiveViewerAsset` | type=httpTrigger; route=news-archive/asset/{asset_id}; methods=['GET']; authLevel=ANONYMOUS | True |
| `NewsArchiveViewerHealth` | type=httpTrigger; route=news-archive/health; methods=['GET']; authLevel=ANONYMOUS | True |
| `NewsArchiveViewerPage` | type=httpTrigger; route=news-archive; methods=['GET']; authLevel=ANONYMOUS | True |

Trigger ANONYMOUS does not bypass handler/EasyAuth authorization. Preserve the checks in the entry point and imported auth modules. Azure settings may disable a trigger independently; consult both settings and binding metadata.

## Differences and verification

30 captured source/assets files. Static syntax, reachable imports, per-file hashes and ZIP CRC checks passed. No handler was invoked against production. [Per-file deployed/development/local comparison](../azure-capture/source-comparison.json) distinguishes the original GitHub development layout and preserved local work.

The normal development folder may lag or diverge from the deployment. Future VM work starts from this appâ€™s captured behavior; do not overwrite newer development work. Use a separate `vm-stack/` implementation when migration begins.
