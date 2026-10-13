<!-- Copyright 2026, American Go Association, All rights reserved -->

# News Article Archive Viewer App

Read-only Azure Function viewer for the ClubExpress news article archive.

Routes:

- `GET /api/news-archive`
- `GET /api/news-archive/articles?offset=0&anchorSnapshotId=...&q=...&month=7&year=2026`
- `GET /api/news-archive?snapshotId=...`
- `GET /api/news-archive/home-cards?limit=70&viewerUrl=...`
- `GET /api/news-archive/article/{snapshot_id}`
- `GET /api/news-archive/asset/{asset_id}`

Timer:

- `NewsArchivePublicSyncTimer` scans the public ClubExpress news list and archives new or changed public articles.
- `NewsArchiveExternalSyncTimer` builds daily external Go news digests from configured public sources and archives new or changed digest articles.
- `NewsArchivePlayerContextSyncTimer` refreshes Ratings Explorer player/article context links from archived AGA news snapshots.

Required settings:

- `SQL_CONNECTION_STRING`
- `AzureWebJobsStorage` or `NEWS_ARCHIVE_STORAGE_CONNECTION_STRING`
- `NEWS_ARCHIVE_CONTAINER`, defaults to `clubexpress-news-archive`
- `NEWS_ARCHIVE_VIEWER_PAGE_SIZE`, defaults to `7`
- `NEWS_ARCHIVE_PUBLIC_SYNC_ENABLED`, defaults to `true`
- `NEWS_ARCHIVE_PUBLIC_SYNC_SCHEDULE`, defaults to `0 20 * * * *`
- `NEWS_ARCHIVE_PUBLIC_SYNC_MAX_CANDIDATES`, defaults to `10`
- `NEWS_ARCHIVE_EXTERNAL_SYNC_ENABLED`, defaults to `true`
- `NEWS_ARCHIVE_EXTERNAL_SYNC_SCHEDULE`, defaults to `0 35 10 * * *`
- `NEWS_ARCHIVE_EXTERNAL_SOURCE_IDS`, optional comma-separated override for the default source list
- `NEWS_ARCHIVE_EXTERNAL_ITEM_LIMIT_PER_SOURCE`, defaults to `20`
- `NEWS_ARCHIVE_EXTERNAL_DIGEST_LOOKBACK_DAYS`, defaults to `3`
- `NEWS_ARCHIVE_EXTERNAL_SOURCE_ITEM_LEDGER_ENABLED`, defaults to `true`; uses the SQL source item ledger when `clubexpress.external_news_source_items` exists
- `NEWS_ARCHIVE_EXTERNAL_TRANSLATE_WITH_AZURE`, defaults to `auto`; when `AZURE_TRANSLATOR_KEY` is configured, non-English helper fields are translated
- `NEWS_ARCHIVE_EXTERNAL_TRANSLATION_CACHE_ENABLED`, defaults to `true`; stores non-English helper translations in SQL by source article and content hash
- `NEWS_ARCHIVE_EXTERNAL_TRANSLATION_RATE_LIMIT_CHARS_PER_MINUTE`, defaults to `20000`; set to `0` to disable local pacing
- `NEWS_ARCHIVE_EXTERNAL_TRANSLATION_RETRY_SECONDS`, defaults to `65`
- `NEWS_ARCHIVE_EXTERNAL_TRANSLATION_RETRY_COUNT`, defaults to `1`
- `NEWS_ARCHIVE_PLAYER_CONTEXT_SYNC_ENABLED`, defaults to `true`
- `NEWS_ARCHIVE_PLAYER_CONTEXT_SYNC_SCHEDULE`, defaults to `0 30 * * * *`
- `NEWS_ARCHIVE_PLAYER_CONTEXT_VIEWER_URL`, defaults to `https://aga-news-archive-viewer.azurewebsites.net/api/news-archive`
- `NEWS_ARCHIVE_PLAYER_CONTEXT_EXCLUDED_SOURCE_MODES`, defaults to `external-go-news-digest`
- `NEWS_ARCHIVE_HOME_CARD_COUNT`, defaults to `70`, capped at `100`
- `NEWS_ARCHIVE_HOME_CARDS_VIEWER_URL`, optional base URL for homepage card `Read More` links

Keyword terms in `q` are ANDed together across article headline, summary, and stored search text. By default the article list is public-safe: `Active`, visible to everyone, and active date no later than the current UTC date. Set `NEWS_ARCHIVE_PUBLIC_ONLY=false` only for a private review deployment. The article list supports `offset` paging; `anchorSnapshotId` starts the returned list at a selected article when opening from homepage cards. The viewer displays the selected article inline immediately below its list item.

Opened articles include title-bar actions for print/save-as-PDF and sharing. Share uses the native Web Share API when the browser and iframe permissions allow it, then falls back to copy-link, Facebook, LinkedIn, Bluesky, and X links. Viewer URLs containing `snapshotId` render article-specific Open Graph and social-card metadata. Every ClubExpress iframe that embeds this app must include the `allow="web-share"` attribute; for example:

```html
<iframe src="https://aga-news-archive-viewer.azurewebsites.net/api/news-archive" allow="web-share"></iframe>
```

The scheduled sync is public-visible only. It does not use ClubExpress admin credentials and will not see hidden or admin-only articles. For scanned articles, it compares the current public article hashes to the latest archived snapshot and writes a new snapshot only when content changed.

External digest articles use the same SQL and blob archive tables as ClubExpress articles, with a reserved synthetic `Club_ID` and stable digest-date item IDs. They appear in the viewer without an opt-in query parameter. Apply `sql/external_news_source_item_ledger.sql` before enabling the sync. Each source item is assigned permanently to the America/New_York calendar date on which the monitor first discovers it. A run builds at most one digest for that Eastern discovery date and creates no digest when the ledger has no items first discovered on that date. Source publication timestamps are converted to Eastern time for display but do not determine digest membership.

Ratings Explorer article context is sourced from `integration.news_archive_article_member_match`, which is refreshed from archived AGA news snapshots and links to the archive viewer by `snapshotId`. External digest articles are excluded by default.

The homepage card endpoint is intended for ClubExpress Advanced Editor iframe embedding. It renders as a one-row carousel by default, advances one card per arrow click, and can render up to 100 recent cards. Pass `layout=grid` only for comparison testing. Pass a ClubExpress custom news-page URL as `viewerUrl` after creating the replacement news page so card links open the selected archive article in the top window. The cards page records a short-lived same-origin pending snapshot before navigation so the viewer iframe can select the clicked article even when ClubExpress does not forward `snapshotId` into the iframe URL.
