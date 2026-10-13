# Copyright 2026, American Go Association, All rights reserved

"""Scheduled external Go news digest synchronization."""

from __future__ import annotations

import html
import json
import logging
import os
import time
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from external_go_news_sources import (
    AIGP_NEWS_URL,
    AZURE_TRANSLATOR_MODEL_LABEL,
    CWA_HOME_URL,
    EGF_HOME_URL,
    FEDIBERGO_HOME_URL,
    FEED_SOURCE_CONFIGS,
    HANGUKKIWON_REPORT_URL,
    KBADUK_PRESS_URL,
    NIHONKIIN_NEWS_URL,
    PGETC_NEWS_URL,
    azure_translate_texts,
    enrichment_record_for_item,
    feed_source_metadata,
    fetch_cwa_news_records,
    fetch_text,
    normalize_text,
    parse_aigp_news_listing,
    parse_cwa_news_records,
    parse_egf_news_listing,
    parse_fedibergo_news_listing,
    parse_generic_feed,
    parse_hangukkiwon_report_listing,
    parse_kbaduk_press_listing,
    parse_nihonkiin_news_listing,
    parse_pgetc_news_listing,
    translated_excerpt,
    utc_now_iso,
)
from news_archive_public_sync import (
    clean_blob_path,
    get_latest_snapshot_state,
    insert_news_links,
    insert_news_snapshot,
    json_dumps,
    normalize_hash,
    normalize_whitespace,
    sha256_text,
    text_from_markup,
    update_archive_run,
    upload_text_blob,
)


EXTERNAL_NEWS_CLUB_ID = 0
SOURCE_MODE = "external-go-news-digest"
DEFAULT_EXTERNAL_SOURCE_IDS = (
    "egf",
    "nihonkiin",
    "kbaduk",
    "hangukkiwon",
    "aigp",
    "cwa",
    "kansaikiin",
    "britgo",
    "igf",
    "singapore",
    "taiwanchi",
    "australiango",
    "thaigo",
    "nagf",
    "dgob",
    "pgetc",
    "fedibergo",
)
MONTH_NAMES = (
    "",
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)
EASTERN_TIME_ZONE = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class ExternalNewsSyncConfig:
    """Configuration for scheduled external Go news digests."""

    source_ids: tuple[str, ...] = DEFAULT_EXTERNAL_SOURCE_IDS
    item_limit_per_source: int = 20
    digest_lookback_days: int = 3
    blob_root: str = "scheduled/external-go-news"
    source_description: str = "Scheduled external Go news source digest."
    translate_with_azure: bool = False
    azure_translator_key_env: str = "AZURE_TRANSLATOR_KEY"
    azure_translator_region_env: str = "AZURE_TRANSLATOR_REGION"
    enrichment_detail_chars: int = 1800
    translation_cache_enabled: bool = True
    translation_rate_limit_chars_per_minute: int = 20000
    translation_retry_seconds: float = 65.0
    translation_retry_count: int = 1
    translation_target_language: str = "en"
    source_item_ledger_enabled: bool = True


def sync_external_go_news_archive(adapter: Any, container_client: Any, config: ExternalNewsSyncConfig) -> dict[str, Any]:
    """Fetch external source items, build daily digests, and archive changed digests."""
    run_started_at = datetime.now(timezone.utc)
    discovery_date = eastern_date(run_started_at)
    run_key = f"scheduled-external-go-news-{run_started_at.strftime('%Y%m%dT%H%M%SZ')}"
    fetched_sources: list[dict[str, Any]] = []
    failed_sources: list[dict[str, Any]] = []
    all_items: list[dict[str, Any]] = []
    ledger_stats = source_item_ledger_status(adapter, config)
    rate_limiter = TranslationRateLimiter(config.translation_rate_limit_chars_per_minute)

    for source_id in config.source_ids:
        try:
            payload = fetch_external_source_payload(source_id, config, translate=False)
            for item in payload["items"]:
                item["digest_date"] = discovery_date.isoformat()
                item["discovered_at"] = run_started_at.isoformat()
            maybe_enrich_payload_with_azure(payload, config, adapter=adapter, rate_limiter=rate_limiter)
            fetched_sources.append(
                {
                    "source_id": source_id,
                    "source_name": payload["source"]["name"],
                    "item_count": len(payload["items"]),
                    "translation_status": payload.get("translation_status") or "not_requested",
                    "translation_error": payload.get("translation_error") or "",
                    "translation_cache_stats": payload.get("translation_cache_stats") or {},
                }
            )
            all_items.extend(payload["items"])
        except Exception as exc:  # pragma: no cover - production hardening.
            logging.exception("Failed to fetch external Go news source %s", source_id)
            failed_sources.append({"source_id": source_id, "error": str(exc)})

    digest_items: list[dict[str, Any]] = []
    ledger_stats["fetched_item_count"] = len(all_items)
    if ledger_stats.get("available"):
        try:
            ledger_stats.update(upsert_external_source_items(adapter, all_items, run_key))
            digest_items = load_external_source_items_for_digest_date(adapter, discovery_date, config)
            ledger_stats["loaded_item_count"] = len(digest_items)
        except Exception as exc:  # pragma: no cover - defensive production fallback.
            logging.exception("External source item ledger failed; no digest can be built safely.")
            ledger_stats["available"] = False
            ledger_stats["error"] = str(exc)
            failed_sources.append(
                {
                    "source_id": "external-digest-ledger",
                    "stage": "ledger",
                    "error": str(exc),
                }
            )
    else:
        error = str(ledger_stats.get("error") or "External source item ledger is required for discovery-based digests.")
        ledger_stats["error"] = error
        failed_sources.append(
            {
                "source_id": "external-digest-ledger",
                "stage": "ledger",
                "error": error,
            }
        )

    digest_articles = build_digest_articles(digest_items, config, digest_date=discovery_date)
    latest_snapshots = get_latest_snapshot_state(
        adapter,
        EXTERNAL_NEWS_CLUB_ID,
        [int(article["item_id"]) for article in digest_articles],
    )
    changed_articles: list[dict[str, Any]] = []
    unchanged_articles: list[dict[str, Any]] = []

    for article in digest_articles:
        hashes = compute_digest_hashes(article)
        article["hashes"] = hashes
        latest = latest_snapshots.get(int(article["item_id"]))
        change_reason = classify_digest_change(latest, hashes)
        if not change_reason:
            unchanged_articles.append(
                {
                    "item_id": article["item_id"],
                    "headline": article["headline"],
                    "snapshot_id": latest.get("News_Article_Snapshot_ID") if latest else None,
                }
            )
            continue
        changed_articles.append(
            {
                "article": article,
                "latest": latest,
                "change_reason": change_reason,
            }
        )

    if not changed_articles and not failed_sources:
        return {
            "created_run": False,
            "source_count": len(config.source_ids),
            "fetched_source_count": len(fetched_sources),
            "failed_source_count": 0,
            "candidate_digest_count": len(digest_articles),
            "changed_digest_count": 0,
            "archived_digest_count": 0,
            "unchanged_digest_count": len(unchanged_articles),
            "source_item_ledger": ledger_stats,
        }

    blob_prefix = clean_blob_path(f"{config.blob_root}/{run_key}")
    run_id = create_external_archive_run(
        adapter,
        run_key,
        config,
        container_client,
        blob_prefix,
        fetched_sources,
        failed_sources,
        digest_articles,
        changed_articles,
        unchanged_articles,
        ledger_stats,
    )
    archived_articles: list[dict[str, Any]] = []

    try:
        for item in changed_articles:
            article = item["article"]
            try:
                uploaded = upload_digest_article(container_client, blob_prefix, article)
                news_article_id = upsert_external_news_article(adapter, run_id, article)
                snapshot_id = insert_news_snapshot(adapter, run_id, run_key, news_article_id, article, uploaded)
                insert_news_links(adapter, snapshot_id, news_article_id, article["links"])
                archived_articles.append(
                    {
                        "item_id": article["item_id"],
                        "headline": article["headline"],
                        "snapshot_id": snapshot_id,
                        "change_reason": item["change_reason"],
                        "previous_snapshot_id": item["latest"].get("News_Article_Snapshot_ID") if item["latest"] else None,
                    }
                )
            except Exception as exc:  # pragma: no cover - production hardening.
                logging.exception("Failed to archive external digest %s", article.get("item_id"))
                failed_sources.append(
                    {
                        "source_id": "external-digest",
                        "item_id": article.get("item_id"),
                        "headline": article.get("headline"),
                        "stage": "archive",
                        "error": str(exc),
                    }
                )

        status = "succeeded" if not failed_sources else ("partial" if archived_articles else "failed")
        summary = {
            "source_count": len(config.source_ids),
            "fetched_sources": fetched_sources,
            "failed_sources": failed_sources,
            "candidate_digest_count": len(digest_articles),
            "changed_digest_count": len(changed_articles),
            "unchanged_digests": unchanged_articles,
            "archived_digests": archived_articles,
            "source_item_ledger": ledger_stats,
        }
        update_archive_run(
            adapter,
            run_id,
            status=status,
            article_count=len(archived_articles),
            asset_count=0,
            summary=summary,
            error_message=None if status != "failed" else json.dumps(failed_sources, ensure_ascii=False),
        )
        return {
            "created_run": True,
            "run_id": run_id,
            "run_key": run_key,
            "status": status,
            "source_count": len(config.source_ids),
            "fetched_source_count": len(fetched_sources),
            "failed_source_count": len(failed_sources),
            "candidate_digest_count": len(digest_articles),
            "changed_digest_count": len(changed_articles),
            "archived_digest_count": len(archived_articles),
            "unchanged_digest_count": len(unchanged_articles),
            "blob_prefix": blob_prefix,
            "source_item_ledger": ledger_stats,
        }
    except Exception as exc:
        update_archive_run(
            adapter,
            run_id,
            status="failed",
            article_count=len(archived_articles),
            asset_count=0,
            summary={
                "fetched_sources": fetched_sources,
                "failed_sources": failed_sources,
                "archived_digests": archived_articles,
                "unchanged_digests": unchanged_articles,
                "source_item_ledger": ledger_stats,
            },
            error_message=str(exc),
        )
        raise


def fetch_external_source_payload(
    source_id: str,
    config: ExternalNewsSyncConfig,
    *,
    translate: bool = True,
) -> dict[str, Any]:
    """Fetch and parse one configured external source."""
    fetched_at = utc_now_iso()
    if source_id == "egf":
        source = {
            "id": "egf",
            "name": "European Go Federation",
            "home_url": EGF_HOME_URL,
            "language": "en",
        }
        items = parse_egf_news_listing(fetch_text(EGF_HOME_URL), fetched_at=fetched_at, limit=config.item_limit_per_source)
    elif source_id == "nihonkiin":
        source = {
            "id": "nihonkiin",
            "name": "Nihon Ki-in",
            "home_url": "https://www.nihonkiin.or.jp/",
            "language": "ja",
        }
        items = parse_nihonkiin_news_listing(
            fetch_text(NIHONKIIN_NEWS_URL),
            fetched_at=fetched_at,
            limit=config.item_limit_per_source,
        )
    elif source_id == "kbaduk":
        source = {
            "id": "kbaduk",
            "name": "Korea Baduk Federation",
            "home_url": "https://www.kbaduk.or.kr/",
            "language": "ko",
        }
        items = parse_kbaduk_press_listing(fetch_text(KBADUK_PRESS_URL), fetched_at=fetched_at, limit=config.item_limit_per_source)
    elif source_id == "hangukkiwon":
        source = {
            "id": "hangukkiwon",
            "name": "Hanguk Kiwon",
            "home_url": "https://www.baduk.or.kr/",
            "language": "ko",
        }
        items = parse_hangukkiwon_report_listing(
            fetch_text(HANGUKKIWON_REPORT_URL),
            fetched_at=fetched_at,
            limit=config.item_limit_per_source,
        )
    elif source_id == "aigp":
        source = {
            "id": "aigp",
            "name": "Association of Indian Go Players",
            "home_url": "https://aigp.org.in/",
            "language": "en",
        }
        items = parse_aigp_news_listing(fetch_text(AIGP_NEWS_URL), fetched_at=fetched_at, limit=config.item_limit_per_source)
    elif source_id == "pgetc":
        source = {
            "id": "pgetc",
            "name": "Pandanet European Team Go Championship",
            "home_url": "https://pandanet-igs.com/communities/euroteamchamps",
            "language": "en",
        }
        items = parse_pgetc_news_listing(fetch_text(PGETC_NEWS_URL), fetched_at=fetched_at, limit=config.item_limit_per_source)
    elif source_id == "fedibergo":
        source = {
            "id": "fedibergo",
            "name": "Federacion Iberoamericana de Go",
            "home_url": FEDIBERGO_HOME_URL,
            "language": "es",
        }
        items = parse_fedibergo_news_listing(
            fetch_text(FEDIBERGO_HOME_URL),
            fetched_at=fetched_at,
            limit=config.item_limit_per_source,
        )
    elif source_id == "cwa":
        source = {
            "id": "cwa",
            "name": "Chinese Weiqi Association",
            "home_url": CWA_HOME_URL,
            "language": "zh-Hans",
        }
        records = fetch_cwa_news_records(limit=config.item_limit_per_source)
        items = parse_cwa_news_records(records, fetched_at=fetched_at, limit=config.item_limit_per_source)
    elif source_id in FEED_SOURCE_CONFIGS:
        feed_config = FEED_SOURCE_CONFIGS[source_id]
        source = feed_source_metadata(feed_config)
        items = parse_generic_feed(
            feed_config,
            fetch_text(str(feed_config["feed_url"])),
            fetched_at=fetched_at,
            limit=config.item_limit_per_source,
        )
    else:
        raise ValueError(f"Unsupported external news source: {source_id}")

    payload = {
        "source": source,
        "fetched_item_count": len(items),
        "items": [asdict(item) for item in items],
    }
    if translate:
        maybe_enrich_payload_with_azure(payload, config)
    return payload


class TranslationRateLimiter:
    """Simple per-sync character budget for Azure Translator calls."""

    def __init__(self, chars_per_minute: int) -> None:
        self.chars_per_minute = max(0, int(chars_per_minute or 0))
        self.window_started_at = time.monotonic()
        self.window_chars = 0

    def wait_for(self, char_count: int) -> None:
        """Pause when adding a request would exceed the configured minute budget."""
        if self.chars_per_minute <= 0 or char_count <= 0:
            return
        now = time.monotonic()
        elapsed = now - self.window_started_at
        if elapsed >= 60.0:
            self.window_started_at = now
            self.window_chars = 0
            elapsed = 0.0
        if self.window_chars and self.window_chars + char_count > self.chars_per_minute:
            sleep_seconds = max(0.0, 60.0 - elapsed)
            if sleep_seconds:
                logging.info(
                    "Pausing Azure Translator %.1f seconds to stay under %s characters per minute.",
                    sleep_seconds,
                    self.chars_per_minute,
                )
                time.sleep(sleep_seconds)
            self.window_started_at = time.monotonic()
            self.window_chars = 0
        self.window_chars += char_count


def maybe_enrich_payload_with_azure(
    payload: dict[str, Any],
    config: ExternalNewsSyncConfig,
    *,
    adapter: Any | None = None,
    rate_limiter: TranslationRateLimiter | None = None,
) -> None:
    """Translate non-English helper fields when Azure Translator is configured."""
    source_language = str(payload.get("source", {}).get("language") or "")
    items = payload.get("items")
    if not config.translate_with_azure or source_language.lower().startswith("en") or not isinstance(items, list) or not items:
        payload["translation_status"] = "not_requested"
        return
    api_key = os.environ.get(config.azure_translator_key_env)
    if not api_key:
        logging.info("Skipping external news translation because %s is not set.", config.azure_translator_key_env)
        payload["translation_status"] = "missing_key"
        return
    rate_limiter = rate_limiter or TranslationRateLimiter(config.translation_rate_limit_chars_per_minute)
    try:
        stats = enrich_payload_with_cached_azure_translator(
            payload,
            config=config,
            adapter=adapter if config.translation_cache_enabled else None,
            rate_limiter=rate_limiter,
            api_key=api_key,
            region=os.environ.get(config.azure_translator_region_env, ""),
            source_language=source_language,
        )
        payload["translation_cache_stats"] = stats
        payload["translation_status"] = translation_status_from_stats(stats)
    except Exception as exc:  # pragma: no cover - depends on external service limits.
        logging.warning(
            "External news translation failed for %s; keeping original source text: %s",
            payload.get("source", {}).get("id"),
            exc,
        )
        payload["translation_status"] = "failed"
        payload["translation_error"] = str(exc)


def enrich_payload_with_cached_azure_translator(
    payload: dict[str, Any],
    *,
    config: ExternalNewsSyncConfig,
    adapter: Any | None,
    rate_limiter: TranslationRateLimiter,
    api_key: str,
    region: str,
    source_language: str,
) -> dict[str, int]:
    """Apply cached translations or translate source items one at a time."""
    items = payload.get("items")
    if not isinstance(items, list):
        return empty_translation_stats()

    stats = empty_translation_stats()
    cache_available = adapter is not None
    for item in items:
        if not isinstance(item, dict):
            stats["skipped"] += 1
            continue

        cached = None
        if cache_available:
            try:
                cached = load_cached_translation(adapter, item, config.translation_target_language)
            except Exception as exc:  # pragma: no cover - defensive production fallback.
                cache_available = False
                stats["cache_errors"] += 1
                logging.warning("Disabling translation cache for this run after SQL cache read failed: %s", exc)
        if cached:
            apply_cached_translation(item, cached)
            stats["hits"] += 1
            continue

        stats["misses"] += 1
        try:
            enriched = enrich_item_with_azure_translator(
                item,
                config=config,
                rate_limiter=rate_limiter,
                api_key=api_key,
                region=region,
                source_language=source_language,
            )
        except Exception as exc:  # pragma: no cover - depends on external pages/API.
            item["enrichment_status"] = "azure_translator_failed"
            item["enrichment_model"] = AZURE_TRANSLATOR_MODEL_LABEL
            item["enrichment_error"] = str(exc)
            stats["failed"] += 1
            logging.warning(
                "Azure Translator enrichment failed for %s/%s: %s",
                item.get("source_id"),
                item.get("source_article_id"),
                exc,
            )
            continue
        if enriched:
            stats["translated"] += 1
            if cache_available:
                try:
                    save_cached_translation(adapter, item, config.translation_target_language)
                    stats["stored"] += 1
                except Exception as exc:  # pragma: no cover - defensive production fallback.
                    cache_available = False
                    stats["cache_errors"] += 1
                    logging.warning("Disabling translation cache for this run after SQL cache write failed: %s", exc)
        else:
            stats["skipped"] += 1
    return stats


def empty_translation_stats() -> dict[str, int]:
    """Return counters for source translation work."""
    return {
        "hits": 0,
        "misses": 0,
        "translated": 0,
        "stored": 0,
        "failed": 0,
        "skipped": 0,
        "cache_errors": 0,
    }


def translation_status_from_stats(stats: dict[str, int]) -> str:
    """Return a compact status string from translation counters."""
    if stats.get("failed") and (stats.get("hits") or stats.get("translated")):
        return "partial"
    if stats.get("failed"):
        return "failed"
    if stats.get("translated") and stats.get("hits"):
        return "translated_with_cache"
    if stats.get("translated"):
        return "translated"
    if stats.get("hits"):
        return "cached"
    if stats.get("skipped"):
        return "skipped"
    return "translated"


def enrich_item_with_azure_translator(
    item: dict[str, Any],
    *,
    config: ExternalNewsSyncConfig,
    rate_limiter: TranslationRateLimiter,
    api_key: str,
    region: str,
    source_language: str,
) -> bool:
    """Translate one source item and set English helper fields."""
    record = enrichment_record_for_item(item, detail_chars=config.enrichment_detail_chars)
    texts, slots = item_translation_texts_and_slots(item, record)
    if not texts:
        item["enrichment_status"] = "azure_translator_no_text"
        item["enrichment_model"] = AZURE_TRANSLATOR_MODEL_LABEL
        return False

    translations = azure_translate_texts_with_retry(
        texts,
        config=config,
        rate_limiter=rate_limiter,
        api_key=api_key,
        region=region,
        source_language=source_language,
    )
    for field, translated in zip(slots, translations):
        translated = normalize_text(translated)
        if field == "summary_en":
            translated = translated_excerpt(translated)
        item[field] = translated

    item["enrichment_status"] = "azure_translator_enriched" if item.get("summary_en") else "azure_translator_partial"
    item["enrichment_model"] = AZURE_TRANSLATOR_MODEL_LABEL
    return bool(item.get("title_en") or item.get("category_en") or item.get("summary_en"))


def item_translation_texts_and_slots(item: dict[str, Any], record: dict[str, Any]) -> tuple[list[str], list[str]]:
    """Return source texts and target fields for one item."""
    texts: list[str] = []
    slots: list[str] = []
    title = normalize_text(str(record.get("title") or ""))
    category = normalize_text(str(record.get("category") or ""))
    summary_source = normalize_text(str(record.get("detail_text") or item.get("summary") or ""))
    if title:
        slots.append("title_en")
        texts.append(title)
    if category and not item.get("category_en"):
        slots.append("category_en")
        texts.append(category)
    if summary_source:
        slots.append("summary_en")
        texts.append(summary_source)
    return texts, slots


def azure_translate_texts_with_retry(
    texts: list[str],
    *,
    config: ExternalNewsSyncConfig,
    rate_limiter: TranslationRateLimiter,
    api_key: str,
    region: str,
    source_language: str,
) -> list[str]:
    """Translate text with pacing and one optional retry after throttling."""
    char_count = sum(len(text) for text in texts)
    translator_url = os.environ.get("AZURE_TRANSLATOR_URL", "https://api.cognitive.microsofttranslator.com/translate")
    max_attempts = max(1, int(config.translation_retry_count) + 1)
    last_error: Exception | None = None
    for attempt in range(max_attempts):
        rate_limiter.wait_for(char_count)
        try:
            return azure_translate_texts(
                texts,
                api_key=api_key,
                region=region,
                translator_url=translator_url,
                source_language=source_language,
                target_language=config.translation_target_language,
            )
        except RuntimeError as exc:
            last_error = exc
            if "HTTP 429" not in str(exc) or attempt + 1 >= max_attempts:
                raise
            logging.warning(
                "Azure Translator returned HTTP 429; pausing %.1f seconds before retry.",
                config.translation_retry_seconds,
            )
            time.sleep(max(0.0, config.translation_retry_seconds))
    if last_error:
        raise last_error
    return []


def load_cached_translation(adapter: Any, item: dict[str, Any], target_language: str) -> dict[str, Any] | None:
    """Return a cached translation row, updating cache usage counters."""
    key = translation_cache_key(item, target_language)
    if key is None:
        return None
    rows = adapter.query_rows(
        """
        SET NOCOUNT ON;
        UPDATE [clubexpress].[external_news_translation_cache]
        SET
            [Hit_Count] = [Hit_Count] + 1,
            [Last_Used_At] = SYSUTCDATETIME()
        OUTPUT
            INSERTED.[Source_ID],
            INSERTED.[Source_Article_ID],
            INSERTED.[Content_Sha256],
            INSERTED.[Source_Language],
            INSERTED.[Target_Language],
            INSERTED.[Title_En],
            INSERTED.[Category_En],
            INSERTED.[Summary_En],
            INSERTED.[Enrichment_Status],
            INSERTED.[Enrichment_Model]
        WHERE [Source_ID] = ?
          AND [Source_Article_ID] = ?
          AND [Content_Sha256] = ?
          AND [Target_Language] = ?;
        """,
        key,
    )
    return rows[0] if rows else None


def apply_cached_translation(item: dict[str, Any], row: dict[str, Any]) -> None:
    """Copy cached English helper fields onto a source item."""
    item["title_en"] = normalize_text(str(row.get("Title_En") or ""))
    item["category_en"] = normalize_text(str(row.get("Category_En") or ""))
    item["summary_en"] = normalize_text(str(row.get("Summary_En") or ""))
    item["enrichment_status"] = str(row.get("Enrichment_Status") or "azure_translator_cache_hit")
    item["enrichment_model"] = str(row.get("Enrichment_Model") or AZURE_TRANSLATOR_MODEL_LABEL)
    item["translation_cache_status"] = "hit"


def save_cached_translation(adapter: Any, item: dict[str, Any], target_language: str) -> None:
    """Insert or update a cached translation row for one source item."""
    key = translation_cache_key(item, target_language)
    if key is None:
        return
    source_id, source_article_id, content_sha, target_language = key
    adapter.query_rows(
        """
        SET NOCOUNT ON;
        MERGE [clubexpress].[external_news_translation_cache] WITH (HOLDLOCK) AS target
        USING
        (
            SELECT
                ? AS [Source_ID],
                ? AS [Source_Article_ID],
                ? AS [Content_Sha256],
                ? AS [Source_Language],
                ? AS [Target_Language],
                ? AS [Title_En],
                ? AS [Category_En],
                ? AS [Summary_En],
                ? AS [Enrichment_Status],
                ? AS [Enrichment_Model]
        ) AS source
            ON target.[Source_ID] = source.[Source_ID]
           AND target.[Source_Article_ID] = source.[Source_Article_ID]
           AND target.[Content_Sha256] = source.[Content_Sha256]
           AND target.[Target_Language] = source.[Target_Language]
        WHEN MATCHED THEN
            UPDATE SET
                [Source_Language] = source.[Source_Language],
                [Title_En] = source.[Title_En],
                [Category_En] = source.[Category_En],
                [Summary_En] = source.[Summary_En],
                [Enrichment_Status] = source.[Enrichment_Status],
                [Enrichment_Model] = source.[Enrichment_Model],
                [Updated_At] = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT
            (
                [Source_ID],
                [Source_Article_ID],
                [Content_Sha256],
                [Source_Language],
                [Target_Language],
                [Title_En],
                [Category_En],
                [Summary_En],
                [Enrichment_Status],
                [Enrichment_Model]
            )
            VALUES
            (
                source.[Source_ID],
                source.[Source_Article_ID],
                source.[Content_Sha256],
                source.[Source_Language],
                source.[Target_Language],
                source.[Title_En],
                source.[Category_En],
                source.[Summary_En],
                source.[Enrichment_Status],
                source.[Enrichment_Model]
            )
        OUTPUT INSERTED.[External_News_Translation_Cache_ID];
        """,
        (
            source_id,
            source_article_id,
            content_sha,
            str(item.get("source_language") or ""),
            target_language,
            normalize_text(str(item.get("title_en") or "")) or None,
            normalize_text(str(item.get("category_en") or "")) or None,
            normalize_text(str(item.get("summary_en") or "")) or None,
            str(item.get("enrichment_status") or "azure_translator_enriched"),
            str(item.get("enrichment_model") or AZURE_TRANSLATOR_MODEL_LABEL),
        ),
    )


def translation_cache_key(item: dict[str, Any], target_language: str) -> tuple[str, str, str, str] | None:
    """Return the stable SQL cache key for one source item."""
    source_id = str(item.get("source_id") or "").strip()[:64]
    source_article_id = str(item.get("source_article_id") or item.get("url") or "").strip()[:512]
    content_sha = normalize_hash(item.get("content_sha256")) or sha256_text(
        normalize_whitespace(
            "\n".join(
                str(item.get(field) or "")
                for field in ("url", "title", "category", "published_text", "summary")
            )
        )
    )
    target_language = str(target_language or "en").strip()[:16] or "en"
    if not source_id or not source_article_id or not content_sha:
        return None
    return source_id, source_article_id, content_sha, target_language


def source_item_ledger_status(adapter: Any, config: ExternalNewsSyncConfig) -> dict[str, Any]:
    """Return source item ledger availability and baseline counters."""
    status: dict[str, Any] = {
        "enabled": bool(config.source_item_ledger_enabled),
        "available": False,
        "fetched_item_count": 0,
        "loaded_item_count": 0,
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
    }
    if not config.source_item_ledger_enabled:
        return status
    try:
        rows = adapter.query_rows(
            """
            SELECT
                CASE
                    WHEN OBJECT_ID(N'clubexpress.external_news_source_items', N'U') IS NULL THEN 0
                    ELSE 1
                END AS [Table_Exists];
            """
        )
    except Exception as exc:  # pragma: no cover - defensive production fallback.
        logging.warning("External source item ledger availability check failed: %s", exc)
        status["error"] = str(exc)
        return status
    status["available"] = bool(int(rows[0].get("Table_Exists") or 0)) if rows else False
    return status


def upsert_external_source_items(adapter: Any, items: list[dict[str, Any]], run_key: str) -> dict[str, Any]:
    """Insert or update fetched external source items in the persistent ledger."""
    stats: dict[str, Any] = {"inserted": 0, "updated": 0, "skipped": 0, "inserted_item_ids": []}
    for item in items:
        params = ledger_source_item_params(item, run_key)
        if params is None:
            stats["skipped"] += 1
            continue
        rows = adapter.query_rows(
            """
            SET NOCOUNT ON;
            MERGE [clubexpress].[external_news_source_items] WITH (HOLDLOCK) AS target
            USING
            (
                SELECT
                    ? AS [Source_ID],
                    ? AS [Source_Article_ID],
                    ? AS [Source_Article_Key_Sha256],
                    ? AS [Source_Name],
                    ? AS [Source_Home_Url],
                    ? AS [Source_Language],
                    ? AS [Canonical_Url],
                    ? AS [Canonical_Url_Sha256],
                    ? AS [Title],
                    ? AS [Author],
                    ? AS [Category],
                    ? AS [Published_Text],
                    ? AS [Published_At],
                    ? AS [Digest_Date],
                    ? AS [Summary],
                    ? AS [Summary_Type],
                    ? AS [Title_En],
                    ? AS [Category_En],
                    ? AS [Summary_En],
                    ? AS [Enrichment_Status],
                    ? AS [Enrichment_Model],
                    ? AS [Image_Url],
                    ? AS [Last_Fetched_At],
                    ? AS [Content_Sha256],
                    ? AS [Last_Run_Key],
                    ? AS [Raw_Item_Json]
            ) AS source
                ON target.[Source_ID] = source.[Source_ID]
               AND target.[Source_Article_Key_Sha256] = source.[Source_Article_Key_Sha256]
            WHEN MATCHED THEN
                UPDATE SET
                    [Source_Article_ID] = source.[Source_Article_ID],
                    [Source_Name] = source.[Source_Name],
                    [Source_Home_Url] = source.[Source_Home_Url],
                    [Source_Language] = source.[Source_Language],
                    [Canonical_Url] = source.[Canonical_Url],
                    [Canonical_Url_Sha256] = source.[Canonical_Url_Sha256],
                    [Title] = source.[Title],
                    [Author] = source.[Author],
                    [Category] = source.[Category],
                    [Published_Text] = source.[Published_Text],
                    [Published_At] = source.[Published_At],
                    [Summary] = source.[Summary],
                    [Summary_Type] = source.[Summary_Type],
                    [Title_En] = source.[Title_En],
                    [Category_En] = source.[Category_En],
                    [Summary_En] = source.[Summary_En],
                    [Enrichment_Status] = source.[Enrichment_Status],
                    [Enrichment_Model] = source.[Enrichment_Model],
                    [Image_Url] = source.[Image_Url],
                    [Last_Seen_At] = SYSUTCDATETIME(),
                    [Last_Fetched_At] = source.[Last_Fetched_At],
                    [Content_Sha256] = source.[Content_Sha256],
                    [Last_Run_Key] = source.[Last_Run_Key],
                    [Raw_Item_Json] = source.[Raw_Item_Json],
                    [Updated_At] = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT
                (
                    [Source_ID],
                    [Source_Article_ID],
                    [Source_Article_Key_Sha256],
                    [Source_Name],
                    [Source_Home_Url],
                    [Source_Language],
                    [Canonical_Url],
                    [Canonical_Url_Sha256],
                    [Title],
                    [Author],
                    [Category],
                    [Published_Text],
                    [Published_At],
                    [Digest_Date],
                    [Summary],
                    [Summary_Type],
                    [Title_En],
                    [Category_En],
                    [Summary_En],
                    [Enrichment_Status],
                    [Enrichment_Model],
                    [Image_Url],
                    [Last_Fetched_At],
                    [Content_Sha256],
                    [Last_Run_Key],
                    [Raw_Item_Json]
                )
                VALUES
                (
                    source.[Source_ID],
                    source.[Source_Article_ID],
                    source.[Source_Article_Key_Sha256],
                    source.[Source_Name],
                    source.[Source_Home_Url],
                    source.[Source_Language],
                    source.[Canonical_Url],
                    source.[Canonical_Url_Sha256],
                    source.[Title],
                    source.[Author],
                    source.[Category],
                    source.[Published_Text],
                    source.[Published_At],
                    source.[Digest_Date],
                    source.[Summary],
                    source.[Summary_Type],
                    source.[Title_En],
                    source.[Category_En],
                    source.[Summary_En],
                    source.[Enrichment_Status],
                    source.[Enrichment_Model],
                    source.[Image_Url],
                    source.[Last_Fetched_At],
                    source.[Content_Sha256],
                    source.[Last_Run_Key],
                    source.[Raw_Item_Json]
                )
            OUTPUT
                $action AS [Merge_Action],
                INSERTED.[External_News_Source_Item_ID] AS [External_News_Source_Item_ID];
            """,
            params,
        )
        action = str(rows[0].get("Merge_Action") or "").upper() if rows else ""
        if action == "INSERT":
            stats["inserted"] += 1
            item_id = rows[0].get("External_News_Source_Item_ID") if rows else None
            if item_id is not None:
                stats["inserted_item_ids"].append(int(item_id))
        elif action == "UPDATE":
            stats["updated"] += 1
    return stats


def ledger_source_item_params(item: dict[str, Any], run_key: str) -> tuple[Any, ...] | None:
    """Return normalized SQL parameters for one source item ledger row."""
    source_id = ledger_text(item.get("source_id"), 64)
    source_article_id = ledger_text(item.get("source_article_id") or item.get("url"), 2048)
    if not source_id or not source_article_id:
        return None
    canonical_url = ledger_text(item.get("url"), 2048)
    content_sha = normalize_hash(item.get("content_sha256")) or source_item_content_hash(item)
    return (
        source_id,
        source_article_id,
        sha256_text(source_article_id),
        ledger_text(item.get("source_name"), 256),
        ledger_text(item.get("source_home_url"), 2048),
        ledger_text(item.get("source_language"), 32),
        canonical_url,
        sha256_text(canonical_url) if canonical_url else None,
        ledger_text(item.get("title"), 1024),
        ledger_text(item.get("author"), 512),
        ledger_text(item.get("category"), 512),
        ledger_text(item.get("published_text"), 512),
        ledger_text(item.get("published_at"), 64),
        parse_digest_date(item.get("digest_date")),
        ledger_text(item.get("summary")),
        ledger_text(item.get("summary_type"), 64),
        ledger_text(item.get("title_en"), 1024),
        ledger_text(item.get("category_en"), 512),
        ledger_text(item.get("summary_en")),
        ledger_text(item.get("enrichment_status"), 64),
        ledger_text(item.get("enrichment_model"), 128),
        ledger_text(item.get("image_url"), 2048),
        ledger_text(item.get("fetched_at"), 64),
        content_sha,
        ledger_text(run_key, 128),
        json_dumps(item),
    )


def source_item_content_hash(item: dict[str, Any]) -> str:
    """Return a stable content hash when the parser did not provide one."""
    return sha256_text(
        normalize_whitespace(
            "\n".join(
                str(item.get(field) or "")
                for field in (
                    "source_id",
                    "source_article_id",
                    "url",
                    "title",
                    "category",
                    "published_text",
                    "summary",
                    "title_en",
                    "category_en",
                    "summary_en",
                )
            )
        )
    )


def ledger_text(value: Any, max_length: int | None = None) -> str | None:
    """Normalize optional source item text for SQL storage."""
    text = normalize_text(str(value or ""))
    if not text:
        return None
    if max_length is not None:
        return text[:max_length]
    return text


def load_external_source_items_for_digest_date(
    adapter: Any,
    digest_date: date,
    config: ExternalNewsSyncConfig,
) -> list[dict[str, Any]]:
    """Load items first discovered on one Eastern digest date."""
    rows = adapter.query_rows(
        """
        SELECT
            [Source_ID],
            [Source_Article_ID],
            [Source_Name],
            [Source_Home_Url],
            [Source_Language],
            [Canonical_Url],
            [Title],
            [Author],
            [Category],
            [Published_Text],
            [Published_At],
            [Digest_Date],
            [Summary],
            [Summary_Type],
            [Title_En],
            [Category_En],
            [Summary_En],
            [Enrichment_Status],
            [Enrichment_Model],
            [Image_Url],
            [Content_Sha256],
            [First_Seen_At]
        FROM [clubexpress].[external_news_source_items]
        WHERE [Digest_Date] = ?;
        """,
        (digest_date,),
    )
    return sort_source_items([source_item_from_ledger_row(row) for row in rows], config)


def source_item_from_ledger_row(row: dict[str, Any]) -> dict[str, Any]:
    """Convert one SQL ledger row back to the normalized source item shape."""
    digest_date = row.get("Digest_Date")
    if isinstance(digest_date, (datetime, date)):
        digest_date_text = digest_date.isoformat()[:10]
    else:
        digest_date_text = str(digest_date or "")[:10]
    return {
        "source_id": row_text(row, "Source_ID"),
        "source_name": row_text(row, "Source_Name"),
        "source_home_url": row_text(row, "Source_Home_Url"),
        "source_language": row_text(row, "Source_Language"),
        "source_article_id": row_text(row, "Source_Article_ID"),
        "title": row_text(row, "Title"),
        "url": row_text(row, "Canonical_Url"),
        "author": row_text(row, "Author"),
        "category": row_text(row, "Category"),
        "published_text": row_text(row, "Published_Text"),
        "published_at": row_text(row, "Published_At"),
        "digest_date": digest_date_text,
        "summary": row_text(row, "Summary"),
        "summary_type": row_text(row, "Summary_Type"),
        "title_en": row_text(row, "Title_En"),
        "category_en": row_text(row, "Category_En"),
        "summary_en": row_text(row, "Summary_En"),
        "enrichment_status": row_text(row, "Enrichment_Status"),
        "enrichment_model": row_text(row, "Enrichment_Model"),
        "image_url": row_text(row, "Image_Url"),
        "content_sha256": row_text(row, "Content_Sha256"),
        "discovered_at": datetime_text(row.get("First_Seen_At")),
    }


def row_text(row: dict[str, Any], key: str) -> str:
    """Return a SQL row value as display-safe text."""
    value = row.get(key)
    return "" if value is None else str(value)


def datetime_text(value: Any) -> str:
    """Return a SQL datetime as an explicit UTC ISO timestamp."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    return str(value or "")


def digest_date_window(config: ExternalNewsSyncConfig) -> tuple[date, date]:
    """Return the inclusive Eastern digest date window."""
    today = eastern_date(datetime.now(timezone.utc))
    start_date = today - timedelta(days=max(1, int(config.digest_lookback_days or 1)) - 1)
    return start_date, today


def recent_digest_items(items: list[dict[str, Any]], config: ExternalNewsSyncConfig) -> list[dict[str, Any]]:
    """Return source items in the configured digest lookback window."""
    start_date, today = digest_date_window(config)
    recent_items: list[dict[str, Any]] = []
    for item in items:
        digest_date = parse_digest_date(item.get("digest_date"))
        if digest_date is not None and start_date <= digest_date <= today:
            recent_items.append(item)
    return sort_source_items(recent_items, config)


def build_digest_articles(
    items: list[dict[str, Any]],
    config: ExternalNewsSyncConfig,
    *,
    digest_date: date | None = None,
) -> list[dict[str, Any]]:
    """Build at most one digest for the requested Eastern discovery date."""
    target_date = digest_date or eastern_date(datetime.now(timezone.utc))
    digest_items = sort_source_items(
        [item for item in items if parse_digest_date(item.get("digest_date")) == target_date],
        config,
    )
    if not digest_items:
        return []

    articles: list[dict[str, Any]] = []
    for digest_date in (target_date,):
        article_html = render_digest_article_html(digest_date, digest_items)
        article_text = text_from_markup(article_html)
        headline = f"Go News Digest: {display_date(digest_date)}"
        summary = digest_summary(digest_items)
        item_id = external_digest_item_id(digest_date)
        links = digest_links(digest_items)
        rendered_metadata = {
            "sourceMode": SOURCE_MODE,
            "digestDate": digest_date.isoformat(),
            "itemId": item_id,
            "headline": headline,
            "sourceItemCount": len(digest_items),
            "sourceIds": sorted({str(item.get("source_id") or "") for item in digest_items if item.get("source_id")}),
            "items": digest_items,
        }
        manager_row = {
            "activeDate": digest_date.isoformat(),
            "author": "AGA external news monitor",
            "category": "External Go News Digest",
            "headline": headline,
            "itemId": str(item_id),
            "sourcePage": SOURCE_MODE,
            "status": "Active",
            "visible": "To Everyone",
            "viewUrl": "",
        }
        articles.append(
            {
                "item_id": item_id,
                "headline": headline,
                "author": "AGA external news monitor",
                "category": "External Go News Digest",
                "status": "Active",
                "visibility": "To Everyone",
                "active_date": digest_date,
                "expires_date": None,
                "summary": summary,
                "view_url": "",
                "article_html": article_html,
                "container_html": article_html,
                "article_text": article_text,
                "images": [],
                "links": links,
                "rendered_metadata": rendered_metadata,
                "manager_row": manager_row,
            }
        )
    return articles


def sort_source_items(items: list[dict[str, Any]], config: ExternalNewsSyncConfig) -> list[dict[str, Any]]:
    """Return source items in a stable source/date/title order."""
    source_order = {source_id: index for index, source_id in enumerate(config.source_ids)}
    return sorted(items, key=lambda item: source_item_sort_key(item, source_order))


def source_item_sort_key(item: dict[str, Any], source_order: dict[str, int]) -> tuple[int, float, str, str]:
    """Return a stable sort key for source items."""
    source_id = str(item.get("source_id") or "")
    return (
        source_order.get(source_id, len(source_order)),
        -source_item_published_timestamp(item),
        display_item_title(item).casefold(),
        str(item.get("url") or ""),
    )


def source_item_published_timestamp(item: dict[str, Any]) -> float:
    """Return a sortable timestamp for source item ordering."""
    published_at = str(item.get("published_at") or "").strip()
    if published_at:
        with suppress(ValueError):
            parsed = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.timestamp()
    digest_date = parse_digest_date(item.get("digest_date"))
    if digest_date is not None:
        return datetime(digest_date.year, digest_date.month, digest_date.day, tzinfo=timezone.utc).timestamp()
    return 0.0


def render_digest_article_html(digest_date: date, items: list[dict[str, Any]]) -> str:
    """Render external source items as a ClubExpress-style digest article body."""
    item_word = "item" if len(items) == 1 else "items"
    parts = [
        (
            f"<p>This daily digest includes {len(items)} external Go news source {item_word} "
            f"first discovered on {html.escape(display_date(digest_date))} Eastern Time. "
            "Source publication timestamps are shown in Eastern Time when a timestamp is available.</p>"
        )
    ]
    for source_name, source_items in grouped_by_source(items):
        parts.append(f"<h3>{html.escape(source_name)}</h3>")
        for item in source_items:
            title = display_item_title(item)
            url = str(item.get("url") or "")
            parts.append(f"<p><strong>{html_link(url, title)}</strong></p>")
            metadata = item_metadata_parts(item)
            if metadata:
                parts.append(f"<p><em>{html.escape(' | '.join(metadata))}</em></p>")
            summary = display_item_summary(item)
            if summary:
                parts.append(f"<p>{html.escape(summary)}</p>")
            else:
                parts.append("<p><em>No source excerpt was available.</em></p>")
    return "\n".join(parts)


def item_metadata_parts(item: dict[str, Any]) -> list[str]:
    """Return compact source item metadata for display."""
    parts: list[str] = []
    category = str(item.get("category_en") or "").strip()
    if not category and source_is_english(item):
        category = str(item.get("category") or "").strip()
    if category:
        parts.append(category)
    author = str(item.get("author") or "").strip()
    if author and source_is_english(item):
        parts.append(f"By {author}")
    published_at = str(item.get("published_at") or "").strip()
    eastern_timestamp = eastern_timestamp_text(published_at)
    if eastern_timestamp:
        parts.append(f"Source time (Eastern): {eastern_timestamp}")
    elif source_is_english(item):
        published_text = str(item.get("published_text") or "").strip()
        if published_text:
            parts.append(f"Source date: {published_text}")
    return parts


def eastern_date(value: datetime) -> date:
    """Return the America/New_York calendar date for a timestamp."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(EASTERN_TIME_ZONE).date()


def eastern_timestamp_text(value: str) -> str:
    """Format an offset-aware ISO timestamp in America/New_York."""
    text = str(value or "").strip()
    if not text or "T" not in text:
        return ""
    with suppress(ValueError):
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return ""
        eastern = parsed.astimezone(EASTERN_TIME_ZONE)
        hour = eastern.strftime("%I").lstrip("0") or "0"
        return (
            f"{MONTH_NAMES[eastern.month]} {eastern.day}, {eastern.year} "
            f"{hour}:{eastern.strftime('%M %p %Z')}"
        )
    return ""


def display_item_title(item: dict[str, Any]) -> str:
    """Return an English helper title when available, otherwise the source title."""
    title_en = str(item.get("title_en") or "").strip()
    if title_en:
        return title_en
    if source_is_english(item):
        title = str(item.get("title") or "").strip()
        if title:
            return title
    return f"Source item from {item.get('source_name') or item.get('source_id') or 'external source'}"


def display_item_summary(item: dict[str, Any]) -> str:
    """Return an English helper summary when available, otherwise a source excerpt."""
    summary_en = normalize_whitespace(str(item.get("summary_en") or ""))
    if summary_en:
        return summary_en
    if source_is_english(item):
        return normalize_whitespace(str(item.get("summary") or ""))
    return ""


def source_is_english(item: dict[str, Any]) -> bool:
    """Return whether an item's source language is English."""
    return str(item.get("source_language") or "").lower().startswith("en")


def digest_summary(items: list[dict[str, Any]]) -> str:
    """Return a short list-page summary for a digest."""
    item_word = "item" if len(items) == 1 else "items"
    return f"Daily external-source digest with {len(items)} {item_word} from {human_join(digest_sources(items))}."


def digest_sources(items: list[dict[str, Any]]) -> list[str]:
    """Return ordered source names for a digest."""
    sources = []
    seen = set()
    for item in items:
        source = str(item.get("source_name") or item.get("source_id") or "External source").strip()
        if source and source not in seen:
            seen.add(source)
            sources.append(source)
    return sources


def grouped_by_source(items: list[dict[str, Any]]) -> list[tuple[str, list[dict[str, Any]]]]:
    """Group source items while preserving first-seen source order."""
    groups: list[tuple[str, list[dict[str, Any]]]] = []
    group_map: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        source = str(item.get("source_name") or item.get("source_id") or "External source")
        if source not in group_map:
            group_map[source] = []
            groups.append((source, group_map[source]))
        group_map[source].append(item)
    return groups


def digest_links(items: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Return article links for all source items in the digest."""
    links = []
    for item in items:
        href = str(item.get("url") or "").strip()
        if not href:
            continue
        links.append({"href": href, "text": display_item_title(item)[:1024]})
    return links


def html_link(url: str, label: str) -> str:
    """Return a safe external link, or plain text when no URL is available."""
    label_html = html.escape(label)
    if not url:
        return label_html
    return f'<a href="{html.escape(url, quote=True)}">{label_html}</a>'


def display_date(value: date) -> str:
    """Display a date without platform-specific strftime flags."""
    return f"{MONTH_NAMES[value.month]} {value.day}, {value.year}"


def human_join(values: list[str]) -> str:
    """Join source names in a readable sentence fragment."""
    unique_values = list(dict.fromkeys(value for value in values if value))
    if not unique_values:
        return "external Go news sources"
    if len(unique_values) == 1:
        return unique_values[0]
    if len(unique_values) == 2:
        return f"{unique_values[0]} and {unique_values[1]}"
    return f"{', '.join(unique_values[:-1])}, and {unique_values[-1]}"


def parse_digest_date(value: Any) -> date | None:
    """Parse an ISO digest date."""
    text = str(value or "").strip()
    if not text:
        return None
    with suppress(ValueError):
        return date.fromisoformat(text[:10])
    return None


def external_digest_item_id(digest_date: date) -> int:
    """Return a stable synthetic item id for one external digest date."""
    return 800000000 + int(digest_date.strftime("%Y%m%d"))


def compute_digest_hashes(article: dict[str, Any]) -> dict[str, str]:
    """Compute hashes used for external digest change detection and storage."""
    article_html = str(article["article_html"])
    container_html = str(article["container_html"])
    rendered_metadata_json = json_dumps(article["rendered_metadata"])
    return {
        "rendered_article_sha256": sha256_text(article_html),
        "rendered_container_sha256": sha256_text(container_html),
        "content_sha256": sha256_text("|".join([article_html, container_html, rendered_metadata_json])),
    }


def classify_digest_change(latest: dict[str, Any] | None, hashes: dict[str, str]) -> str | None:
    """Return a digest change reason, or None when already archived."""
    if latest is None:
        return "new"
    latest_content_hash = normalize_hash(latest.get("Content_Sha256"))
    if latest_content_hash and latest_content_hash == hashes["content_sha256"]:
        return None
    latest_article_hash = normalize_hash(latest.get("Rendered_Article_Sha256"))
    latest_container_hash = normalize_hash(latest.get("Rendered_Container_Sha256"))
    if (
        latest_article_hash
        and latest_container_hash
        and latest_article_hash == hashes["rendered_article_sha256"]
        and latest_container_hash == hashes["rendered_container_sha256"]
    ):
        return None
    return "updated"


def create_external_archive_run(
    adapter: Any,
    run_key: str,
    config: ExternalNewsSyncConfig,
    container_client: Any,
    blob_prefix: str,
    fetched_sources: list[dict[str, Any]],
    failed_sources: list[dict[str, Any]],
    digest_articles: list[dict[str, Any]],
    changed_articles: list[dict[str, Any]],
    unchanged_articles: list[dict[str, Any]],
    ledger_stats: dict[str, Any],
) -> int:
    """Create a scheduled external digest archive run row."""
    rows = adapter.query_rows(
        """
        INSERT INTO [clubexpress].[news_archive_runs]
        (
            [Run_Key],
            [Run_Type],
            [Source_Mode],
            [Source_Description],
            [Status],
            [Blob_Account],
            [Blob_Container],
            [Blob_Prefix],
            [Summary_Json]
        )
        OUTPUT INSERTED.[News_Archive_Run_ID]
        VALUES (?, N'scheduled', ?, ?, N'running', ?, ?, ?, ?);
        """,
        (
            run_key,
            SOURCE_MODE,
            config.source_description,
            str(getattr(container_client, "account_name", "") or ""),
            str(getattr(container_client, "container_name", "") or ""),
            blob_prefix,
            json_dumps(
                {
                    "source_ids": list(config.source_ids),
                    "fetched_sources": fetched_sources,
                    "failed_sources": failed_sources,
                    "candidate_digest_count": len(digest_articles),
                    "changed_digest_count": len(changed_articles),
                    "unchanged_digest_count": len(unchanged_articles),
                    "changed_item_ids": [item["article"]["item_id"] for item in changed_articles],
                    "source_item_ledger": ledger_stats,
                }
            ),
        ),
    )
    return int(rows[0]["News_Archive_Run_ID"])


def upload_digest_article(container_client: Any, blob_prefix: str, article: dict[str, Any]) -> dict[str, Any]:
    """Upload one digest article's archive artifacts."""
    item_prefix = clean_blob_path(f"{blob_prefix}/{article['item_id']}")
    hashes = article.get("hashes") or compute_digest_hashes(article)
    article_html = str(article["article_html"])
    container_html = str(article["container_html"])
    rendered_metadata_json = json_dumps(article["rendered_metadata"])
    manager_row_json = json_dumps(article["manager_row"])
    asset_manifest = {
        "capturedAt": datetime.now(timezone.utc).isoformat(),
        "articleItemId": article["item_id"],
        "sourceMode": SOURCE_MODE,
        "assets": [],
    }
    paths = {
        "manager_row": upload_text_blob(container_client, f"{item_prefix}/manager-row.json", manager_row_json, "application/json"),
        "rendered_metadata": upload_text_blob(
            container_client,
            f"{item_prefix}/rendered-article.json",
            rendered_metadata_json,
            "application/json",
        ),
        "rendered_article": upload_text_blob(container_client, f"{item_prefix}/rendered-article.html", article_html, "text/html"),
        "rendered_container": upload_text_blob(container_client, f"{item_prefix}/rendered-container.html", container_html, "text/html"),
        "asset_manifest": upload_text_blob(
            container_client,
            f"{item_prefix}/asset-manifest.json",
            json_dumps(asset_manifest),
            "application/json",
        ),
    }
    return {
        "paths": paths,
        "assets": [],
        "asset_manifest": asset_manifest,
        "rendered_article_sha256": hashes["rendered_article_sha256"],
        "rendered_container_sha256": hashes["rendered_container_sha256"],
        "content_sha256": hashes["content_sha256"],
        "blob_prefix": item_prefix,
    }


def upsert_external_news_article(adapter: Any, run_id: int, article: dict[str, Any]) -> int:
    """Insert or update an external digest article shell and return its archive ID."""
    existing = adapter.query_rows(
        """
        SELECT [News_Article_ID]
        FROM [clubexpress].[news_articles]
        WHERE [Club_ID] = ?
          AND [Item_ID] = ?;
        """,
        (EXTERNAL_NEWS_CLUB_ID, article["item_id"]),
    )
    params = (
        article["headline"],
        article["author"],
        article["category"],
        article["status"],
        article["visibility"],
        article["active_date"],
        article["expires_date"],
        article["view_url"],
        run_id,
    )
    if existing:
        article_id = int(existing[0]["News_Article_ID"])
        adapter.query_rows(
            """
            SET NOCOUNT ON;
            UPDATE [clubexpress].[news_articles]
            SET
                [Headline] = ?,
                [Author] = ?,
                [Category] = ?,
                [Current_Status] = ?,
                [Visibility] = ?,
                [Active_Date] = ?,
                [Expires_Date] = ?,
                [View_Url] = ?,
                [Last_Seen_Run_ID] = ?,
                [Last_Seen_At] = SYSUTCDATETIME(),
                [Updated_At] = SYSUTCDATETIME()
            WHERE [News_Article_ID] = ?;
            SELECT CAST(? AS bigint) AS [News_Article_ID];
            """,
            (*params, article_id, article_id),
        )
        return article_id

    rows = adapter.query_rows(
        """
        INSERT INTO [clubexpress].[news_articles]
        (
            [Club_ID],
            [Item_ID],
            [Headline],
            [Author],
            [Category],
            [Current_Status],
            [Visibility],
            [Active_Date],
            [Expires_Date],
            [View_Url],
            [First_Seen_Run_ID],
            [Last_Seen_Run_ID]
        )
        OUTPUT INSERTED.[News_Article_ID]
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            EXTERNAL_NEWS_CLUB_ID,
            article["item_id"],
            article["headline"],
            article["author"],
            article["category"],
            article["status"],
            article["visibility"],
            article["active_date"],
            article["expires_date"],
            article["view_url"],
            run_id,
            run_id,
        ),
    )
    return int(rows[0]["News_Article_ID"])
