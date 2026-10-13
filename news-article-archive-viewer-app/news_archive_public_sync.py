# Copyright 2026, American Go Association, All rights reserved

"""Public ClubExpress news archive synchronization."""

from __future__ import annotations

import hashlib
import html
import json
import logging
import mimetypes
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from urllib import error, parse, request

try:
    from azure.storage.blob import ContentSettings
except Exception:  # pragma: no cover - available in Azure Functions.
    ContentSettings = None


CLUB_ID = 454497
PUBLIC_NEWS_LIST_URL = f"https://usgo.org/content.aspx?page_id=3&club_id={CLUB_ID}"
PUBLIC_NEWS_VIEW_URL = "https://usgo.org/content.aspx?page_id=5&club_id={club_id}&item_id={item_id}&action=view&al=y&actr=2"
SOURCE_MODE = "public-clubexpress-news-list"
USER_AGENT = "AGA-News-Archive-Sync/1.0 (+https://usgo.org/)"


@dataclass(frozen=True)
class PublicNewsSyncConfig:
    """Configuration for public ClubExpress news synchronization."""

    list_url: str = PUBLIC_NEWS_LIST_URL
    club_id: int = CLUB_ID
    max_candidates: int = 10
    request_timeout_seconds: float = 30.0
    blob_root: str = "scheduled/public-news"
    source_description: str = "Scheduled public ClubExpress news-list sync."


def sync_public_news_archive(adapter: Any, container_client: Any, config: PublicNewsSyncConfig) -> dict[str, Any]:
    """Find new or updated public ClubExpress news articles and push them into the archive."""
    candidates = discover_public_news_candidates(config)
    latest_snapshots = get_latest_snapshot_state(adapter, config.club_id, [item["item_id"] for item in candidates])
    changed_items: list[dict[str, Any]] = []
    unchanged_articles: list[dict[str, Any]] = []
    failed_articles: list[dict[str, Any]] = []

    for candidate in candidates:
        item_id = int(candidate["item_id"])
        latest = latest_snapshots.get(item_id)
        try:
            captured = capture_public_article(candidate, config)
            hashes = compute_public_article_hashes(captured)
            captured["hashes"] = hashes
            change_reason = classify_public_article_change(latest, hashes)
            if not change_reason:
                unchanged_articles.append(
                    {
                        "item_id": item_id,
                        "headline": captured["headline"],
                        "snapshot_id": latest.get("News_Article_Snapshot_ID") if latest else None,
                    }
                )
                continue
            changed_items.append(
                {
                    "candidate": candidate,
                    "captured": captured,
                    "latest": latest,
                    "change_reason": change_reason,
                }
            )
        except Exception as exc:  # pragma: no cover - defensive production logging.
            logging.exception("Failed to inspect public news article %s", item_id)
            failed_articles.append(
                {
                    "item_id": item_id,
                    "headline": candidate.get("headline"),
                    "stage": "inspect",
                    "error": str(exc),
                }
            )

    if not changed_items:
        return {
            "created_run": False,
            "candidate_count": len(candidates),
            "new_article_count": 0,
            "updated_article_count": 0,
            "changed_article_count": 0,
            "archived_article_count": 0,
            "unchanged_article_count": len(unchanged_articles),
            "failed_article_count": len(failed_articles),
            "unchanged_item_ids": [item["item_id"] for item in unchanged_articles],
        }

    run_key = f"scheduled-public-news-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    blob_prefix = clean_blob_path(f"{config.blob_root}/{run_key}")
    run_id = create_archive_run(
        adapter,
        run_key,
        config,
        container_client,
        blob_prefix,
        candidates,
        changed_items,
        unchanged_articles,
        failed_articles,
    )
    archived_articles: list[dict[str, Any]] = []
    asset_count = 0

    try:
        for item in changed_items:
            candidate = item["candidate"]
            captured = item["captured"]
            try:
                uploaded = upload_public_article(container_client, blob_prefix, captured)
                news_article_id = upsert_news_article(adapter, run_id, captured)
                snapshot_id = insert_news_snapshot(adapter, run_id, run_key, news_article_id, captured, uploaded)
                inserted_assets = insert_news_assets(adapter, snapshot_id, news_article_id, uploaded["assets"])
                assign_snapshot_thumbnail_from_captured_images(adapter, snapshot_id)
                insert_news_links(adapter, snapshot_id, news_article_id, captured["links"])
                asset_count += inserted_assets
                archived_articles.append(
                    {
                        "item_id": captured["item_id"],
                        "headline": captured["headline"],
                        "snapshot_id": snapshot_id,
                        "asset_count": inserted_assets,
                        "change_reason": item["change_reason"],
                        "previous_snapshot_id": item["latest"].get("News_Article_Snapshot_ID") if item["latest"] else None,
                    }
                )
            except Exception as exc:  # pragma: no cover - defensive production logging.
                logging.exception("Failed to archive public news article %s", candidate.get("item_id"))
                failed_articles.append(
                    {
                        "item_id": candidate.get("item_id"),
                        "headline": captured.get("headline") if captured else candidate.get("headline"),
                        "stage": "archive",
                        "change_reason": item.get("change_reason"),
                        "error": str(exc),
                    }
                )

        status = "succeeded" if not failed_articles else ("partial" if archived_articles else "failed")
        summary = {
            "candidate_count": len(candidates),
            "new_article_count": sum(1 for item in changed_items if item["change_reason"] == "new"),
            "updated_article_count": sum(1 for item in changed_items if item["change_reason"] == "updated"),
            "changed_article_count": len(changed_items),
            "unchanged_articles": unchanged_articles,
            "archived_articles": archived_articles,
            "failed_articles": failed_articles,
        }
        update_archive_run(
            adapter,
            run_id,
            status=status,
            article_count=len(archived_articles),
            asset_count=asset_count,
            summary=summary,
            error_message=None if status != "failed" else json.dumps(failed_articles, ensure_ascii=False),
        )
        return {
            "created_run": True,
            "run_id": run_id,
            "run_key": run_key,
            "status": status,
            "candidate_count": len(candidates),
            "new_article_count": summary["new_article_count"],
            "updated_article_count": summary["updated_article_count"],
            "changed_article_count": len(changed_items),
            "archived_article_count": len(archived_articles),
            "unchanged_article_count": len(unchanged_articles),
            "failed_article_count": len(failed_articles),
            "asset_count": asset_count,
            "blob_prefix": blob_prefix,
        }
    except Exception as exc:
        update_archive_run(
            adapter,
            run_id,
            status="failed",
            article_count=len(archived_articles),
            asset_count=asset_count,
            summary={
                "archived_articles": archived_articles,
                "unchanged_articles": unchanged_articles,
                "failed_articles": failed_articles,
            },
            error_message=str(exc),
        )
        raise


def discover_public_news_candidates(config: PublicNewsSyncConfig) -> list[dict[str, Any]]:
    """Return recent public article candidates from the ClubExpress news list."""
    markup = fetch_text(config.list_url, config.request_timeout_seconds)
    seen: set[int] = set()
    candidates: list[dict[str, Any]] = []
    for match in re.finditer(r"(?i)item_id=(\d+)", markup):
        item_id = int(match.group(1))
        if item_id in seen:
            continue
        seen.add(item_id)
        view_url = PUBLIC_NEWS_VIEW_URL.format(club_id=config.club_id, item_id=item_id)
        candidates.append({"item_id": item_id, "view_url": view_url})
        if len(candidates) >= max(1, config.max_candidates):
            break
    return candidates


def capture_public_article(candidate: dict[str, Any], config: PublicNewsSyncConfig) -> dict[str, Any]:
    """Fetch and parse one public ClubExpress article."""
    item_id = int(candidate["item_id"])
    view_url = str(candidate.get("view_url") or PUBLIC_NEWS_VIEW_URL.format(club_id=config.club_id, item_id=item_id))
    page_markup = fetch_text(view_url, config.request_timeout_seconds)
    article_html = extract_div_by_class(page_markup, "news-text")
    container_html = extract_div_by_class(page_markup, "article-container")
    if not article_html:
        article_html = container_html or page_markup
    if not container_html:
        container_html = article_html

    headline = text_from_markup(first_match(page_markup, r'(?is)<h2\b[^>]*class=["\'][^"\']*\bzero-top-margin\b[^"\']*["\'][^>]*>(.*?)</h2>'))
    if not headline:
        headline = text_from_markup(first_match(page_markup, r"(?is)<title[^>]*>(.*?)</title>")).replace(" - American Go Association", "")
    author_line = text_from_markup(first_match(page_markup, r'(?is)<div\b[^>]*class=["\'][^"\']*\bnews-author\b[^"\']*["\'][^>]*>(.*?)</div>'))
    author, active_date = parse_author_line(author_line)
    article_text = text_from_markup(article_html)
    summary = summarize_text(article_text)
    images = extract_images(article_html, view_url)
    links = extract_links(article_html, view_url)
    rendered_metadata = {
        "url": view_url,
        "itemId": item_id,
        "headline": headline,
        "author": author,
        "activeDate": active_date.isoformat() if active_date else None,
        "articleText": article_text,
        "articleImages": images,
        "articleLinks": links,
    }
    manager_row = {
        "activeDate": active_date.isoformat() if active_date else "",
        "author": author,
        "category": "News",
        "configureUrl": "",
        "editTextUrl": "",
        "expires": "",
        "headline": headline,
        "itemId": str(item_id),
        "sourcePage": "public-news-list",
        "status": "Active",
        "subgroup": "",
        "viewUrl": view_url,
        "visible": "To Everyone",
    }
    return {
        "item_id": item_id,
        "headline": headline,
        "author": author,
        "category": "News",
        "status": "Active",
        "visibility": "To Everyone",
        "active_date": active_date,
        "expires_date": None,
        "summary": summary,
        "view_url": view_url,
        "article_html": article_html,
        "container_html": container_html,
        "article_text": article_text,
        "images": images,
        "links": links,
        "rendered_metadata": rendered_metadata,
        "manager_row": manager_row,
    }


def upload_public_article(container_client: Any, blob_prefix: str, article: dict[str, Any]) -> dict[str, Any]:
    """Upload captured public article artifacts and image assets."""
    item_prefix = clean_blob_path(f"{blob_prefix}/{article['item_id']}")
    article_html = str(article["article_html"])
    container_html = str(article["container_html"])
    rendered_metadata_json = json_dumps(article["rendered_metadata"])
    manager_row_json = json_dumps(article["manager_row"])
    hashes = article.get("hashes") or compute_public_article_hashes(article)
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
    }

    assets = []
    for index, image in enumerate(article["images"], start=1):
        assets.append(capture_and_upload_asset(container_client, item_prefix, image, index))

    asset_manifest = {
        "capturedAt": datetime.now(timezone.utc).isoformat(),
        "articleItemId": article["item_id"],
        "assets": assets,
    }
    paths["asset_manifest"] = upload_text_blob(
        container_client,
        f"{item_prefix}/asset-manifest.json",
        json_dumps(asset_manifest),
        "application/json",
    )
    return {
        "paths": paths,
        "assets": assets,
        "asset_manifest": asset_manifest,
        "rendered_article_sha256": hashes["rendered_article_sha256"],
        "rendered_container_sha256": hashes["rendered_container_sha256"],
        "content_sha256": hashes["content_sha256"],
        "blob_prefix": item_prefix,
    }


def capture_and_upload_asset(container_client: Any, item_prefix: str, image: dict[str, Any], index: int) -> dict[str, Any]:
    """Fetch and upload one image asset."""
    source_url = str(image.get("src") or "")
    file_name = file_name_for_asset(source_url, index)
    blob_path = f"{item_prefix}/assets/{file_name}"
    try:
        content, content_type = fetch_bytes(source_url, 30.0)
        sha = sha256_bytes(content)
        upload_bytes_blob(container_client, blob_path, content, content_type or guess_content_type(file_name))
        return {
            **image,
            "sourceUrl": source_url,
            "blobPath": blob_path,
            "fileName": file_name,
            "contentType": content_type or guess_content_type(file_name),
            "byteLength": len(content),
            "sha256": sha,
            "captureStatus": "captured",
            "assetClass": "managed" if is_managed_clubexpress_asset(source_url) else "external",
            "error": "",
        }
    except Exception as exc:
        logging.warning("Failed to capture article image %s: %s", source_url, exc)
        return {
            **image,
            "sourceUrl": source_url,
            "blobPath": "",
            "fileName": file_name,
            "contentType": "",
            "byteLength": None,
            "sha256": "",
            "captureStatus": "failed",
            "assetClass": "managed" if is_managed_clubexpress_asset(source_url) else "external",
            "error": str(exc),
        }


def compute_public_article_hashes(article: dict[str, Any]) -> dict[str, str]:
    """Compute public article hashes used for update detection and storage."""
    article_html = str(article["article_html"])
    container_html = str(article["container_html"])
    rendered_metadata_json = json_dumps(article["rendered_metadata"])
    return {
        "rendered_article_sha256": sha256_text(article_html),
        "rendered_container_sha256": sha256_text(container_html),
        "content_sha256": sha256_text("|".join([article_html, container_html, rendered_metadata_json])),
    }


def classify_public_article_change(latest: dict[str, Any] | None, hashes: dict[str, str]) -> str | None:
    """Return a change reason, or None when the latest snapshot already matches."""
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


def create_archive_run(
    adapter: Any,
    run_key: str,
    config: PublicNewsSyncConfig,
    container_client: Any,
    blob_prefix: str,
    candidates: list[dict[str, Any]],
    changed_items: list[dict[str, Any]],
    unchanged_articles: list[dict[str, Any]],
    failed_articles: list[dict[str, Any]],
) -> int:
    """Create a scheduled archive run row."""
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
                    "candidate_count": len(candidates),
                    "new_article_count": sum(1 for item in changed_items if item["change_reason"] == "new"),
                    "updated_article_count": sum(1 for item in changed_items if item["change_reason"] == "updated"),
                    "changed_article_count": len(changed_items),
                    "unchanged_article_count": len(unchanged_articles),
                    "failed_article_count": len(failed_articles),
                    "candidate_item_ids": [item["item_id"] for item in candidates],
                    "changed_item_ids": [item["captured"]["item_id"] for item in changed_items],
                    "unchanged_item_ids": [item["item_id"] for item in unchanged_articles],
                }
            ),
        ),
    )
    return int(rows[0]["News_Archive_Run_ID"])


def update_archive_run(
    adapter: Any,
    run_id: int,
    *,
    status: str,
    article_count: int,
    asset_count: int,
    summary: dict[str, Any],
    error_message: str | None,
) -> None:
    """Mark an archive run complete."""
    adapter.query_rows(
        """
        SET NOCOUNT ON;
        UPDATE [clubexpress].[news_archive_runs]
        SET
            [Completed_At] = SYSUTCDATETIME(),
            [Status] = ?,
            [Article_Count] = ?,
            [Asset_Count] = ?,
            [Summary_Json] = ?,
            [Error_Message] = ?
        WHERE [News_Archive_Run_ID] = ?;
        SELECT CAST(? AS bigint) AS [News_Archive_Run_ID];
        """,
        (status, article_count, asset_count, json_dumps(summary), error_message, run_id, run_id),
    )


def get_latest_snapshot_state(adapter: Any, club_id: int, item_ids: list[int]) -> dict[int, dict[str, Any]]:
    """Return the latest archived snapshot state for each item ID."""
    if not item_ids:
        return {}
    placeholders = ", ".join("?" for _ in item_ids)
    rows = adapter.query_rows(
        f"""
        WITH LatestSnapshots AS
        (
            SELECT
                a.[Item_ID],
                a.[News_Article_ID],
                s.[News_Article_Snapshot_ID],
                s.[Content_Sha256],
                s.[Rendered_Article_Sha256],
                s.[Rendered_Container_Sha256],
                s.[Created_At],
                ROW_NUMBER() OVER
                (
                    PARTITION BY a.[Item_ID]
                    ORDER BY s.[Created_At] DESC, s.[News_Article_Snapshot_ID] DESC
                ) AS [Snapshot_Rank]
            FROM [clubexpress].[news_articles] AS a
            LEFT JOIN [clubexpress].[news_article_snapshots] AS s
                ON s.[News_Article_ID] = a.[News_Article_ID]
            WHERE a.[Club_ID] = ?
              AND a.[Item_ID] IN ({placeholders})
        )
        SELECT
            [Item_ID],
            [News_Article_ID],
            [News_Article_Snapshot_ID],
            [Content_Sha256],
            [Rendered_Article_Sha256],
            [Rendered_Container_Sha256],
            [Created_At]
        FROM LatestSnapshots
        WHERE [Snapshot_Rank] = 1;
        """,
        tuple([club_id, *item_ids]),
    )
    return {int(row["Item_ID"]): row for row in rows}


def upsert_news_article(adapter: Any, run_id: int, article: dict[str, Any]) -> int:
    """Insert or update a news article shell and return its archive ID."""
    existing = adapter.query_rows(
        """
        SELECT [News_Article_ID]
        FROM [clubexpress].[news_articles]
        WHERE [Club_ID] = ?
          AND [Item_ID] = ?;
        """,
        (CLUB_ID, article["item_id"]),
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
            CLUB_ID,
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


def insert_news_snapshot(
    adapter: Any,
    run_id: int,
    run_key: str,
    news_article_id: int,
    article: dict[str, Any],
    uploaded: dict[str, Any],
) -> int:
    """Insert one public article snapshot."""
    asset_manifest_json = json_dumps(uploaded["asset_manifest"])
    rendered_metadata_json = json_dumps(article["rendered_metadata"])
    manager_row_json = json_dumps(article["manager_row"])
    search_text = normalize_whitespace(
        "\n".join(
            str(value or "")
            for value in (article["headline"], article["author"], article["summary"], article["article_text"])
        )
    )
    captured_asset_count = sum(1 for asset in uploaded["assets"] if asset.get("captureStatus") == "captured")
    failed_asset_count = sum(1 for asset in uploaded["assets"] if asset.get("captureStatus") == "failed")
    rows = adapter.query_rows(
        """
        INSERT INTO [clubexpress].[news_article_snapshots]
        (
            [News_Article_ID],
            [News_Archive_Run_ID],
            [Snapshot_Key],
            [Item_ID],
            [Headline],
            [Author],
            [Category],
            [Status],
            [Visibility],
            [Active_Date],
            [Expires_Date],
            [Summary],
            [Search_Text],
            [Rendered_Text_Length],
            [Rendered_Article_Image_Count],
            [Rendered_Article_Link_Count],
            [Managed_Image_Count],
            [External_Image_Count],
            [Asset_Captured_Count],
            [Asset_Failure_Count],
            [Manager_Row_Json],
            [Rendered_Metadata_Json],
            [Asset_Manifest_Json],
            [Blob_Prefix],
            [Manager_Row_Blob_Path],
            [Rendered_Article_Blob_Path],
            [Rendered_Container_Blob_Path],
            [Asset_Manifest_Blob_Path],
            [Rendered_Article_Sha256],
            [Rendered_Container_Sha256],
            [Content_Sha256]
        )
        OUTPUT INSERTED.[News_Article_Snapshot_ID]
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            news_article_id,
            run_id,
            f"{run_key}:{article['item_id']}",
            article["item_id"],
            article["headline"],
            article["author"],
            article["category"],
            article["status"],
            article["visibility"],
            article["active_date"],
            article["expires_date"],
            article["summary"],
            search_text,
            len(article["article_text"]),
            len(article["images"]),
            len(article["links"]),
            sum(1 for image in article["images"] if is_managed_clubexpress_asset(str(image.get("src") or ""))),
            sum(1 for image in article["images"] if not is_managed_clubexpress_asset(str(image.get("src") or ""))),
            captured_asset_count,
            failed_asset_count,
            manager_row_json,
            rendered_metadata_json,
            asset_manifest_json,
            uploaded["blob_prefix"],
            uploaded["paths"]["manager_row"],
            uploaded["paths"]["rendered_article"],
            uploaded["paths"]["rendered_container"],
            uploaded["paths"]["asset_manifest"],
            uploaded["rendered_article_sha256"],
            uploaded["rendered_container_sha256"],
            uploaded["content_sha256"],
        ),
    )
    return int(rows[0]["News_Article_Snapshot_ID"])


def insert_news_assets(adapter: Any, snapshot_id: int, article_id: int, assets: list[dict[str, Any]]) -> int:
    """Insert asset rows for one snapshot."""
    for asset in assets:
        source_url = str(asset.get("sourceUrl") or asset.get("src") or "")
        adapter.query_rows(
            """
            SET NOCOUNT ON;
            INSERT INTO [clubexpress].[news_article_assets]
            (
                [News_Article_Snapshot_ID],
                [News_Article_ID],
                [Source_Url],
                [Source_Url_Sha256],
                [Asset_Class],
                [Capture_Status],
                [Blob_Path],
                [File_Name],
                [Content_Type],
                [Byte_Length],
                [Sha256],
                [Width],
                [Height],
                [Alt_Text],
                [Title_Text],
                [Error_Message]
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            SELECT CAST(SCOPE_IDENTITY() AS bigint) AS [News_Article_Asset_ID];
            """,
            (
                snapshot_id,
                article_id,
                source_url,
                sha256_text(source_url),
                asset.get("assetClass") or "unknown",
                asset.get("captureStatus") or "failed",
                asset.get("blobPath") or None,
                asset.get("fileName") or None,
                asset.get("contentType") or None,
                asset.get("byteLength"),
                asset.get("sha256") or None,
                parse_optional_int(asset.get("width")),
                parse_optional_int(asset.get("height")),
                asset.get("alt") or None,
                asset.get("title") or None,
                asset.get("error") or None,
            ),
        )
    return len(assets)


def assign_snapshot_thumbnail_from_captured_images(adapter: Any, snapshot_id: int) -> None:
    """Set a snapshot thumbnail from already captured article images."""
    adapter.query_rows(
        """
        SET NOCOUNT ON;
        ;WITH CapturedImageCandidates AS
        (
            SELECT
                asset.[News_Article_Asset_ID],
                ROW_NUMBER() OVER
                (
                    ORDER BY
                        CASE
                            WHEN asset.[Asset_Class] = N'managed' THEN 0
                            WHEN asset.[Asset_Class] = N'external' THEN 1
                            WHEN asset.[Asset_Class] = N'legacy_external' THEN 2
                            ELSE 3
                        END,
                        CASE
                            WHEN COALESCE(asset.[Width], 0) >= 120
                             AND COALESCE(asset.[Height], 0) >= 90
                            THEN 0
                            ELSE 1
                        END,
                        asset.[News_Article_Asset_ID]
                ) AS [Candidate_Rank]
            FROM [clubexpress].[news_article_assets] AS asset
            WHERE asset.[News_Article_Snapshot_ID] = ?
              AND asset.[Capture_Status] = N'captured'
              AND asset.[Blob_Path] IS NOT NULL
              AND
              (
                  asset.[Content_Type] LIKE N'image/%'
                  OR asset.[File_Name] LIKE N'%.jpg'
                  OR asset.[File_Name] LIKE N'%.jpeg'
                  OR asset.[File_Name] LIKE N'%.png'
                  OR asset.[File_Name] LIKE N'%.gif'
                  OR asset.[File_Name] LIKE N'%.webp'
              )
        )
        UPDATE snapshot
        SET
            [Thumbnail_Asset_ID] = candidate.[News_Article_Asset_ID],
            [Thumbnail_Source] = N'captured_article_image',
            [Thumbnail_Selected_At] = SYSUTCDATETIME()
        FROM [clubexpress].[news_article_snapshots] AS snapshot
        INNER JOIN CapturedImageCandidates AS candidate
            ON candidate.[Candidate_Rank] = 1
        WHERE snapshot.[News_Article_Snapshot_ID] = ?
          AND snapshot.[Thumbnail_Asset_ID] IS NULL;
        SELECT 1 AS [Ok];
        """,
        (snapshot_id, snapshot_id),
    )


def insert_news_links(adapter: Any, snapshot_id: int, article_id: int, links: list[dict[str, Any]]) -> None:
    """Insert rendered article links."""
    seen = set()
    for link in links:
        href = str(link.get("href") or "")
        if not href or href in seen:
            continue
        seen.add(href)
        adapter.query_rows(
            """
            SET NOCOUNT ON;
            INSERT INTO [clubexpress].[news_article_links]
            (
                [News_Article_Snapshot_ID],
                [News_Article_ID],
                [Href],
                [Link_Text],
                [Link_Source]
            )
            VALUES (?, ?, ?, ?, N'rendered_article');
            SELECT CAST(SCOPE_IDENTITY() AS bigint) AS [News_Article_Link_ID];
            """,
            (snapshot_id, article_id, href, link.get("text") or None),
        )


def fetch_text(url: str, timeout_seconds: float) -> str:
    """Fetch URL text."""
    content, _ = fetch_bytes(url, timeout_seconds)
    return content.decode("utf-8", errors="replace")


def fetch_bytes(url: str, timeout_seconds: float) -> tuple[bytes, str | None]:
    """Fetch URL bytes and content type."""
    req = request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            content_type = response.headers.get_content_type() if response.headers else None
            return response.read(), content_type
    except error.HTTPError:
        raise
    except error.URLError:
        raise


def upload_text_blob(container_client: Any, blob_path: str, text: str, content_type: str) -> str:
    """Upload text to blob storage."""
    content = text.encode("utf-8")
    upload_bytes_blob(container_client, blob_path, content, content_type + "; charset=utf-8")
    return blob_path


def upload_bytes_blob(container_client: Any, blob_path: str, content: bytes, content_type: str) -> str:
    """Upload bytes to blob storage."""
    kwargs: dict[str, Any] = {
        "name": blob_path,
        "data": content,
        "overwrite": True,
        "metadata": {"sha256": sha256_bytes(content)},
    }
    if ContentSettings is not None:
        kwargs["content_settings"] = ContentSettings(content_type=content_type)
    container_client.upload_blob(**kwargs)
    return blob_path


def extract_div_by_class(markup: str, class_name: str) -> str:
    """Extract inner HTML for the first div containing a class."""
    start_match = re.search(
        rf'(?is)<div\b(?=[^>]*\bclass\s*=\s*["\'][^"\']*\b{re.escape(class_name)}\b[^"\']*["\'])[^>]*>',
        markup,
    )
    if not start_match:
        return ""
    depth = 0
    inner_start = start_match.end()
    for match in re.finditer(r"(?is)</?div\b[^>]*>", markup[start_match.start() :]):
        absolute_start = start_match.start() + match.start()
        tag = match.group(0)
        if tag.startswith("</"):
            depth -= 1
            if depth == 0:
                return markup[inner_start:absolute_start].strip()
        else:
            depth += 1
    return ""


def first_match(markup: str, pattern: str) -> str:
    """Return the first regex group match."""
    match = re.search(pattern, markup)
    return match.group(1) if match else ""


def extract_images(markup: str, base_url: str) -> list[dict[str, Any]]:
    """Extract image metadata from article HTML."""
    images = []
    for tag_match in re.finditer(r"(?is)<img\b[^>]*>", markup):
        tag = tag_match.group(0)
        src = normalize_url(attr_value(tag, "src"), base_url)
        if not src:
            continue
        images.append(
            {
                "src": src,
                "alt": html.unescape(attr_value(tag, "alt")),
                "title": html.unescape(attr_value(tag, "title")),
                "width": attr_value(tag, "width"),
                "height": attr_value(tag, "height"),
            }
        )
    return images


def extract_links(markup: str, base_url: str) -> list[dict[str, Any]]:
    """Extract links from article HTML."""
    links = []
    for match in re.finditer(r"(?is)<a\b[^>]*href\s*=\s*(['\"])(.*?)\1[^>]*>(.*?)</a>", markup):
        href = normalize_url(html.unescape(match.group(2)), base_url)
        if not href:
            continue
        links.append({"href": href, "text": text_from_markup(match.group(3))[:1024]})
    return links


def attr_value(tag: str, name: str) -> str:
    """Read one HTML attribute value."""
    match = re.search(rf"(?is)\b{re.escape(name)}\s*=\s*(['\"])(.*?)\1", tag)
    return html.unescape(match.group(2)) if match else ""


def normalize_url(value: str, base_url: str) -> str:
    """Normalize relative and protocol-relative URLs."""
    text = (value or "").strip()
    if not text:
        return ""
    if text.startswith("//"):
        return "https:" + text
    return parse.urljoin(base_url, text)


def parse_author_line(value: str) -> tuple[str, date | None]:
    """Parse ClubExpress author/date line."""
    text = normalize_whitespace(value)
    date_match = re.search(r"Published on\s+(\d{1,2})/(\d{1,2})/(\d{4})", text, flags=re.I)
    active_date = None
    if date_match:
        active_date = date(int(date_match.group(3)), int(date_match.group(1)), int(date_match.group(2)))
    author = re.sub(r"\|\s*Published on\s+\d{1,2}/\d{1,2}/\d{4}.*$", "", text, flags=re.I).strip()
    return author, active_date


def text_from_markup(markup: str) -> str:
    """Convert HTML markup to text."""
    without_scripts = re.sub(r"(?is)<(script|style)\b[^>]*>.*?</\1>", " ", markup or "")
    text = re.sub(r"(?is)<br\s*/?>", "\n", without_scripts)
    text = re.sub(r"(?is)</p\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    return normalize_whitespace(html.unescape(text))


def summarize_text(value: str, limit: int = 600) -> str:
    """Return a compact summary from rendered text."""
    text = normalize_whitespace(value)
    if len(text) <= limit:
        return text
    return text[:limit].rsplit(" ", 1)[0] + "..."


def normalize_whitespace(value: str) -> str:
    """Normalize whitespace."""
    return re.sub(r"\s+", " ", value or "").strip()


def is_managed_clubexpress_asset(url: str) -> bool:
    """Return whether URL points at ClubExpress-managed graphics storage."""
    return bool(
        re.search(r"^https://images\.clubexpress\.com/454497/graphics/", url, flags=re.I)
        or re.search(r"^https://s3\.amazonaws\.com/ClubExpressClubFiles/454497/graphics/", url, flags=re.I)
    )


def file_name_for_asset(url: str, index: int) -> str:
    """Return a stable filename for one asset."""
    parsed = parse.urlparse(url)
    name = parsed.path.rsplit("/", 1)[-1] or f"asset-{index}"
    name = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip(".-")
    if not name:
        name = f"asset-{index}"
    if "." not in name:
        extension = mimetypes.guess_extension(guess_content_type(name)) or ".bin"
        name += extension
    return f"{index:03d}-{name}"


def guess_content_type(file_name: str) -> str:
    """Guess content type."""
    return mimetypes.guess_type(file_name)[0] or "application/octet-stream"


def clean_blob_path(path_text: str) -> str:
    """Normalize blob path."""
    return path_text.replace("\\", "/").strip("/")


def json_dumps(value: Any) -> str:
    """Dump JSON consistently."""
    return json.dumps(value, default=json_safe_value, ensure_ascii=False, sort_keys=True)


def json_safe_value(value: Any) -> Any:
    """Return JSON-safe values."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def sha256_text(value: str) -> str:
    """Hash text."""
    return sha256_bytes(value.encode("utf-8"))


def sha256_bytes(value: bytes) -> str:
    """Hash bytes."""
    return hashlib.sha256(value).hexdigest()


def normalize_hash(value: Any) -> str:
    """Normalize stored hash text."""
    return str(value or "").strip().lower()


def parse_optional_int(value: Any) -> int | None:
    """Parse optional integer."""
    text = str(value or "").strip()
    if not text.isdigit():
        return None
    return int(text)
