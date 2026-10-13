# Copyright 2026, American Go Association, All rights reserved

"""Public read-only viewer for archived ClubExpress news articles."""

from __future__ import annotations

import html
import json
import logging
import mimetypes
import os
import re
from contextlib import suppress
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

import azure.functions as func
from azure.storage.blob import BlobServiceClient

from news_archive_external_sync import ExternalNewsSyncConfig, sync_external_go_news_archive
from news_archive_player_context_sync import NewsArchivePlayerContextSyncConfig, sync_news_archive_player_context
from news_archive_public_sync import PublicNewsSyncConfig, sync_public_news_archive

try:
    import certifi
except Exception:
    certifi = None

try:
    import pyodbc
except Exception:
    pyodbc = None

try:
    import pytds
except Exception:
    pytds = None


app = func.FunctionApp()

DEFAULT_CONTAINER = "clubexpress-news-archive"
DEFAULT_PAGE_SIZE = 7
DEFAULT_HOME_CARD_COUNT = 70
DEFAULT_PUBLIC_SYNC_SCHEDULE = os.environ.get("NEWS_ARCHIVE_PUBLIC_SYNC_SCHEDULE", "0 20 * * * *")
DEFAULT_EXTERNAL_SYNC_SCHEDULE = os.environ.get("NEWS_ARCHIVE_EXTERNAL_SYNC_SCHEDULE", "0 35 10 * * *")
DEFAULT_PLAYER_CONTEXT_SYNC_SCHEDULE = os.environ.get("NEWS_ARCHIVE_PLAYER_CONTEXT_SYNC_SCHEDULE", "0 30 * * * *")
CLUBEXPRESS_FRAME_ANCESTORS = "'self' https://usgo.org https://www.usgo.org https://aga1.clubexpress.com"
EXTERNAL_DIGEST_CATEGORY = "External Go News Digest"
DIGEST_CARD_IMAGE_FILENAME = "international-go-news-digest-card-v7-1200x675.jpg"
DEFAULT_CARD_IMAGE_FILENAME = "aga-news-card-default-v2-1200x675.jpg"
ARTICLE_LIST_BASE_CTE = """
WITH RankedSnapshots AS
(
    SELECT
        s.*,
        ROW_NUMBER() OVER
        (
            PARTITION BY s.[News_Article_ID]
            ORDER BY s.[Created_At] DESC, s.[News_Article_Snapshot_ID] DESC
        ) AS [Snapshot_Rank]
    FROM [clubexpress].[news_article_snapshots] AS s
)
"""


LIST_ARTICLES_TEMPLATE = ARTICLE_LIST_BASE_CTE + """
SELECT
    s.[News_Article_Snapshot_ID],
    a.[News_Article_ID],
    a.[Item_ID],
    COALESCE(s.[Headline], a.[Headline]) AS [Headline],
    COALESCE(s.[Author], a.[Author]) AS [Author],
    COALESCE(s.[Category], a.[Category]) AS [Category],
    COALESCE(s.[Status], a.[Current_Status]) AS [Status],
    COALESCE(s.[Visibility], a.[Visibility]) AS [Visibility],
    COALESCE(s.[Active_Date], a.[Active_Date]) AS [Active_Date],
    s.[Summary],
    s.[Rendered_Text_Length],
    s.[Rendered_Article_Image_Count],
    s.[Rendered_Article_Link_Count],
    s.[Asset_Captured_Count],
    s.[Blob_Prefix],
    thumbnail.[News_Article_Asset_ID] AS [Thumbnail_Asset_ID],
    s.[Thumbnail_Source],
    thumbnail.[Alt_Text] AS [Thumbnail_Alt_Text],
    thumbnail.[Title_Text] AS [Thumbnail_Title_Text],
    thumbnail.[Width] AS [Thumbnail_Width],
    thumbnail.[Height] AS [Thumbnail_Height],
    r.[Run_Key]
FROM RankedSnapshots AS s
INNER JOIN [clubexpress].[news_articles] AS a
    ON a.[News_Article_ID] = s.[News_Article_ID]
INNER JOIN [clubexpress].[news_archive_runs] AS r
    ON r.[News_Archive_Run_ID] = s.[News_Archive_Run_ID]
OUTER APPLY
(
    SELECT TOP (1)
        asset.[News_Article_Asset_ID],
        asset.[Alt_Text],
        asset.[Title_Text],
        asset.[Width],
        asset.[Height]
    FROM [clubexpress].[news_article_assets] AS asset
    WHERE asset.[News_Article_Snapshot_ID] = s.[News_Article_Snapshot_ID]
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
    ORDER BY
        CASE WHEN asset.[News_Article_Asset_ID] = s.[Thumbnail_Asset_ID] THEN 0 ELSE 1 END,
        CASE WHEN asset.[Asset_Class] = N'managed' THEN 0 ELSE 1 END,
        asset.[News_Article_Asset_ID]
) AS thumbnail
WHERE s.[Snapshot_Rank] = 1
{public_where}
{filter_where}
ORDER BY COALESCE(s.[Active_Date], a.[Active_Date]) DESC, a.[Item_ID] DESC
OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;
"""


HOME_CARDS_SQL = ARTICLE_LIST_BASE_CTE + """
SELECT TOP (?)
    s.[News_Article_Snapshot_ID],
    a.[News_Article_ID],
    a.[Item_ID],
    COALESCE(s.[Headline], a.[Headline]) AS [Headline],
    COALESCE(s.[Author], a.[Author]) AS [Author],
    COALESCE(s.[Category], a.[Category]) AS [Category],
    COALESCE(s.[Active_Date], a.[Active_Date]) AS [Active_Date],
    s.[Summary],
    s.[Rendered_Article_Image_Count],
    s.[Asset_Captured_Count],
    thumbnail.[News_Article_Asset_ID] AS [Thumbnail_Asset_ID],
    s.[Thumbnail_Source],
    thumbnail.[Alt_Text] AS [Thumbnail_Alt_Text],
    thumbnail.[Title_Text] AS [Thumbnail_Title_Text],
    thumbnail.[Width] AS [Thumbnail_Width],
    thumbnail.[Height] AS [Thumbnail_Height],
    r.[Run_Key]
FROM RankedSnapshots AS s
INNER JOIN [clubexpress].[news_articles] AS a
    ON a.[News_Article_ID] = s.[News_Article_ID]
INNER JOIN [clubexpress].[news_archive_runs] AS r
    ON r.[News_Archive_Run_ID] = s.[News_Archive_Run_ID]
OUTER APPLY
(
    SELECT TOP (1)
        asset.[News_Article_Asset_ID],
        asset.[Alt_Text],
        asset.[Title_Text],
        asset.[Width],
        asset.[Height]
    FROM [clubexpress].[news_article_assets] AS asset
    WHERE asset.[News_Article_Snapshot_ID] = s.[News_Article_Snapshot_ID]
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
    ORDER BY
        CASE WHEN asset.[News_Article_Asset_ID] = s.[Thumbnail_Asset_ID] THEN 0 ELSE 1 END,
        CASE WHEN asset.[Asset_Class] = N'managed' THEN 0 ELSE 1 END,
        asset.[News_Article_Asset_ID]
) AS thumbnail
WHERE s.[Snapshot_Rank] = 1
{public_where}
ORDER BY COALESCE(s.[Active_Date], a.[Active_Date]) DESC, a.[Item_ID] DESC;
"""


COUNT_ARTICLES_TEMPLATE = ARTICLE_LIST_BASE_CTE + """
SELECT COUNT_BIG(1) AS [Total_Count]
FROM RankedSnapshots AS s
INNER JOIN [clubexpress].[news_articles] AS a
    ON a.[News_Article_ID] = s.[News_Article_ID]
INNER JOIN [clubexpress].[news_archive_runs] AS r
    ON r.[News_Archive_Run_ID] = s.[News_Archive_Run_ID]
WHERE s.[Snapshot_Rank] = 1
{public_where}
{filter_where};
"""


ANCHOR_ARTICLE_OFFSET_TEMPLATE = ARTICLE_LIST_BASE_CTE + """
SELECT CAST([Article_Row] - 1 AS int) AS [Article_Offset]
FROM
(
    SELECT
        s.[News_Article_Snapshot_ID],
        ROW_NUMBER() OVER
        (
            ORDER BY COALESCE(s.[Active_Date], a.[Active_Date]) DESC, a.[Item_ID] DESC
        ) AS [Article_Row]
    FROM RankedSnapshots AS s
    INNER JOIN [clubexpress].[news_articles] AS a
        ON a.[News_Article_ID] = s.[News_Article_ID]
    INNER JOIN [clubexpress].[news_archive_runs] AS r
        ON r.[News_Archive_Run_ID] = s.[News_Archive_Run_ID]
    WHERE s.[Snapshot_Rank] = 1
    {public_where}
    {filter_where}
) AS OrderedArticles
WHERE OrderedArticles.[News_Article_Snapshot_ID] = ?;
"""


GET_SNAPSHOT_TEMPLATE = """
SELECT
    s.[News_Article_Snapshot_ID],
    a.[News_Article_ID],
    a.[Item_ID],
    COALESCE(s.[Headline], a.[Headline]) AS [Headline],
    COALESCE(s.[Author], a.[Author]) AS [Author],
    COALESCE(s.[Category], a.[Category]) AS [Category],
    COALESCE(s.[Status], a.[Current_Status]) AS [Status],
    COALESCE(s.[Visibility], a.[Visibility]) AS [Visibility],
    COALESCE(s.[Active_Date], a.[Active_Date]) AS [Active_Date],
    s.[Summary],
    a.[View_Url],
    s.[Rendered_Text_Length],
    s.[Rendered_Article_Image_Count],
    s.[Rendered_Article_Link_Count],
    s.[Asset_Captured_Count],
    s.[Rendered_Article_Blob_Path],
    s.[Rendered_Container_Blob_Path],
    s.[Asset_Manifest_Blob_Path],
    s.[Blob_Prefix],
    s.[Thumbnail_Asset_ID],
    s.[Thumbnail_Source],
    r.[Run_Key],
    r.[Blob_Container]
FROM [clubexpress].[news_article_snapshots] AS s
INNER JOIN [clubexpress].[news_articles] AS a
    ON a.[News_Article_ID] = s.[News_Article_ID]
INNER JOIN [clubexpress].[news_archive_runs] AS r
    ON r.[News_Archive_Run_ID] = s.[News_Archive_Run_ID]
WHERE s.[News_Article_Snapshot_ID] = ?
{public_where};
"""


GET_ASSETS_SQL = """
SELECT
    [News_Article_Asset_ID],
    [Source_Url],
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
    [Title_Text]
FROM [clubexpress].[news_article_assets]
WHERE [News_Article_Snapshot_ID] = ?
ORDER BY [News_Article_Asset_ID];
"""


GET_ASSET_TEMPLATE = """
SELECT
    asset.[News_Article_Asset_ID],
    asset.[Blob_Path],
    asset.[File_Name],
    asset.[Content_Type]
FROM [clubexpress].[news_article_assets] AS asset
INNER JOIN [clubexpress].[news_article_snapshots] AS s
    ON s.[News_Article_Snapshot_ID] = asset.[News_Article_Snapshot_ID]
INNER JOIN [clubexpress].[news_articles] AS a
    ON a.[News_Article_ID] = s.[News_Article_ID]
WHERE asset.[News_Article_Asset_ID] = ?
{public_where};
"""


PUBLIC_ACTIVE_WHERE = "  AND COALESCE(s.[Status], a.[Current_Status]) = N'Active'"
PUBLIC_VISIBILITY_WHERE = "  AND COALESCE(s.[Visibility], a.[Visibility], N'') IN (N'To Everyone', N'')"
PUBLIC_ACTIVE_DATE_WHERE = "  AND COALESCE(s.[Active_Date], a.[Active_Date]) <= CAST(SYSUTCDATETIME() AS date)"


class SqlAdapter:
    """Small SQL adapter with pyodbc first and python-tds fallback."""

    def __init__(self, connection_string: str) -> None:
        """Initialize the adapter."""
        self.connection_string = connection_string

    def query_rows(self, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        """Query SQL rows."""
        if pyodbc is not None:
            try:
                return _query_rows_via_odbc(self.connection_string, query, params)
            except Exception:
                if pytds is None:
                    raise
        return _query_rows_via_tds(self.connection_string, query, params)


_sql_adapter: SqlAdapter | None = None
_blob_container = None


@app.function_name(name="NewsArchiveViewerPage")
@app.route(route="news-archive", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def news_archive_page(req: func.HttpRequest) -> func.HttpResponse:
    """Serve the news archive viewer page."""
    return _html_response(_render_viewer_page(req), frame_ancestors=CLUBEXPRESS_FRAME_ANCESTORS)


@app.function_name(name="NewsArchiveViewerArticles")
@app.route(route="news-archive/articles", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def news_archive_articles(req: func.HttpRequest) -> func.HttpResponse:
    """Return archived article list."""
    adapter, error_response = _adapter_or_response()
    if error_response is not None:
        return error_response
    page, parse_response = _viewer_page(req)
    if parse_response is not None:
        return parse_response
    filter_where, filter_params, parse_response = _article_filter_where_and_params(req)
    if parse_response is not None:
        return parse_response
    page_size = _viewer_page_size()
    offset, parse_response = _viewer_offset(req, page, page_size)
    if parse_response is not None:
        return parse_response
    anchor_snapshot_id, parse_response = _viewer_anchor_snapshot_id(req)
    if parse_response is not None:
        return parse_response
    access_options = _viewer_access_options(req)
    public_where = _public_where(access_options)
    query_parts = {"public_where": public_where, "filter_where": filter_where}
    count_rows = adapter.query_rows(COUNT_ARTICLES_TEMPLATE.format(**query_parts), filter_params)
    total_count = int(count_rows[0].get("Total_Count") or 0) if count_rows else 0
    anchor_found = False
    if anchor_snapshot_id is not None:
        anchor_rows = adapter.query_rows(
            ANCHOR_ARTICLE_OFFSET_TEMPLATE.format(**query_parts),
            filter_params + (anchor_snapshot_id,),
        )
        if anchor_rows:
            offset = int(anchor_rows[0].get("Article_Offset") or 0)
            anchor_found = True
    offset = max(0, min(offset, max(0, total_count - 1))) if total_count else 0
    rows = adapter.query_rows(
        LIST_ARTICLES_TEMPLATE.format(**query_parts),
        filter_params + (offset, page_size),
    )
    return _json_response(
        {
            "articles": [_json_safe_row(row) for row in rows],
            "page": offset // page_size,
            "offset": offset,
            "pageSize": page_size,
            "totalCount": total_count,
            "hasPrevious": offset > 0,
            "hasNext": offset + page_size < total_count,
            "publicOnly": _public_only(),
            "includeInactive": access_options["includeInactive"],
            "includeNotVisible": access_options["includeNotVisible"],
            "anchorSnapshotId": anchor_snapshot_id,
            "anchorFound": anchor_found,
        }
    )


@app.function_name(name="NewsArchiveHomeCards")
@app.route(route="news-archive/home-cards", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def news_archive_home_cards(req: func.HttpRequest) -> func.HttpResponse:
    """Serve recent news cards for embedding on the ClubExpress home page."""
    adapter, error_response = _adapter_or_response()
    if error_response is not None:
        return error_response
    card_count, parse_response = _home_card_count(req)
    if parse_response is not None:
        return parse_response
    public_where = _public_where(_viewer_access_options(req))
    rows = adapter.query_rows(HOME_CARDS_SQL.format(public_where=public_where), (card_count,))
    return _html_response(
        _render_home_cards_page([_json_safe_row(row) for row in rows], req),
        frame_ancestors=CLUBEXPRESS_FRAME_ANCESTORS,
    )


@app.function_name(name="NewsArchiveDigestCardImage")
@app.route(route="news-archive/static/digest-card-image", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def news_archive_digest_card_image(req: func.HttpRequest) -> func.HttpResponse:
    """Serve the default card image for external go news digests."""
    del req
    return _local_card_image_response(DIGEST_CARD_IMAGE_FILENAME, "Digest card image not found.")


@app.function_name(name="NewsArchiveDefaultCardImage")
@app.route(route="news-archive/static/default-card-image", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def news_archive_default_card_image(req: func.HttpRequest) -> func.HttpResponse:
    """Serve the default card image for news articles without a thumbnail."""
    del req
    return _local_card_image_response(DEFAULT_CARD_IMAGE_FILENAME, "Default card image not found.")


def _local_card_image_response(filename: str, missing_message: str) -> func.HttpResponse:
    """Serve one packaged card image asset."""
    image_path = Path(__file__).resolve().parent / "assets" / filename
    if not image_path.exists():
        return _text_response(missing_message, 404)
    return func.HttpResponse(
        body=image_path.read_bytes(),
        mimetype=mimetypes.guess_type(filename)[0] or "image/jpeg",
        status_code=200,
        headers={
            "Cache-Control": "public, max-age=86400",
            "X-Content-Type-Options": "nosniff",
        },
    )


@app.function_name(name="NewsArchiveViewerArticle")
@app.route(route="news-archive/article/{snapshot_id}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def news_archive_article(req: func.HttpRequest) -> func.HttpResponse:
    """Serve one archived article."""
    snapshot_id, parse_response = _positive_int_route_param(req, "snapshot_id")
    if parse_response is not None:
        return parse_response
    loaded = _load_rendered_article(req, snapshot_id)
    if isinstance(loaded, func.HttpResponse):
        return loaded
    snapshot, assets, article_html = loaded
    article_url = _absolute_viewer_article_url(req, snapshot_id)
    copy_url = f"{_request_origin(req)}/api/news-archive/article/{snapshot_id}"
    email_html_url = f"/api/news-archive/article/{snapshot_id}/email-html"
    email_params = {
        "share": "1",
        **_viewer_access_query_params(_viewer_access_options(req)),
    }
    email_html_url = f"{email_html_url}?{urlencode(email_params)}"
    return _html_response(
        _render_article_page(
            snapshot,
            article_html,
            assets,
            article_url=article_url,
            copy_url=copy_url,
            email_html_url=email_html_url,
        ),
        frame_ancestors=CLUBEXPRESS_FRAME_ANCESTORS,
    )


@app.function_name(name="NewsArchiveViewerArticleEmailHtml")
@app.route(route="news-archive/article/{snapshot_id}/email-html", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def news_archive_article_email_html(req: func.HttpRequest) -> func.HttpResponse:
    """Return one archived article as copy-ready email HTML."""
    snapshot_id, parse_response = _positive_int_route_param(req, "snapshot_id")
    if parse_response is not None:
        return parse_response
    loaded = _load_rendered_article(req, snapshot_id, absolute_asset_urls=True)
    if isinstance(loaded, func.HttpResponse):
        return loaded
    snapshot, assets, article_html = loaded
    article_url = _absolute_viewer_article_url(req, snapshot_id)
    image_max_width = 600 if _query_flag(req, "share") else 220
    email_html = _render_email_article_fragment(
        snapshot,
        article_html,
        article_url,
        assets,
        image_max_width=image_max_width,
    )
    return _json_response(
        {
            "snapshotId": snapshot_id,
            "title": str(snapshot.get("Headline") or "Untitled"),
            "url": article_url,
            "html": email_html,
            "text": _html_to_text(email_html),
            "assetCount": len(assets),
        }
    )


def _load_rendered_article(
    req: func.HttpRequest,
    snapshot_id: int,
    *,
    absolute_asset_urls: bool = False,
) -> tuple[dict[str, Any], list[dict[str, Any]], str] | func.HttpResponse:
    """Load and sanitize one rendered article."""
    adapter, error_response = _adapter_or_response()
    if error_response is not None:
        return error_response

    access_options = _viewer_access_options(req)
    public_where = _public_where(access_options)
    rows = adapter.query_rows(GET_SNAPSHOT_TEMPLATE.format(public_where=public_where), (snapshot_id,))
    if not rows:
        return _text_response("Article not found.", 404)
    snapshot = _json_safe_row(rows[0])
    assets = [_json_safe_row(row) for row in adapter.query_rows(GET_ASSETS_SQL, (snapshot_id,))]
    blob_path = snapshot.get("Rendered_Article_Blob_Path") or snapshot.get("Rendered_Container_Blob_Path")
    if not blob_path:
        return _text_response("Article HTML blob not found.", 404)

    article_html = _blob_text(str(blob_path))
    article_html = _sanitize_article_html(article_html)
    article_html = _rewrite_external_links(article_html)
    article_html = _rewrite_article_assets(
        article_html,
        assets,
        _viewer_access_query_params(access_options),
        asset_base_url=_request_origin(req) if absolute_asset_urls else "",
    )
    return snapshot, assets, article_html


@app.function_name(name="NewsArchiveViewerAsset")
@app.route(route="news-archive/asset/{asset_id}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def news_archive_asset(req: func.HttpRequest) -> func.HttpResponse:
    """Serve one archived article asset."""
    asset_id, parse_response = _positive_int_route_param(req, "asset_id")
    if parse_response is not None:
        return parse_response
    adapter, error_response = _adapter_or_response()
    if error_response is not None:
        return error_response

    public_where = _public_where(_viewer_access_options(req))
    rows = adapter.query_rows(GET_ASSET_TEMPLATE.format(public_where=public_where), (asset_id,))
    if not rows or not rows[0].get("Blob_Path"):
        return _text_response("Asset not found.", 404)
    asset = _json_safe_row(rows[0])
    content = _blob_bytes(str(asset["Blob_Path"]))
    content_type = (
        asset.get("Content_Type")
        or mimetypes.guess_type(str(asset.get("File_Name") or ""))[0]
        or "application/octet-stream"
    )
    return func.HttpResponse(
        body=content,
        mimetype=str(content_type),
        status_code=200,
        headers={
            "Cache-Control": "private, max-age=300",
            "X-Content-Type-Options": "nosniff",
        },
    )


@app.function_name(name="NewsArchiveViewerHealth")
@app.route(route="news-archive/health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def news_archive_health(req: func.HttpRequest) -> func.HttpResponse:
    """Serve a simple health endpoint."""
    del req
    return _json_response({"ok": True})


@app.function_name(name="NewsArchivePublicSyncTimer")
@app.timer_trigger(schedule=DEFAULT_PUBLIC_SYNC_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def news_archive_public_sync_timer(timer: func.TimerRequest) -> None:
    """Archive newly published public ClubExpress news articles on a schedule."""
    if timer.past_due:
        logging.warning("News archive public sync timer is past due.")
    if not _news_archive_public_sync_enabled():
        logging.info("News archive public sync timer is disabled.")
        return
    config = _news_archive_public_sync_config()
    result = sync_public_news_archive(_get_sql_adapter(), _blob_container_client(), config)
    logging.info("News archive public sync result: %s", json.dumps(result, default=_json_safe_value))


@app.function_name(name="NewsArchiveExternalSyncTimer")
@app.timer_trigger(schedule=DEFAULT_EXTERNAL_SYNC_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def news_archive_external_sync_timer(timer: func.TimerRequest) -> None:
    """Archive the daily external Go news digest on a schedule."""
    if timer.past_due:
        logging.warning("News archive external sync timer is past due.")
    if not _news_archive_external_sync_enabled():
        logging.info("News archive external sync timer is disabled.")
        return
    config = _news_archive_external_sync_config()
    result = sync_external_go_news_archive(_get_sql_adapter(), _blob_container_client(), config)
    logging.info("News archive external sync result: %s", json.dumps(result, default=_json_safe_value))


@app.function_name(name="NewsArchivePlayerContextSyncTimer")
@app.timer_trigger(schedule=DEFAULT_PLAYER_CONTEXT_SYNC_SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def news_archive_player_context_sync_timer(timer: func.TimerRequest) -> None:
    """Refresh Ratings Explorer article context links from archived news."""
    if timer.past_due:
        logging.warning("News archive player context sync timer is past due.")
    if not _news_archive_player_context_sync_enabled():
        logging.info("News archive player context sync timer is disabled.")
        return
    config = _news_archive_player_context_sync_config()
    result = sync_news_archive_player_context(_get_sql_adapter(), config)
    logging.info("News archive player context sync result: %s", json.dumps(result, default=_json_safe_value))


def _adapter_or_response() -> tuple[SqlAdapter, None] | tuple[None, func.HttpResponse]:
    """Return SQL adapter or a configuration error response."""
    try:
        return _get_sql_adapter(), None
    except RuntimeError as exc:
        return None, _text_response(str(exc), 500)


def _get_sql_adapter() -> SqlAdapter:
    """Return cached SQL adapter."""
    global _sql_adapter
    if _sql_adapter is not None:
        return _sql_adapter
    conn = _get_sql_connection_string()
    if not conn:
        raise RuntimeError("Missing SQL_CONNECTION_STRING application setting.")
    _sql_adapter = SqlAdapter(conn)
    return _sql_adapter


def _get_sql_connection_string() -> str | None:
    """Return SQL connection string from settings or local development file."""
    conn = os.environ.get("SQL_CONNECTION_STRING") or os.environ.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
    if conn and conn.strip():
        return conn

    app_root = Path(__file__).resolve().parent
    for settings_path in (app_root / "local.settings.json", app_root.parent / "local.settings.json"):
        if not settings_path.exists():
            continue
        with suppress(OSError, json.JSONDecodeError):
            values = json.loads(settings_path.read_text(encoding="utf-8")).get("Values", {})
            conn = values.get("SQL_CONNECTION_STRING") or values.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
            if conn and str(conn).strip():
                return str(conn)
    return None


def _blob_container_client():
    """Return cached Blob container client."""
    global _blob_container
    if _blob_container is not None:
        return _blob_container
    conn = (
        os.environ.get("NEWS_ARCHIVE_STORAGE_CONNECTION_STRING")
        or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        or os.environ.get("AzureWebJobsStorage")
    )
    if not conn:
        raise RuntimeError("Missing storage connection string.")
    container_name = os.environ.get("NEWS_ARCHIVE_CONTAINER", DEFAULT_CONTAINER)
    service = BlobServiceClient.from_connection_string(conn)
    _blob_container = service.get_container_client(container_name)
    return _blob_container


def _blob_text(blob_path: str) -> str:
    """Read blob text."""
    return _blob_bytes(blob_path).decode("utf-8", errors="replace")


def _blob_bytes(blob_path: str) -> bytes:
    """Read blob bytes."""
    return _blob_container_client().download_blob(blob_path).readall()


def _query_rows_via_odbc(conn_str: str, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    """Query rows via pyodbc."""
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(query, *params)
            rows: list[dict[str, Any]] = []
            while True:
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    rows = [dict(zip(columns, record)) for record in cursor.fetchall()]
                    break
                if not cursor.nextset():
                    break
            conn.commit()
            return rows
        finally:
            cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _query_rows_via_tds(conn_str: str, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    """Query rows via python-tds."""
    conn = _tds_connect(conn_str)
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(_render_query(query, params))
            rows = list(cursor.fetchall())
            conn.commit()
            return rows
        finally:
            cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _parse_sql_connection_string(connection_string: str) -> dict[str, object]:
    """Parse an ODBC-style SQL connection string for python-tds."""
    parts: dict[str, str] = {}
    for item in connection_string.split(";"):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        parts[key.strip().lower()] = value.strip().strip("{}")

    server = parts["server"].replace("tcp:", "")
    host, port_text = (server.split(",", 1) + ["1433"])[:2]
    return {
        "server": host,
        "port": int(port_text),
        "database": parts.get("database") or parts["initial catalog"],
        "user": parts.get("uid") or parts["user id"],
        "password": parts.get("pwd") or parts["password"],
    }


def _tds_connect(conn_str: str):
    """Connect to SQL using python-tds."""
    if pytds is None:
        raise RuntimeError("Neither pyodbc nor python-tds is available for SQL access.")
    sql = _parse_sql_connection_string(conn_str)
    kwargs = {
        "server": sql["server"],
        "port": sql["port"],
        "database": sql["database"],
        "user": sql["user"],
        "password": sql["password"],
        "validate_host": True,
        "enc_login_only": False,
        "autocommit": False,
        "timeout": 60,
        "as_dict": True,
    }
    if certifi is not None:
        kwargs["cafile"] = certifi.where()
    return pytds.connect(**kwargs)


def _sql_literal(value: Any) -> str:
    """Render a SQL literal for python-tds fallback."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (datetime, date)):
        return "'" + value.isoformat() + "'"
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _render_query(query: str, params: tuple[Any, ...]) -> str:
    """Render parameterized query for python-tds fallback."""
    rendered = query
    for value in params:
        rendered = rendered.replace("?", _sql_literal(value), 1)
    return rendered


def _public_only() -> bool:
    """Return whether viewer should only expose currently public articles."""
    value = os.environ.get("NEWS_ARCHIVE_PUBLIC_ONLY", "true").strip().lower()
    return value not in {"0", "false", "no", "off"}


def _viewer_access_options(req: func.HttpRequest) -> dict[str, bool]:
    """Return hidden cleanup access flags from query parameters."""
    return {
        "includeInactive": _query_flag(req, "includeInactive", "include_inactive"),
        "includeNotVisible": _query_flag(
            req,
            "includeNotVisible",
            "include_not_visible",
            "includeHidden",
            "include_hidden",
        ),
    }


def _public_where(access_options: dict[str, bool]) -> str:
    """Build the public-safety WHERE clause for the requested cleanup scope."""
    if not _public_only():
        return ""
    clauses: list[str] = []
    if not access_options.get("includeInactive"):
        clauses.append(PUBLIC_ACTIVE_WHERE)
    if not access_options.get("includeNotVisible"):
        clauses.append(PUBLIC_VISIBILITY_WHERE)
    clauses.append(PUBLIC_ACTIVE_DATE_WHERE)
    return "\n".join(clauses)


def _viewer_access_query_params(access_options: dict[str, bool]) -> dict[str, str]:
    """Return access flags that should be forwarded to article asset URLs."""
    params: dict[str, str] = {}
    if access_options.get("includeInactive"):
        params["includeInactive"] = "1"
    if access_options.get("includeNotVisible"):
        params["includeNotVisible"] = "1"
    return params


def _query_flag(req: func.HttpRequest, *names: str) -> bool:
    """Return whether any hidden query flag is enabled."""
    for name in names:
        if name not in req.params:
            continue
        value = (req.params.get(name) or "").strip().lower()
        return value not in {"0", "false", "no", "off"}
    return False


def _news_archive_public_sync_enabled() -> bool:
    """Return whether the scheduled public news sync should run."""
    value = os.environ.get("NEWS_ARCHIVE_PUBLIC_SYNC_ENABLED", "true").strip().lower()
    return value not in {"0", "false", "no", "off"}


def _news_archive_public_sync_config() -> PublicNewsSyncConfig:
    """Build public sync configuration from app settings."""
    return PublicNewsSyncConfig(
        max_candidates=_env_int("NEWS_ARCHIVE_PUBLIC_SYNC_MAX_CANDIDATES", 10, minimum=1, maximum=50),
        request_timeout_seconds=_env_float("NEWS_ARCHIVE_PUBLIC_SYNC_REQUEST_TIMEOUT_SECONDS", 30.0, minimum=5.0),
        blob_root=os.environ.get("NEWS_ARCHIVE_PUBLIC_SYNC_BLOB_ROOT", "scheduled/public-news").strip()
        or "scheduled/public-news",
        source_description=os.environ.get(
            "NEWS_ARCHIVE_PUBLIC_SYNC_SOURCE_DESCRIPTION",
            "Scheduled public ClubExpress news-list sync.",
        ).strip()
        or "Scheduled public ClubExpress news-list sync.",
    )


def _news_archive_external_sync_enabled() -> bool:
    """Return whether the scheduled external Go news digest sync should run."""
    value = os.environ.get("NEWS_ARCHIVE_EXTERNAL_SYNC_ENABLED", "true").strip().lower()
    return value not in {"0", "false", "no", "off"}


def _news_archive_external_sync_config() -> ExternalNewsSyncConfig:
    """Build external news sync configuration from app settings."""
    return ExternalNewsSyncConfig(
        source_ids=_env_csv_tuple("NEWS_ARCHIVE_EXTERNAL_SOURCE_IDS") or ExternalNewsSyncConfig().source_ids,
        item_limit_per_source=_env_int("NEWS_ARCHIVE_EXTERNAL_ITEM_LIMIT_PER_SOURCE", 20, minimum=1, maximum=100),
        digest_lookback_days=_env_int("NEWS_ARCHIVE_EXTERNAL_DIGEST_LOOKBACK_DAYS", 3, minimum=1, maximum=30),
        blob_root=os.environ.get("NEWS_ARCHIVE_EXTERNAL_BLOB_ROOT", "scheduled/external-go-news").strip()
        or "scheduled/external-go-news",
        source_description=os.environ.get(
            "NEWS_ARCHIVE_EXTERNAL_SOURCE_DESCRIPTION",
            "Scheduled external Go news source digest.",
        ).strip()
        or "Scheduled external Go news source digest.",
        translate_with_azure=_news_archive_external_translate_enabled(),
        azure_translator_key_env=os.environ.get("NEWS_ARCHIVE_EXTERNAL_AZURE_TRANSLATOR_KEY_ENV", "AZURE_TRANSLATOR_KEY").strip()
        or "AZURE_TRANSLATOR_KEY",
        azure_translator_region_env=os.environ.get(
            "NEWS_ARCHIVE_EXTERNAL_AZURE_TRANSLATOR_REGION_ENV",
            "AZURE_TRANSLATOR_REGION",
        ).strip()
        or "AZURE_TRANSLATOR_REGION",
        enrichment_detail_chars=_env_int("NEWS_ARCHIVE_EXTERNAL_ENRICHMENT_DETAIL_CHARS", 1800, minimum=200, maximum=5000),
        translation_cache_enabled=_env_bool("NEWS_ARCHIVE_EXTERNAL_TRANSLATION_CACHE_ENABLED", True),
        translation_rate_limit_chars_per_minute=_env_int(
            "NEWS_ARCHIVE_EXTERNAL_TRANSLATION_RATE_LIMIT_CHARS_PER_MINUTE",
            20000,
            minimum=0,
            maximum=100000,
        ),
        translation_retry_seconds=_env_float("NEWS_ARCHIVE_EXTERNAL_TRANSLATION_RETRY_SECONDS", 65.0, minimum=0.0),
        translation_retry_count=_env_int("NEWS_ARCHIVE_EXTERNAL_TRANSLATION_RETRY_COUNT", 1, minimum=0, maximum=5),
        source_item_ledger_enabled=_env_bool("NEWS_ARCHIVE_EXTERNAL_SOURCE_ITEM_LEDGER_ENABLED", True),
    )


def _news_archive_external_translate_enabled() -> bool:
    """Return whether external source helper fields should be translated."""
    value = os.environ.get("NEWS_ARCHIVE_EXTERNAL_TRANSLATE_WITH_AZURE", "auto").strip().lower()
    if value == "auto":
        key_env = os.environ.get("NEWS_ARCHIVE_EXTERNAL_AZURE_TRANSLATOR_KEY_ENV", "AZURE_TRANSLATOR_KEY").strip()
        return bool(os.environ.get(key_env or "AZURE_TRANSLATOR_KEY"))
    return value not in {"0", "false", "no", "off"}


def _news_archive_player_context_sync_enabled() -> bool:
    """Return whether archived news should refresh Ratings Explorer article context."""
    return _env_bool("NEWS_ARCHIVE_PLAYER_CONTEXT_SYNC_ENABLED", True)


def _news_archive_player_context_sync_config() -> NewsArchivePlayerContextSyncConfig:
    """Build archive-backed player context sync configuration from app settings."""
    return NewsArchivePlayerContextSyncConfig(
        viewer_url=os.environ.get("NEWS_ARCHIVE_PLAYER_CONTEXT_VIEWER_URL", "https://aga-news-archive-viewer.azurewebsites.net/api/news-archive").strip()
        or "https://aga-news-archive-viewer.azurewebsites.net/api/news-archive",
        excluded_source_modes=_env_csv_tuple("NEWS_ARCHIVE_PLAYER_CONTEXT_EXCLUDED_SOURCE_MODES")
        or NewsArchivePlayerContextSyncConfig().excluded_source_modes,
        include_inactive=_env_bool("NEWS_ARCHIVE_PLAYER_CONTEXT_INCLUDE_INACTIVE", False),
        include_not_visible=_env_bool("NEWS_ARCHIVE_PLAYER_CONTEXT_INCLUDE_NOT_VISIBLE", False),
        max_articles=_env_int("NEWS_ARCHIVE_PLAYER_CONTEXT_MAX_ARTICLES", 0, minimum=0, maximum=100000),
    )


def _env_csv_tuple(name: str) -> tuple[str, ...]:
    """Read a comma-separated app setting as a tuple."""
    raw_value = os.environ.get(name, "").strip()
    if not raw_value:
        return ()
    return tuple(value.strip() for value in raw_value.split(",") if value.strip())


def _env_bool(name: str, default: bool) -> bool:
    """Read a boolean app setting."""
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() not in {"0", "false", "no", "off"}


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    """Read a bounded integer setting."""
    with suppress(ValueError):
        return max(minimum, min(int(os.environ.get(name, str(default)).strip()), maximum))
    return default


def _env_float(name: str, default: float, *, minimum: float) -> float:
    """Read a bounded float setting."""
    with suppress(ValueError):
        return max(minimum, float(os.environ.get(name, str(default)).strip()))
    return default


def _viewer_page(req: func.HttpRequest) -> tuple[int, None] | tuple[None, func.HttpResponse]:
    """Parse the zero-based viewer page number."""
    raw_value = (req.params.get("page") or "0").strip()
    if not raw_value:
        return 0, None
    if not raw_value.isdigit():
        return None, _text_response("Query parameter 'page' must be a non-negative integer.", 400)
    return int(raw_value), None


def _viewer_page_size() -> int:
    """Return the article count for one viewer page."""
    raw_value = os.environ.get("NEWS_ARCHIVE_VIEWER_PAGE_SIZE", str(DEFAULT_PAGE_SIZE)).strip()
    with suppress(ValueError):
        return max(1, min(int(raw_value), 100))
    return DEFAULT_PAGE_SIZE


def _viewer_offset(
    req: func.HttpRequest,
    page: int,
    page_size: int,
) -> tuple[int, None] | tuple[None, func.HttpResponse]:
    """Parse the viewer result offset, falling back to page-based navigation."""
    raw_value = (req.params.get("offset") or "").strip()
    if not raw_value:
        return page * page_size, None
    if not raw_value.isdigit():
        return None, _text_response("Query parameter 'offset' must be a non-negative integer.", 400)
    return int(raw_value), None


def _viewer_anchor_snapshot_id(req: func.HttpRequest) -> tuple[int | None, None] | tuple[None, func.HttpResponse]:
    """Parse an optional snapshot id used to anchor the article list."""
    raw_value = (
        req.params.get("anchorSnapshotId")
        or req.params.get("snapshotId")
        or req.params.get("snapshot")
        or req.params.get("sid")
        or ""
    ).strip()
    if not raw_value:
        return None, None
    if not raw_value.isdigit() or int(raw_value) < 1:
        return None, _text_response("Query parameter 'anchorSnapshotId' must be a positive integer.", 400)
    return int(raw_value), None


def _home_card_count(req: func.HttpRequest) -> tuple[int, None] | tuple[None, func.HttpResponse]:
    """Parse the requested homepage card count."""
    raw_value = (req.params.get("limit") or os.environ.get("NEWS_ARCHIVE_HOME_CARD_COUNT") or str(DEFAULT_HOME_CARD_COUNT)).strip()
    if not raw_value.isdigit():
        return None, _text_response("Query parameter 'limit' must be a positive integer.", 400)
    return max(1, min(int(raw_value), 100)), None


def _article_filter_where_and_params(
    req: func.HttpRequest,
) -> tuple[str, tuple[Any, ...], None] | tuple[None, None, func.HttpResponse]:
    """Build the archive list filter clause and parameters."""
    clauses: list[str] = []
    params: list[Any] = []
    keyword = (req.params.get("q") or req.params.get("keyword") or "").strip()
    if keyword:
        full_text_query = _full_text_query(keyword)
        if not full_text_query:
            return None, None, _text_response("Keyword search must include at least one letter or number.", 400)
        clauses.append(
            "  AND s.[News_Article_Snapshot_ID] IN "
            "(SELECT [KEY] FROM CONTAINSTABLE([clubexpress].[news_article_snapshots], "
            "([Headline], [Summary], [Search_Text]), ?))"
        )
        params.append(full_text_query)

    month, parse_response = _int_query_param(req, "month", 1, 12)
    if parse_response is not None:
        return None, None, parse_response
    year, parse_response = _int_query_param(req, "year", 1900, 2100)
    if parse_response is not None:
        return None, None, parse_response
    active_date_expr = "COALESCE(s.[Active_Date], a.[Active_Date])"
    if year is not None and month is not None:
        start_date = date(year, month, 1)
        end_date = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
        clauses.append(f"  AND {active_date_expr} >= ?")
        params.append(start_date)
        clauses.append(f"  AND {active_date_expr} < ?")
        params.append(end_date)
    elif year is not None:
        clauses.append(f"  AND {active_date_expr} >= ?")
        params.append(date(year, 1, 1))
        clauses.append(f"  AND {active_date_expr} < ?")
        params.append(date(year + 1, 1, 1))
    elif month is not None:
        clauses.append(f"  AND DATEPART(month, {active_date_expr}) = ?")
        params.append(month)

    filter_where = "\n".join(clauses)
    return filter_where, tuple(params), None


def _int_query_param(
    req: func.HttpRequest,
    name: str,
    minimum: int,
    maximum: int,
) -> tuple[int | None, None] | tuple[None, func.HttpResponse]:
    """Parse an optional bounded integer query parameter."""
    raw_value = (req.params.get(name) or "").strip()
    if not raw_value:
        return None, None
    if not raw_value.isdigit():
        return None, _text_response(f"Query parameter '{name}' must be an integer.", 400)
    value = int(raw_value)
    if value < minimum or value > maximum:
        return None, _text_response(f"Query parameter '{name}' must be between {minimum} and {maximum}.", 400)
    return value, None


def _full_text_query(value: str) -> str:
    """Build a safe SQL Server CONTAINS query from user keyword text."""
    phrases = []
    for phrase in re.findall(r'"([^"]+)"', value):
        phrase_text = " ".join(re.findall(r"[A-Za-z0-9]+", phrase))
        if phrase_text:
            phrases.append(f'"{phrase_text}"')
    without_phrases = re.sub(r'"[^"]+"', " ", value)
    terms = []
    for term in re.findall(r"[A-Za-z0-9]+", without_phrases):
        if len(term) < 2:
            continue
        terms.append(f'"{term}*"')
    pieces = (phrases + terms)[:12]
    return " AND ".join(pieces)


def _positive_int_route_param(req: func.HttpRequest, name: str) -> tuple[int, None] | tuple[None, func.HttpResponse]:
    """Parse positive integer route parameter."""
    raw_value = str(req.route_params.get(name) or "")
    if not raw_value.isdigit() or int(raw_value) < 1:
        return None, _text_response(f"Route parameter '{name}' must be a positive integer.", 400)
    return int(raw_value), None


def _rewrite_article_assets(
    article_html: str,
    assets: list[dict[str, Any]],
    access_query_params: dict[str, str] | None = None,
    *,
    asset_base_url: str = "",
) -> str:
    """Rewrite captured article image URLs to the local asset proxy."""
    rewritten = article_html
    normalized_base_url = asset_base_url.rstrip("/")
    for asset in assets:
        if asset.get("Capture_Status") != "captured" or not asset.get("Blob_Path"):
            continue
        source_url = _normalize_url(str(asset.get("Source_Url") or ""))
        if not source_url:
            continue
        local_path = f"/api/news-archive/asset/{asset['News_Article_Asset_ID']}"
        local_url = f"{normalized_base_url}{local_path}" if normalized_base_url else local_path
        if access_query_params:
            local_url = f"{local_url}?{urlencode(access_query_params)}"
        for variant in _asset_url_variants(source_url):
            rewritten = rewritten.replace(variant, local_url)
            rewritten = rewritten.replace(html.escape(variant, quote=True), local_url)
    return rewritten


def _asset_url_variants(source_url: str) -> set[str]:
    """Return likely editor/render URL variants for the same asset."""
    variants = {source_url}
    parsed = urlparse(source_url)
    if parsed.scheme and parsed.netloc:
        protocol_relative = f"//{parsed.netloc}{parsed.path}"
        if parsed.query:
            protocol_relative += f"?{parsed.query}"
        variants.add(protocol_relative)
    if parsed.netloc.lower() == "images.clubexpress.com":
        path = parsed.path.lstrip("/")
        s3_path = f"/ClubExpressClubFiles/{path}"
        variants.add(f"//s3.amazonaws.com{s3_path}")
        variants.add(f"https://s3.amazonaws.com{s3_path}")
    return variants


def _sanitize_article_html(article_html: str) -> str:
    """Remove active content from archived article HTML before public display."""
    sanitized = re.sub(r"(?is)<script\b[^>]*>.*?</script>", "", article_html)
    sanitized = re.sub(r"(?is)<iframe\b[^>]*>.*?</iframe>", "", sanitized)
    sanitized = re.sub(r"(?is)<object\b[^>]*>.*?</object>", "", sanitized)
    sanitized = re.sub(r"(?is)<embed\b[^>]*>", "", sanitized)
    sanitized = re.sub(r"\son[a-z]+\s*=\s*(['\"]).*?\1", "", sanitized)
    return sanitized


def _rewrite_external_links(article_html: str) -> str:
    """Make external links open outside the article preview iframe."""

    def replace_anchor(match: re.Match[str]) -> str:
        tag = match.group(0)
        href_match = re.search(r"(?is)\bhref\s*=\s*(['\"])(.*?)\1", tag)
        if not href_match:
            return tag
        href = html.unescape(href_match.group(2)).strip().lower()
        if not (href.startswith("http://") or href.startswith("https://") or href.startswith("//")):
            return tag
        additions = ""
        if not re.search(r"(?is)\starget\s*=", tag):
            additions += ' target="_blank"'
        if not re.search(r"(?is)\srel\s*=", tag):
            additions += ' rel="noopener noreferrer"'
        if not additions:
            return tag
        return tag[:-1] + additions + ">"

    return re.sub(r"(?is)<a\b(?=[^>]*\bhref\s*=)[^>]*>", replace_anchor, article_html)


def _request_origin(req: func.HttpRequest) -> str:
    """Return the request origin for absolute article and asset URLs."""
    parsed = urlparse(req.url)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return os.environ.get("NEWS_ARCHIVE_PUBLIC_BASE_URL", "https://aga-news-archive-viewer.azurewebsites.net").rstrip("/")


def _absolute_viewer_article_url(req: func.HttpRequest, snapshot_id: int) -> str:
    """Return a clean absolute viewer URL for one article."""
    return f"{_request_origin(req)}/api/news-archive?{urlencode({'snapshotId': str(snapshot_id)})}"


def _render_viewer_page(req: func.HttpRequest) -> str:
    """Render the viewer shell with article-specific social metadata when available."""
    metadata = _viewer_social_metadata(req)
    title = str(metadata.get("title") or "AGA E-Journal")
    description = str(
        metadata.get("description")
        or "Search and read the American Go Association E-Journal news archive."
    )
    canonical_url = str(metadata.get("url") or f"{_request_origin(req)}/api/news-archive")
    image_url = str(
        metadata.get("image")
        or f"{_request_origin(req)}/api/news-archive/static/default-card-image"
    )
    document_title = f"{title} | AGA E-Journal" if metadata else title
    social_tags = "\n".join(
        (
            f'  <meta name="description" content="{html.escape(description, quote=True)}">',
            '  <meta property="og:type" content="article">' if metadata else '  <meta property="og:type" content="website">',
            '  <meta property="og:site_name" content="American Go Association">',
            f'  <meta property="og:title" content="{html.escape(title, quote=True)}">',
            f'  <meta property="og:description" content="{html.escape(description, quote=True)}">',
            f'  <meta property="og:url" content="{html.escape(canonical_url, quote=True)}">',
            f'  <meta property="og:image" content="{html.escape(image_url, quote=True)}">',
            f'  <meta property="og:image:alt" content="{html.escape(title, quote=True)}">',
            '  <meta name="twitter:card" content="summary_large_image">',
            f'  <meta name="twitter:title" content="{html.escape(title, quote=True)}">',
            f'  <meta name="twitter:description" content="{html.escape(description, quote=True)}">',
            f'  <meta name="twitter:image" content="{html.escape(image_url, quote=True)}">',
            f'  <link rel="canonical" href="{html.escape(canonical_url, quote=True)}">',
        )
    )
    return (
        INDEX_HTML.replace("__DOCUMENT_TITLE__", html.escape(document_title))
        .replace("__SOCIAL_META_TAGS__", social_tags)
    )


def _viewer_social_metadata(req: func.HttpRequest) -> dict[str, str]:
    """Load safe share-card metadata for the selected public article."""
    raw_snapshot_id = str(
        req.params.get("snapshotId")
        or req.params.get("snapshot")
        or req.params.get("sid")
        or ""
    ).strip()
    if not raw_snapshot_id.isdigit() or int(raw_snapshot_id) < 1:
        return {}

    adapter, error_response = _adapter_or_response()
    if error_response is not None:
        return {}
    snapshot_id = int(raw_snapshot_id)
    try:
        public_where = _public_where(_viewer_access_options(req))
        rows = adapter.query_rows(GET_SNAPSHOT_TEMPLATE.format(public_where=public_where), (snapshot_id,))
        if not rows:
            return {}
        snapshot = _json_safe_row(rows[0])
        assets = [_json_safe_row(row) for row in adapter.query_rows(GET_ASSETS_SQL, (snapshot_id,))]
    except Exception:
        logging.exception("Unable to load social metadata for news snapshot %s.", snapshot_id)
        return {}

    title = str(snapshot.get("Headline") or "Untitled").strip() or "Untitled"
    summary = re.sub(r"\s+", " ", _html_to_text(str(snapshot.get("Summary") or ""))).strip()
    if len(summary) > 240:
        summary = summary[:237].rstrip() + "..."
    if not summary:
        summary = f"Read {title} in the American Go Association E-Journal archive."
    return {
        "title": title,
        "description": summary,
        "url": _absolute_viewer_article_url(req, snapshot_id),
        "image": _absolute_social_image_url(req, snapshot, assets),
    }


def _absolute_social_image_url(
    req: func.HttpRequest,
    snapshot: dict[str, Any],
    assets: list[dict[str, Any]],
) -> str:
    """Return the best absolute social-card image URL for an article."""
    origin = _request_origin(req)
    if _is_external_digest_article(snapshot):
        return f"{origin}/api/news-archive/static/digest-card-image"
    thumbnail_id = snapshot.get("Thumbnail_Asset_ID")
    if thumbnail_id:
        return f"{origin}/api/news-archive/asset/{int(thumbnail_id)}"
    for asset in assets:
        content_type = str(asset.get("Content_Type") or "").lower()
        file_name = str(asset.get("File_Name") or "").lower()
        if content_type.startswith("image/") or file_name.endswith((".jpg", ".jpeg", ".png", ".gif", ".webp")):
            asset_id = asset.get("News_Article_Asset_ID")
            if asset_id:
                return f"{origin}/api/news-archive/asset/{int(asset_id)}"
    return f"{origin}/api/news-archive/static/default-card-image"


def _script_json(value: Any) -> str:
    """Serialize data for an inline script without allowing an HTML end-tag escape."""
    return (
        json.dumps(value, ensure_ascii=False)
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


def _render_home_cards_page(articles: list[dict[str, Any]], req: func.HttpRequest) -> str:
    """Render recent article cards for a homepage iframe."""
    cards_html = "\n".join(_render_home_card(article, req) for article in articles)
    if not cards_html:
        cards_html = '<div class="empty">No recent news is available.</div>'
    layout = (req.params.get("layout") or "carousel").strip().lower()
    layout_class = "grid-mode" if layout == "grid" else "carousel-mode"
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AGA Recent News</title>
  <style>
    :root {
      color-scheme: light;
      --ink: #1f3342;
      --muted: #687a87;
      --line: #d7e3ec;
      --link: #2171b9;
      --link-dark: #014a7d;
      --button: #2f73b7;
      --button-hover: #1f5f9d;
      --paper: #ffffff;
      --soft: #f4f9fd;
    }
    * { box-sizing: border-box; }
    html, body {
      margin: 0;
      background: transparent;
      color: var(--ink);
      font: 14px/1.42 "Red Hat Text", Arial, Helvetica, sans-serif;
    }
    .cards-shell {
      position: relative;
      width: 100%;
      padding: 2px 46px;
    }
    .cards-viewport {
      overflow: hidden;
      width: 100%;
      cursor: grab;
      touch-action: pan-y;
    }
    .cards-viewport.is-dragging {
      cursor: grabbing;
      user-select: none;
    }
    .cards-viewport.is-dragging .cards-track {
      transition: none;
    }
    .cards-track {
      display: flex;
      gap: 24px;
      align-items: stretch;
      transition: transform 220ms ease;
      will-change: transform;
    }
    .card {
      flex: 0 0 calc((100% - 48px) / 3);
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--paper);
      display: grid;
      grid-template-rows: auto 1fr;
      overflow: hidden;
    }
    .carousel-button {
      position: absolute;
      top: 50%;
      z-index: 2;
      width: 34px;
      height: 54px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.96);
      color: var(--link-dark);
      box-shadow: 0 6px 14px rgba(1, 74, 125, 0.12);
      cursor: pointer;
      display: grid;
      place-items: center;
      font-size: 28px;
      line-height: 1;
      transform: translateY(-50%);
    }
    .carousel-button:hover:not(:disabled),
    .carousel-button:focus:not(:disabled) {
      border-color: var(--link);
      color: var(--link);
    }
    .carousel-button:disabled {
      cursor: not-allowed;
      opacity: 0.32;
    }
    .carousel-button.prev { left: 2px; }
    .carousel-button.next { right: 2px; }
    .thumb {
      width: 100%;
      aspect-ratio: 4 / 3;
      background: linear-gradient(135deg, #d7e8f4, #f6fbfe);
      overflow: hidden;
    }
    .thumb img {
      width: 100%;
      height: 100%;
      display: block;
      object-fit: cover;
      object-position: center 35%;
      user-select: none;
      -webkit-user-drag: none;
    }
    .body {
      min-width: 0;
      padding: 14px;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .title {
      margin: 0;
      color: var(--link-dark);
      font-size: 18px;
      font-weight: 700;
      line-height: 1.18;
      letter-spacing: 0;
      overflow-wrap: anywhere;
    }
    .meta {
      display: flex;
      flex-wrap: wrap;
      gap: 4px 8px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.25;
    }
    .meta span:not(:last-child)::after {
      content: "|";
      margin-left: 8px;
      color: #9baab4;
    }
    .summary {
      margin: 0;
      color: #334f65;
      font-size: 14px;
      line-height: 1.4;
      display: -webkit-box;
      -webkit-line-clamp: 4;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }
    .read-more {
      width: fit-content;
      margin-top: auto;
      align-self: flex-start;
      border-radius: 999px;
      padding: 7px 13px;
      background: var(--button);
      color: #ffffff;
      cursor: pointer;
      font-size: 13px;
      font-weight: 700;
      line-height: 1;
      text-decoration: none;
    }
    .read-more:hover,
    .read-more:focus {
      background: var(--button-hover);
      color: #ffffff;
      text-decoration: none;
    }
    .empty {
      border: 1px dashed var(--line);
      border-radius: 8px;
      padding: 24px;
      color: var(--muted);
      background: var(--soft);
      text-align: center;
    }
    .grid-mode {
      padding: 2px;
    }
    .grid-mode .cards-viewport {
      overflow: visible;
      cursor: auto;
      touch-action: auto;
    }
    .grid-mode .cards-track {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      transform: none !important;
    }
    .grid-mode .card {
      flex-basis: auto;
    }
    .grid-mode .carousel-button {
      display: none;
    }
    @media (max-width: 900px) {
      .card {
        flex-basis: calc((100% - 24px) / 2);
      }
      .grid-mode .cards-track {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }
    @media (max-width: 560px) {
      .cards-shell {
        padding: 2px 36px;
      }
      .cards-track {
        gap: 12px;
      }
      .card { flex-basis: 100%; }
      .carousel-button {
        width: 30px;
        height: 48px;
        font-size: 24px;
      }
      .grid-mode .cards-track {
        grid-template-columns: 1fr;
      }
      .body { padding: 12px; }
      .title { font-size: 17px; }
    }
  </style>
</head>
<body>
  <section class="cards-shell __LAYOUT_CLASS__" data-carousel data-layout="__LAYOUT_MODE__" aria-label="Recent AGA news">
    <button class="carousel-button prev" type="button" aria-label="View newer news">&lsaquo;</button>
    <div class="cards-viewport">
      <div class="cards-track">
        __CARDS_HTML__
      </div>
    </div>
    <button class="carousel-button next" type="button" aria-label="View older news">&rsaquo;</button>
  </section>
    <script>
    (function () {
      var shell = document.querySelector('[data-carousel]');
      if (!shell || shell.dataset.layout === 'grid') return;
      var viewport = shell.querySelector('.cards-viewport');
      var track = shell.querySelector('.cards-track');
      var cards = Array.prototype.slice.call(shell.querySelectorAll('.card'));
      var previous = shell.querySelector('.carousel-button.prev');
      var next = shell.querySelector('.carousel-button.next');
      var pendingSnapshotKey = 'agaNewsArchivePendingSnapshot';
      var index = 0;
      var activePointerId = null;
      var dragStartX = 0;
      var dragDeltaX = 0;
      var hasDragged = false;
      var suppressClick = false;

      function rememberSnapshot(snapshotId) {
        snapshotId = String(snapshotId || '').trim();
        if (!/^\\d+$/.test(snapshotId)) return;
        var payload = JSON.stringify({
          snapshotId: snapshotId,
          createdAt: Date.now()
        });
        try {
          window.localStorage.setItem(pendingSnapshotKey, payload);
        } catch (error) {}
        try {
          window.sessionStorage.setItem(pendingSnapshotKey, payload);
        } catch (error) {}
      }

      function isInteractiveTarget(target) {
        return Boolean(
          target &&
          target.closest &&
          target.closest('a, button, input, select, textarea, [role="button"], [contenteditable="true"]')
        );
      }

      function visibleCount() {
        var width = shell.clientWidth || window.innerWidth;
        if (width <= 560) return 1;
        if (width <= 900) return 2;
        return 3;
      }

      function maxIndex() {
        return Math.max(0, cards.length - visibleCount());
      }

      function stepWidth() {
        if (!cards.length) return 0;
        var styles = window.getComputedStyle(track);
        var gap = parseFloat(styles.columnGap || styles.gap || '0') || 0;
        return cards[0].getBoundingClientRect().width + gap;
      }

      function update() {
        var max = maxIndex();
        index = Math.max(0, Math.min(index, max));
        track.style.transform = 'translateX(' + (-index * stepWidth()) + 'px)';
        previous.disabled = index <= 0;
        next.disabled = index >= max;
      }

      function dragPosition(deltaX) {
        var step = stepWidth();
        var position = (-index * step) + deltaX;
        var minimum = -maxIndex() * step;
        if (position > 0) position *= 0.25;
        if (position < minimum) position = minimum + ((position - minimum) * 0.25);
        track.style.transform = 'translateX(' + position + 'px)';
      }

      function finishDrag(event, shouldCommit) {
        if (activePointerId === null || event.pointerId !== activePointerId) return;
        var pointerId = activePointerId;
        activePointerId = null;
        viewport.classList.remove('is-dragging');
        if (viewport.hasPointerCapture && viewport.hasPointerCapture(pointerId)) {
          viewport.releasePointerCapture(pointerId);
        }

        if (hasDragged && shouldCommit) {
          var step = stepWidth();
          var threshold = Math.min(80, Math.max(32, step * 0.18));
          if (Math.abs(dragDeltaX) >= threshold && step > 0) {
            var cardCount = Math.max(1, Math.round(Math.abs(dragDeltaX) / step));
            index += dragDeltaX < 0 ? cardCount : -cardCount;
          }
        }
        if (hasDragged) {
          suppressClick = true;
          window.setTimeout(function () { suppressClick = false; }, 500);
        }

        dragDeltaX = 0;
        hasDragged = false;
        update();
      }

      previous.addEventListener('click', function () {
        index -= 1;
        update();
      });
      next.addEventListener('click', function () {
        index += 1;
        update();
      });
      viewport.addEventListener('pointerdown', function (event) {
        if (!event.isPrimary || activePointerId !== null) return;
        if (event.pointerType === 'mouse' && event.button !== 0) return;
        suppressClick = false;
        if (isInteractiveTarget(event.target)) return;
        if (maxIndex() <= 0) return;
        activePointerId = event.pointerId;
        dragStartX = event.clientX;
        dragDeltaX = 0;
        hasDragged = false;
        viewport.classList.add('is-dragging');
        if (viewport.setPointerCapture) viewport.setPointerCapture(activePointerId);
      });
      viewport.addEventListener('pointermove', function (event) {
        if (activePointerId === null || event.pointerId !== activePointerId) return;
        dragDeltaX = event.clientX - dragStartX;
        if (!hasDragged && Math.abs(dragDeltaX) < 6) return;
        hasDragged = true;
        dragPosition(dragDeltaX);
      });
      viewport.addEventListener('pointerup', function (event) {
        finishDrag(event, true);
      });
      viewport.addEventListener('pointercancel', function (event) {
        finishDrag(event, false);
      });
      viewport.addEventListener('lostpointercapture', function (event) {
        finishDrag(event, false);
      });
      viewport.addEventListener('dragstart', function (event) {
        event.preventDefault();
      });
      viewport.addEventListener('click', function (event) {
        if (!suppressClick) return;
        event.preventDefault();
        event.stopPropagation();
        suppressClick = false;
      }, true);
      shell.addEventListener('click', function (event) {
        var target = event.target;
        if (!target || !target.closest) return;
        var link = target.closest('a.read-more');
        if (!link) return;
        rememberSnapshot(link.getAttribute('data-snapshot-id'));
      });
      window.addEventListener('resize', update);
      update();
    }());
  </script>
</body>
</html>""".replace("__CARDS_HTML__", cards_html).replace("__LAYOUT_CLASS__", layout_class).replace("__LAYOUT_MODE__", layout)


def _render_home_card(article: dict[str, Any], req: func.HttpRequest) -> str:
    """Render one recent news card."""
    snapshot_id = int(article["News_Article_Snapshot_ID"])
    headline = html.escape(str(article.get("Headline") or "Untitled"))
    summary = html.escape(_card_summary(str(article.get("Summary") or "")))
    meta = [
        _clubexpress_display_date(article.get("Active_Date")),
        str(article.get("Author") or "").strip(),
        _card_category(article),
    ]
    meta_html = "".join(f"<span>{html.escape(value)}</span>" for value in meta if value)
    thumbnail_id = article.get("Thumbnail_Asset_ID")
    thumbnail_html = ""
    if _is_external_digest_article(article):
        thumbnail_html = (
            '<div class="thumb">'
            '<img src="/api/news-archive/static/digest-card-image" '
            'alt="International Go News Digest" loading="lazy">'
            "</div>"
        )
    elif thumbnail_id:
        alt_text = str(article.get("Thumbnail_Alt_Text") or article.get("Thumbnail_Title_Text") or article.get("Headline") or "")
        thumbnail_html = (
            '<div class="thumb">'
            f'<img src="/api/news-archive/asset/{int(thumbnail_id)}" alt="{html.escape(alt_text, quote=True)}" loading="lazy">'
            "</div>"
        )
    else:
        thumbnail_html = (
            '<div class="thumb">'
            '<img src="/api/news-archive/static/default-card-image" '
            'alt="AGA news" loading="lazy">'
            "</div>"
        )
    read_more_url = html.escape(_viewer_article_url(req, snapshot_id), quote=True)
    return f"""<article class="card">
  {thumbnail_html}
  <div class="body">
    <h2 class="title">{headline}</h2>
    <div class="meta">{meta_html}</div>
    <p class="summary">{summary or "No summary available."}</p>
    <a class="read-more" href="{read_more_url}" target="_top" data-snapshot-id="{snapshot_id}">Read More</a>
  </div>
</article>"""


def _viewer_article_url(req: func.HttpRequest, snapshot_id: int) -> str:
    """Return a top-level viewer URL for a card link."""
    base_url = (
        req.params.get("viewerUrl")
        or os.environ.get("NEWS_ARCHIVE_HOME_CARDS_VIEWER_URL")
        or "/api/news-archive"
    ).strip()
    param_name = (req.params.get("snapshotParam") or "snapshotId").strip() or "snapshotId"
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{urlencode({param_name: str(snapshot_id)})}"


def _card_summary(value: str) -> str:
    """Return compact homepage-card summary text."""
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def _card_category(article: dict[str, Any]) -> str:
    """Return a compact display category for a card."""
    category = str(article.get("Category") or "").strip()
    if category == EXTERNAL_DIGEST_CATEGORY:
        return "External Digest"
    return category


def _is_external_digest_article(article: dict[str, Any]) -> bool:
    """Return whether a card is an external go news digest."""
    return str(article.get("Category") or "").strip() == EXTERNAL_DIGEST_CATEGORY


def _render_article_page(
    snapshot: dict[str, Any],
    article_html: str,
    assets: list[dict[str, Any]],
    *,
    article_url: str,
    copy_url: str,
    email_html_url: str,
) -> str:
    """Render one archived article page."""
    del assets
    headline_text = str(snapshot.get("Headline") or "Untitled").strip() or "Untitled"
    headline = html.escape(headline_text)
    author = html.escape(str(snapshot.get("Author") or ""))
    published_date = html.escape(_clubexpress_display_date(snapshot.get("Active_Date")))
    author_prefix = f"{author} |  " if author else ""
    date_text = f"Published on {published_date}" if published_date else ""
    summary = re.sub(r"\s+", " ", _html_to_text(str(snapshot.get("Summary") or ""))).strip()
    share_text = summary[:240].rstrip() if summary else f"Read {headline_text} in the AGA E-Journal archive."
    share_data = _script_json({"title": headline_text, "text": share_text, "url": article_url})
    copy_data = _script_json({"url": copy_url})
    email_data = _script_json({"endpoint": email_html_url, "subject": headline_text})
    email_url = "mailto:?" + urlencode({"subject": headline_text})
    facebook_url = "https://www.facebook.com/sharer/sharer.php?" + urlencode({"u": article_url})
    linkedin_url = "https://www.linkedin.com/sharing/share-offsite/?" + urlencode({"url": article_url})
    bluesky_url = "https://bsky.app/intent/compose?" + urlencode({"text": f"{headline_text}\n{article_url}"})
    x_url = "https://twitter.com/intent/tweet?" + urlencode({"text": headline_text, "url": article_url})
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{headline}</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #222222;
      --muted: #959595;
      --link: #2e56a3;
      --club-blue: #2171b9;
      --paper: #ffffff;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--paper);
      color: var(--ink);
      font: 15px/1.4 "Red Hat Text", Arial, Helvetica, sans-serif;
    }}
    main {{
      max-width: 100rem;
      margin: 0 auto;
      padding: 20px 24px 34px;
    }}
    .article-container h2 {{
      background: var(--club-blue);
      color: #ffffff;
      line-height: 1.2;
      letter-spacing: 0;
      border-bottom: 0;
    }}
    .article-container h2 {{
      margin: 0;
      padding: 5px 9px;
      font-size: 22px;
      font-weight: 500;
    }}
    .article-heading {{
      position: relative;
      display: flex;
      align-items: center;
      gap: 8px;
      background: var(--club-blue);
    }}
    .article-heading > h2 {{
      flex: 1 1 auto;
      min-width: 0;
      background: transparent;
    }}
    .article-actions {{
      display: flex;
      flex: 0 0 auto;
      align-items: center;
      gap: 2px;
      padding-right: 5px;
    }}
    .article-action {{
      display: inline-flex;
      width: 36px;
      height: 36px;
      align-items: center;
      justify-content: center;
      padding: 0;
      border: 1px solid transparent;
      border-radius: 6px;
      color: #ffffff;
      background: transparent;
      cursor: pointer;
      text-decoration: none;
    }}
    .article-action:hover,
    .article-action:focus-visible,
    .article-action[aria-expanded="true"] {{
      border-color: rgba(255, 255, 255, 0.62);
      background: rgba(255, 255, 255, 0.16);
      outline: none;
    }}
    .article-action svg {{
      width: 20px;
      height: 20px;
      fill: none;
      stroke: currentColor;
      stroke-linecap: round;
      stroke-linejoin: round;
      stroke-width: 1.8;
    }}
    .share-control {{ position: relative; }}
    .share-menu {{
      position: absolute;
      z-index: 20;
      top: calc(100% + 6px);
      right: 0;
      width: 220px;
      overflow: hidden;
      border: 1px solid #c8d7e5;
      border-radius: 8px;
      background: #ffffff;
      box-shadow: 0 10px 28px rgba(20, 45, 68, 0.22);
    }}
    .share-menu[hidden] {{ display: none; }}
    .share-menu [hidden] {{ display: none !important; }}
    .share-menu a,
    .share-menu button {{
      display: block;
      width: 100%;
      padding: 9px 12px;
      border: 0;
      border-bottom: 1px solid #e6edf3;
      color: #173b57;
      background: #ffffff;
      font: inherit;
      text-align: left;
      text-decoration: none;
      cursor: pointer;
    }}
    .share-menu :last-child {{ border-bottom: 0; }}
    .share-menu a:hover,
    .share-menu a:focus-visible,
    .share-menu button:hover,
    .share-menu button:focus-visible {{
      background: #edf6fc;
      outline: none;
    }}
    .share-status {{
      position: fixed;
      z-index: 90;
      right: 16px;
      bottom: 16px;
      max-width: min(360px, calc(100vw - 32px));
      padding: 8px 12px;
      border: 1px solid #b9ccdc;
      border-radius: 6px;
      background: #ffffff;
      color: #173b57;
      box-shadow: 0 6px 18px rgba(20, 45, 68, 0.2);
    }}
    .share-status:empty {{ display: none; }}
    .email-ready {{
      position: fixed;
      z-index: 100;
      inset: 0;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
      background: rgba(15, 38, 56, 0.45);
    }}
    .email-ready[hidden] {{ display: none; }}
    .email-ready-card {{
      width: min(430px, 100%);
      padding: 20px;
      border-radius: 10px;
      background: #ffffff;
      box-shadow: 0 16px 44px rgba(20, 45, 68, 0.3);
    }}
    .email-ready-card h3 {{ margin: 0 0 8px; font-size: 20px; }}
    .email-ready-card p {{ margin: 0 0 16px; }}
    .email-ready-actions {{ display: flex; flex-wrap: wrap; gap: 8px; }}
    .email-ready-actions a,
    .email-ready-actions button {{
      display: inline-flex;
      align-items: center;
      min-height: 38px;
      padding: 7px 13px;
      border: 1px solid #2171b9;
      border-radius: 6px;
      background: #2171b9;
      color: #ffffff;
      font: inherit;
      text-decoration: none;
      cursor: pointer;
    }}
    .email-ready-actions button {{ background: #ffffff; color: #173b57; }}
    .zero-top-margin {{ margin-top: 0; }}
    .news-author {{
      color: var(--muted);
      font-size: 15px;
      line-height: 24px;
      margin-top: 8px;
    }}
    .news-text {{
      line-height: 1.4;
      margin-top: 10px;
      overflow-wrap: anywhere;
    }}
    .news-text a {{ color: var(--link); }}
    .news-text :is(img, video, iframe) {{
      max-width: 100%;
    }}
    .news-text :is(img, video) {{
      height: auto;
    }}
    .inner-column img {{
      height: auto !important;
    }}
    table {{
      max-width: 100%;
      border-collapse: collapse;
    }}
    .clear {{ clear: both; }}
    grammarly-extension {{ display: none !important; }}
    .resp-row,
    .build-row {{
      clear: both;
      width: 100%;
    }}
    .resp-row::after,
    .build-row::after {{
      clear: both;
      content: "";
      display: block;
    }}
    .column,
    .build-column,
    .template-column {{
      box-sizing: border-box;
      float: left;
      padding: 10px;
    }}
    .build-row,
    .template-row {{
      display: flex;
      flex-wrap: wrap;
    }}
    .build-column,
    .template-column {{
      float: none;
      vertical-align: top;
    }}
    .inner-column,
    .build-column .inner-column,
    .template-column .inner-column {{
      box-sizing: border-box;
    }}
    .inner-column :is(img, iframe, .ce-widget) {{
      max-width: 100%;
    }}
    .third {{ width: 33.3%; }}
    .quarter {{ width: 25%; }}
    .half {{ width: 50%; }}
    .twothirds {{ width: 66.6%; }}
    .threequarter {{ width: 75%; }}
    .thirty {{ width: 30%; }}
    .forty {{ width: 40%; }}
    .sixty {{ width: 60%; }}
    .seventy {{ width: 70%; }}
    .full {{ width: 100%; }}
    .fifth {{ width: 20%; }}
    .img-left-flt {{
      float: left;
      margin: 0 10px 10px 0;
      max-width: 30%;
    }}
    .img-right-flt {{
      float: right;
      margin: 0 0 10px 10px;
      max-width: 30%;
    }}
    .img-left-flt img,
    .img-right-flt img {{
      max-width: 100%;
    }}
    @media (max-width: 767px) {{
      main {{ padding: 16px; }}
      .article-container h2 {{ font-size: 20px; }}
      .article-heading {{ align-items: flex-start; }}
      .article-actions {{ padding-top: 1px; }}
      .column.half,
      .column.seventy,
      .column.thirty,
      .column.forty,
      .column.sixty,
      .column.quarter,
      .column.threequarter,
      .build-column.half,
      .build-column.seventy,
      .build-column.thirty,
      .build-column.forty,
      .build-column.sixty,
      .build-column.quarter,
      .build-column.threequarter,
      .build-column.fifth,
      .template-column.half,
      .template-column.seventy,
      .template-column.thirty,
      .template-column.forty,
      .template-column.sixty,
      .template-column.quarter,
      .template-column.threequarter,
      .template-column.fifth,
      .threequarter.two-column,
      .twothirds.two-column,
      .quarter.two-column,
      .third.two-column,
      .column.third,
      .build-column.third,
      .template-column.third,
      .quarter.three-column,
      .half.three-column,
      .quarter.four-column {{
        width: 100%;
      }}
      .img-left-flt,
      .img-right-flt {{
        clear: left;
        display: block;
        float: none;
        margin: 0 auto 10px;
        max-width: 100%;
        width: 100%;
      }}
    }}
    @media print {{
      .article-actions {{ display: none !important; }}
      main {{ max-width: none; padding: 0; }}
      .article-heading {{ print-color-adjust: exact; -webkit-print-color-adjust: exact; }}
    }}
  </style>
</head>
<body>
  <main>
    <div class="article-container">
      <div class="article-heading">
        <h2 class="zero-top-margin">{headline}</h2>
        <div class="article-actions" aria-label="Article actions">
          <div class="share-control" id="shareControl">
            <button class="article-action" id="shareArticle" type="button" title="Share article" aria-label="Share article" aria-haspopup="menu" aria-expanded="false">
              <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><circle cx="18" cy="5" r="2.5"/><circle cx="6" cy="12" r="2.5"/><circle cx="18" cy="19" r="2.5"/><path d="m8.2 10.8 7.6-4.5m-7.6 6.9 7.6 4.5"/></svg>
            </button>
            <div class="share-menu" id="shareMenu" role="menu" hidden>
              <button id="printArticle" type="button" role="menuitem">Print</button>
              <button id="emailFullArticle" type="button" role="menuitem">Email full article</button>
              <button id="copyArticleLink" type="button" role="menuitem">Copy link</button>
              <a href="{html.escape(facebook_url, quote=True)}" target="_blank" rel="noopener noreferrer" role="menuitem">Facebook</a>
              <a href="{html.escape(linkedin_url, quote=True)}" target="_blank" rel="noopener noreferrer" role="menuitem">LinkedIn</a>
              <a href="{html.escape(bluesky_url, quote=True)}" target="_blank" rel="noopener noreferrer" role="menuitem">Bluesky</a>
              <a href="{html.escape(x_url, quote=True)}" target="_blank" rel="noopener noreferrer" role="menuitem">X</a>
            </div>
          </div>
          <span class="share-status" id="shareStatus" role="status" aria-live="polite"></span>
        </div>
      </div>
      <div class="news-author">{author_prefix}<span>{date_text}</span></div>
      <div class="clear"></div>
      <div class="news-text">{article_html}</div>
    </div>
  </main>
  <div class="email-ready" id="emailReady" role="dialog" aria-modal="true" aria-labelledby="emailReadyTitle" hidden>
    <div class="email-ready-card">
      <h3 id="emailReadyTitle">Article copied</h3>
      <p>The complete formatted article is on your clipboard. Open a new email, click in the message body, and press <strong>Ctrl+V</strong>.</p>
      <div class="email-ready-actions">
        <a id="openEmail" href="{html.escape(email_url, quote=True)}" target="_top">Open email</a>
        <button id="closeEmailReady" type="button">Close</button>
      </div>
    </div>
  </div>
  <script>
    (function () {{
      const shareData = {share_data};
      const copyData = {copy_data};
      const emailData = {email_data};
      const printButton = document.getElementById('printArticle');
      const shareControl = document.getElementById('shareControl');
      const shareButton = document.getElementById('shareArticle');
      const shareMenu = document.getElementById('shareMenu');
      const emailButton = document.getElementById('emailFullArticle');
      const copyButton = document.getElementById('copyArticleLink');
      const status = document.getElementById('shareStatus');
      const emailReady = document.getElementById('emailReady');
      const openEmail = document.getElementById('openEmail');
      const closeEmailReady = document.getElementById('closeEmailReady');
      let emailPayloadPromise;

      function loadEmailPayload() {{
        if (!emailPayloadPromise) {{
          emailPayloadPromise = fetch(emailData.endpoint).then(async function (response) {{
            if (!response.ok) throw new Error(await response.text());
            return response.json();
          }});
        }}
        return emailPayloadPromise;
      }}

      function setMenuOpen(open) {{
        shareMenu.hidden = !open;
        shareButton.setAttribute('aria-expanded', open ? 'true' : 'false');
        if (open) {{
          void loadEmailPayload().catch(function () {{
            emailPayloadPromise = undefined;
          }});
          const firstItem = shareMenu.querySelector('[role="menuitem"]');
          if (firstItem) firstItem.focus();
        }}
      }}

      function copyWithSelection(value) {{
        const textarea = document.createElement('textarea');
        textarea.value = value;
        textarea.style.position = 'fixed';
        textarea.style.left = '-99999px';
        document.body.appendChild(textarea);
        textarea.focus();
        textarea.select();
        const copied = document.execCommand('copy');
        textarea.remove();
        if (!copied) throw new Error('Clipboard permission was denied.');
      }}

      async function copyLink() {{
        try {{
          await copyArticleLinkText();
          status.textContent = 'Article link copied.';
          setMenuOpen(false);
        }} catch (error) {{
          status.textContent = 'Unable to copy the article link.';
        }}
      }}

      async function copyArticleLinkText() {{
        if (navigator.clipboard && navigator.clipboard.writeText) {{
          try {{
            await navigator.clipboard.writeText(copyData.url);
            return;
          }} catch (error) {{
            // The selection fallback works in browsers that restrict clipboard access in iframes.
          }}
        }}
        copyWithSelection(copyData.url);
      }}

      function copyHtmlWithSelection(htmlValue) {{
        const container = document.createElement('div');
        container.contentEditable = 'true';
        container.style.position = 'fixed';
        container.style.left = '-99999px';
        container.style.top = '0';
        container.style.width = '800px';
        container.innerHTML = htmlValue;
        document.body.appendChild(container);
        const selection = window.getSelection();
        const range = document.createRange();
        range.selectNodeContents(container);
        selection.removeAllRanges();
        selection.addRange(range);
        const copied = document.execCommand('copy');
        selection.removeAllRanges();
        container.remove();
        if (!copied) throw new Error('Clipboard permission was denied.');
      }}

      async function copyHtml(htmlValue, plainText) {{
        if (!htmlValue) throw new Error('Article HTML is empty.');
        if (navigator.clipboard && navigator.clipboard.write && window.ClipboardItem) {{
          try {{
            const item = new ClipboardItem({{
              'text/html': new Blob([htmlValue], {{ type: 'text/html' }}),
              'text/plain': new Blob([plainText || htmlValue], {{ type: 'text/plain' }})
            }});
            await navigator.clipboard.write([item]);
            return;
          }} catch (error) {{
            // The selection fallback works in browsers that restrict ClipboardItem in iframes.
          }}
        }}
        copyHtmlWithSelection(htmlValue);
      }}

      async function prepareFullArticleEmail() {{
        emailButton.disabled = true;
        status.textContent = 'Preparing the full article for email.';
        try {{
          const payload = await loadEmailPayload();
          await copyHtml(payload.html || '', payload.text || '');
          setMenuOpen(false);
          emailReady.hidden = false;
          openEmail.focus();
          status.textContent = 'Full article copied for email.';
        }} catch (error) {{
          emailPayloadPromise = undefined;
          status.textContent = 'Unable to copy the full article for email.';
        }} finally {{
          emailButton.disabled = false;
        }}
      }}

      printButton.addEventListener('click', function () {{
        setMenuOpen(false);
        window.print();
      }});

      shareButton.addEventListener('click', function () {{
        setMenuOpen(shareMenu.hidden);
      }});

      emailButton.addEventListener('click', prepareFullArticleEmail);
      copyButton.addEventListener('click', copyLink);
      closeEmailReady.addEventListener('click', function () {{
        emailReady.hidden = true;
        shareButton.focus();
      }});
      emailReady.addEventListener('click', function (event) {{
        if (event.target === emailReady) closeEmailReady.click();
      }});
      document.addEventListener('click', function (event) {{
        if (!shareMenu.hidden && !shareControl.contains(event.target)) setMenuOpen(false);
      }});
      document.addEventListener('keydown', function (event) {{
        if (event.key === 'Escape' && !shareMenu.hidden) {{
          setMenuOpen(false);
          shareButton.focus();
        }} else if (event.key === 'Escape' && !emailReady.hidden) {{
          closeEmailReady.click();
        }}
      }});
    }}());
  </script>
</body>
</html>"""


def _render_email_article_fragment(
    snapshot: dict[str, Any],
    article_html: str,
    _article_url: str,
    assets: list[dict[str, Any]] | None = None,
    *,
    image_max_width: int = 220,
) -> str:
    """Render copy-ready article HTML with inline styles for email editors."""
    headline = html.escape(str(snapshot.get("Headline") or "Untitled"))
    author = html.escape(str(snapshot.get("Author") or "").strip())
    published_date = html.escape(_clubexpress_display_date(snapshot.get("Active_Date")))
    meta_parts = []
    if author:
        meta_parts.append(author)
    if published_date:
        meta_parts.append(f"Published on {published_date}")
    meta_html = " | ".join(meta_parts)
    email_article_html = _prepare_email_article_html(
        article_html,
        assets or [],
        image_max_width=image_max_width,
    )
    return f"""<div style="font-family: Arial, Helvetica, sans-serif; color: #222222; line-height: 1.4; max-width: 100%;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse: collapse; border-spacing: 0; margin: 0; padding: 0; width: 100%;">
    <tr>
      <td bgcolor="#2171b9" style="background-color: #2171b9; padding: 5px 9px; margin: 0; border: 0;">
        <div style="margin: 0; padding: 0; color: #ffffff; font-family: Arial, Helvetica, sans-serif; font-size: 22px; line-height: 26px; font-weight: 500; letter-spacing: 0;">
          <span style="color: #ffffff; text-decoration: none;">{headline}</span>
        </div>
      </td>
    </tr>
  </table>
  <div style="color: #777777; font-family: Arial, Helvetica, sans-serif; font-size: 15px; line-height: 24px; margin: 8px 0 0 0;">{meta_html}</div>
  <div style="clear: both;"></div>
  <div style="font-family: Arial, Helvetica, sans-serif; font-size: 15px; line-height: 1.4; margin-top: 10px; overflow-wrap: anywhere;">
    {email_article_html}
  </div>
  <br clear="all" style="clear: both; line-height: 0; font-size: 0;">
  <div style="font-family: Arial, Helvetica, sans-serif; font-size: 15px; line-height: 18px; margin: 14px 0 0 0;">&nbsp;</div>
</div>"""


def _prepare_email_article_html(
    article_html: str,
    assets: list[dict[str, Any]],
    *,
    image_max_width: int = 220,
) -> str:
    """Inline the article layout rules that the ClubExpress email editor needs."""
    asset_widths: dict[str, int] = {}
    for asset in assets:
        asset_id = str(asset.get("News_Article_Asset_ID") or "").strip()
        with suppress(TypeError, ValueError):
            width = int(asset.get("Width") or 0)
            if asset_id and width > 0:
                asset_widths[asset_id] = width

    def rewrite_paragraph(match: re.Match[str]) -> str:
        paragraph = match.group(0)
        parts = re.match(r"(?is)(<p\b[^>]*>)(.*)(</p>)", paragraph)
        if not parts:
            return paragraph
        floated_images: list[str] = []

        def remove_floated_image(image_match: re.Match[str]) -> str:
            tag = image_match.group(0)
            direction = _email_image_float_direction(tag)
            if not direction:
                return tag
            floated_images.append(
                _email_image_tag(
                    tag,
                    direction,
                    asset_widths,
                    max_width=image_max_width,
                )
            )
            return ""

        body = re.sub(r"(?is)<img\b[^>]*>", remove_floated_image, parts.group(2))
        if not floated_images:
            return paragraph
        return f"{parts.group(1)}{''.join(floated_images)}{body}{parts.group(3)}"

    return re.sub(r"(?is)<p\b[^>]*>.*?</p>", rewrite_paragraph, article_html)


def _email_image_float_direction(image_tag: str) -> str | None:
    """Return the intended float direction for an article image tag."""
    class_value = _html_attr_value(image_tag, "class").lower()
    if "img-left-flt" in class_value:
        return "left"
    if "img-right-flt" in class_value:
        return "right"
    style_value = _html_attr_value(image_tag, "style").lower()
    style_match = re.search(r"(?i)\bfloat\s*:\s*(left|right)\b", style_value)
    if style_match:
        return style_match.group(1).lower()
    align_value = _html_attr_value(image_tag, "align").lower()
    if align_value in {"left", "right"}:
        return align_value
    return None


def _email_image_tag(
    image_tag: str,
    direction: str,
    asset_widths: dict[str, int],
    *,
    max_width: int = 220,
) -> str:
    """Make one floated article image more stable when pasted into email editors."""
    tag = image_tag
    opposite_margin = "right" if direction == "left" else "left"
    tag = _html_set_attr(tag, "align", direction)
    tag = _html_set_attr(tag, "hspace", "7")
    tag = _html_set_attr(tag, "vspace", "3")
    if not _html_attr_value(tag, "width") and not re.search(r"(?i)(?:^|;)\s*width\s*:", _html_attr_value(tag, "style")):
        email_width = max_width
        asset_id_match = re.search(r"(?i)/asset/(\d+)", tag)
        if asset_id_match:
            original_width = asset_widths.get(asset_id_match.group(1), 0)
            if original_width > 0:
                email_width = min(original_width, max_width)
        tag = _html_set_attr(tag, "width", str(email_width))
    layout_style = (
        f"float: {direction}; margin: 0 10px 10px 0; margin-{opposite_margin}: 10px; "
        f"border: 0; max-width: {max_width}px; height: auto;"
    )
    return _html_append_style(tag, layout_style)


def _html_attr_value(tag: str, attr_name: str) -> str:
    """Return one HTML attribute value from a tag."""
    match = re.search(rf"(?is)\b{re.escape(attr_name)}\s*=\s*(['\"])(.*?)\1", tag)
    return html.unescape(match.group(2)) if match else ""


def _html_set_attr(tag: str, attr_name: str, attr_value: str) -> str:
    """Set or add an HTML attribute on a single tag string."""
    escaped_value = html.escape(attr_value, quote=True)
    pattern = rf"(?is)(\b{re.escape(attr_name)}\s*=\s*)(['\"])(.*?)\2"
    if re.search(pattern, tag):
        return re.sub(pattern, lambda match: f"{match.group(1)}{match.group(2)}{escaped_value}{match.group(2)}", tag, count=1)
    return _html_insert_before_tag_close(tag, f' {attr_name}="{escaped_value}"')


def _html_append_style(tag: str, style_value: str) -> str:
    """Append inline CSS to one tag's style attribute."""
    existing_style = _html_attr_value(tag, "style").strip()
    combined = f"{existing_style.rstrip(';')}; {style_value}" if existing_style else style_value
    return _html_set_attr(tag, "style", combined)


def _html_insert_before_tag_close(tag: str, insertion: str) -> str:
    """Insert text before the end of an HTML start tag."""
    stripped = tag.rstrip()
    trailing = tag[len(stripped) :]
    if stripped.endswith("/>"):
        return f"{stripped[:-2].rstrip()}{insertion} />{trailing}"
    if stripped.endswith(">"):
        return f"{stripped[:-1].rstrip()}{insertion}>{trailing}"
    return f"{tag}{insertion}"


def _html_to_text(value: str) -> str:
    """Return readable plain text from an HTML fragment."""
    text_value = re.sub(r"(?is)<script\b[^>]*>.*?</script>", " ", value)
    text_value = re.sub(r"(?is)<style\b[^>]*>.*?</style>", " ", text_value)
    text_value = re.sub(r"(?i)<br\s*/?>", "\n", text_value)
    text_value = re.sub(r"(?i)</(?:p|div|h[1-6]|li|tr)>", "\n", text_value)
    text_value = re.sub(r"(?is)<[^>]+>", " ", text_value)
    text_value = html.unescape(text_value)
    text_value = re.sub(r"[ \t\r\f\v]+", " ", text_value)
    text_value = re.sub(r"\n\s+", "\n", text_value)
    text_value = re.sub(r"\n{3,}", "\n\n", text_value)
    return text_value.strip()


def _clubexpress_display_date(value: Any) -> str:
    """Return a ClubExpress-style m/d/yyyy date string."""
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return f"{value.month}/{value.day}/{value.year}"
    text = str(value or "").strip()
    if not text:
        return ""
    with suppress(ValueError):
        parsed = date.fromisoformat(text[:10])
        return f"{parsed.month}/{parsed.day}/{parsed.year}"
    return text


def _normalize_url(value: str) -> str:
    """Normalize protocol-relative URLs."""
    text = value.strip()
    if text.startswith("//"):
        return "https:" + text
    return text


def _json_safe_row(row: dict[str, Any]) -> dict[str, Any]:
    """Convert row to JSON-safe values."""
    return {key: _json_safe_value(value) for key, value in row.items()}


def _json_safe_value(value: Any) -> Any:
    """Convert values not handled by JSON's default encoder."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _json_response(payload: dict[str, Any], status_code: int = 200) -> func.HttpResponse:
    """Return JSON response."""
    return func.HttpResponse(
        json.dumps(payload, default=_json_safe_value),
        mimetype="application/json",
        status_code=status_code,
        headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
    )


def _html_response(
    body: str,
    status_code: int = 200,
    *,
    allow_same_origin_frame: bool = False,
    frame_ancestors: str | None = None,
) -> func.HttpResponse:
    """Return HTML response."""
    if frame_ancestors is None:
        frame_ancestors = "'self'" if allow_same_origin_frame else "'none'"
    return func.HttpResponse(
        body,
        mimetype="text/html",
        status_code=status_code,
        headers={
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
            "Permissions-Policy": "web-share=(self)",
            "Content-Security-Policy": (
                "default-src 'self'; "
                "img-src 'self' data: https:; "
                "style-src 'self' 'unsafe-inline'; "
                "script-src 'self' 'unsafe-inline'; "
                "frame-src 'self'; "
                f"frame-ancestors {frame_ancestors}"
            ),
        },
    )


def _text_response(message: str, status_code: int) -> func.HttpResponse:
    """Return plain text response."""
    return func.HttpResponse(
        message,
        status_code=status_code,
        mimetype="text/plain",
        headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"},
    )


INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>__DOCUMENT_TITLE__</title>
__SOCIAL_META_TAGS__
  <style>
    :root {
      color-scheme: light;
      --bg: #eaf3fa;
      --panel: #ffffff;
      --panel-alt: #f4f9fd;
      --ink: #163248;
      --muted: #5a7082;
      --line: #bfd4e5;
      --accent: #3a73b2;
      --accent-strong: #014a7d;
      --accent-soft: rgba(174, 208, 232, 0.32);
      --warning: #946112;
      --shadow: 0 18px 40px rgba(1, 74, 125, 0.12);
    }
    * { box-sizing: border-box; }
    html { min-height: 100%; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(174, 208, 232, 0.5), transparent 36rem),
        radial-gradient(circle at bottom right, rgba(207, 228, 241, 0.56), transparent 32rem),
        var(--bg);
      font: 14px/1.45 "Red Hat Text", system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    button,
    input,
    select {
      font: inherit;
    }
    button {
      cursor: pointer;
    }
    button:disabled {
      cursor: not-allowed;
      opacity: 0.52;
    }
    .wrap {
      width: min(1380px, calc(100% - 28px));
      min-height: 100vh;
      margin: 0 auto;
      padding: 18px 0 22px;
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
      gap: 14px;
    }
    .panel {
      background: rgba(255, 255, 255, 0.92);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }
    .search-panel {
      padding: 18px;
      display: grid;
      gap: 14px;
    }
    .search-title {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 16px;
    }
    h1 {
      margin: 0;
      font-size: clamp(24px, 3.4vw, 42px);
      line-height: 1;
      letter-spacing: 0;
      color: var(--accent-strong);
    }
    .archive-state {
      color: var(--muted);
      font-size: 13px;
      text-align: right;
    }
    .search-grid {
      display: grid;
      grid-template-columns: minmax(260px, 1.4fr) minmax(145px, 0.6fr) minmax(145px, 0.6fr) auto auto;
      gap: 12px;
      align-items: end;
    }
    .field {
      display: grid;
      gap: 6px;
      min-width: 0;
    }
    label {
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0;
    }
    input,
    select {
      width: 100%;
      min-width: 0;
      height: 42px;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 0 12px;
      background: #fff;
      color: var(--ink);
      outline: none;
    }
    input:focus,
    select:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 3px var(--accent-soft);
    }
    .action {
      height: 42px;
      border: 1px solid var(--accent);
      border-radius: 999px;
      padding: 0 18px;
      background: var(--accent);
      color: white;
      font-weight: 700;
      white-space: nowrap;
    }
    .action.secondary {
      background: #fff;
      color: var(--accent-strong);
      border-color: var(--line);
    }
    .workspace {
      min-height: 0;
      display: grid;
      grid-template-columns: minmax(0, 1fr);
      gap: 14px;
    }
    .results-panel {
      min-height: 0;
      display: grid;
      overflow: hidden;
    }
    .results-panel {
      grid-template-rows: auto minmax(0, 1fr) auto;
    }
    .section-tools {
      min-height: 62px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      background: var(--panel-alt);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    .section-title {
      min-width: 0;
      display: grid;
      gap: 2px;
    }
    .section-title strong {
      color: var(--ink);
      font-size: 15px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .section-title span {
      color: var(--muted);
      font-size: 12px;
    }
    .pager {
      display: flex;
      align-items: center;
      gap: 8px;
      white-space: nowrap;
    }
    .pager button {
      height: 34px;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 0 12px;
      background: white;
      color: var(--accent-strong);
      font-weight: 700;
    }
    .section-tools-bottom {
      min-height: 54px;
      border-top: 1px solid var(--line);
      border-bottom: 0;
    }
    .results {
      min-height: 0;
      overflow: auto;
      padding: 10px;
      display: grid;
      align-content: start;
      gap: 8px;
    }
    .article-card {
      width: 100%;
      min-height: 116px;
      border: 1px solid transparent;
      border-radius: 8px;
      padding: 12px;
      background: white;
      color: inherit;
      text-align: left;
      display: grid;
      grid-template-columns: 88px minmax(0, 1fr);
      grid-template-areas:
        ". title"
        "thumb summary"
        "thumb meta";
      gap: 6px 10px;
      align-items: start;
    }
    .article-card:hover {
      border-color: var(--line);
      box-shadow: 0 8px 18px rgba(1, 74, 125, 0.08);
    }
    .article-card.selected {
      border-color: var(--accent);
      background: linear-gradient(180deg, #fff, rgba(174, 208, 232, 0.22));
    }
    .article-thumb {
      grid-area: thumb;
      width: 88px;
      height: 66px;
      border-radius: 6px;
      overflow: hidden;
      background: #edf4f8;
      border: 1px solid rgba(149, 173, 190, 0.38);
    }
    .article-thumb img {
      display: block;
      width: 100%;
      height: 100%;
      object-fit: cover;
      object-position: center 35%;
    }
    .article-title {
      grid-area: title;
      min-width: 0;
      margin: 0;
      color: var(--ink);
      font-size: 15px;
      line-height: 1.25;
      font-weight: 800;
      overflow-wrap: anywhere;
    }
    .summary {
      grid-area: summary;
      min-width: 0;
      margin: 0;
      color: #334f65;
      font-size: 13px;
      line-height: 1.38;
      display: -webkit-box;
      -webkit-line-clamp: 3;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }
    .article-meta {
      grid-area: meta;
      min-width: 0;
      display: flex;
      flex-wrap: wrap;
      gap: 6px 10px;
      color: var(--muted);
      font-size: 12px;
    }
    .inline-preview {
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
      background: white;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.74);
      display: grid;
      grid-template-rows: auto auto;
    }
    .preview-tools {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 8px;
      padding: 8px 10px;
      border-bottom: 1px solid var(--line);
      background: var(--panel-alt);
      flex-wrap: wrap;
    }
    .preview-tools button {
      min-height: 32px;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 0 12px;
      background: #fff;
      color: var(--accent-strong);
      font-size: 12px;
      font-weight: 800;
      cursor: pointer;
    }
    .preview-tools button:hover:not(:disabled),
    .preview-tools button:focus:not(:disabled) {
      border-color: var(--accent);
    }
    .preview-tools button:disabled {
      cursor: wait;
      opacity: 0.65;
    }
    .copy-status {
      margin-right: auto;
      color: var(--muted);
      font-size: 12px;
    }
    iframe {
      width: 100%;
      height: 260px;
      min-height: 0;
      border: 0;
      background: white;
    }
    .empty {
      border: 1px dashed var(--line);
      border-radius: 8px;
      padding: 24px;
      color: var(--muted);
      background: rgba(255, 255, 255, 0.72);
      text-align: center;
    }
    .error {
      color: var(--warning);
    }
    @media (max-width: 980px) {
      .wrap {
        min-height: auto;
      }
      .search-title {
        display: grid;
      }
      .archive-state {
        text-align: left;
      }
      .search-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
      .workspace {
        grid-template-columns: 1fr;
      }
      .results-panel {
        min-height: 520px;
      }
    }
    @media (max-width: 560px) {
      .wrap {
        width: min(100% - 20px, 1380px);
        padding-top: 10px;
      }
      .search-grid {
        grid-template-columns: 1fr;
      }
      .section-tools {
        align-items: stretch;
        flex-direction: column;
      }
      .pager {
        justify-content: space-between;
      }
      .pager button {
        flex: 1;
      }
      .article-card {
        grid-template-columns: 76px minmax(0, 1fr);
        grid-template-areas:
          ". title"
          "thumb summary"
          "thumb meta";
        gap: 9px;
        min-height: 104px;
        padding: 10px;
      }
      .article-thumb {
        width: 76px;
        height: 57px;
      }
    }
  </style>
</head>
<body>
  <main class="wrap">
    <section class="panel search-panel" aria-label="Archive search">
      <div class="search-title">
        <h1>AGA E-Journal</h1>
        <div class="archive-state" id="archiveState">Loading archive...</div>
      </div>
      <form class="search-grid" id="searchForm">
        <div class="field">
          <label for="keyword">Keywords</label>
          <input id="keyword" name="q" type="search" autocomplete="off">
        </div>
        <div class="field">
          <label for="month">Month</label>
          <select id="month" name="month">
            <option value=""></option>
            <option value="1">January</option>
            <option value="2">February</option>
            <option value="3">March</option>
            <option value="4">April</option>
            <option value="5">May</option>
            <option value="6">June</option>
            <option value="7">July</option>
            <option value="8">August</option>
            <option value="9">September</option>
            <option value="10">October</option>
            <option value="11">November</option>
            <option value="12">December</option>
          </select>
        </div>
        <div class="field">
          <label for="year">Year</label>
          <input id="year" name="year" type="number" min="1900" max="2100" inputmode="numeric">
        </div>
        <button class="action" id="searchButton" type="submit">Search</button>
        <button class="action secondary" id="resetButton" type="button">Reset</button>
      </form>
    </section>

    <section class="workspace" aria-label="Archive results and article preview">
      <section class="panel results-panel" aria-label="Article results">
        <div class="section-tools">
          <div class="section-title">
            <strong>Articles</strong>
            <span id="resultCount"></span>
          </div>
          <div class="pager" aria-label="Article pages">
            <button id="prevPage" type="button" data-page-action="previous">Previous</button>
            <button id="nextPage" type="button" data-page-action="next">Next</button>
          </div>
        </div>
        <div class="results" id="results"></div>
        <div class="section-tools section-tools-bottom">
          <div class="section-title">
            <strong>Articles</strong>
            <span id="resultCountBottom"></span>
          </div>
          <div class="pager" aria-label="Article pages">
            <button id="prevPageBottom" type="button" data-page-action="previous">Previous</button>
            <button id="nextPageBottom" type="button" data-page-action="next">Next</button>
          </div>
        </div>
      </section>
    </section>
  </main>

  <script>
    const state = {
      articles: [],
      page: 0,
      offset: 0,
      pageSize: 7,
      totalCount: 0,
      hasPrevious: false,
      hasNext: false,
      selectedId: null,
      initialSnapshotHandled: false,
      requestId: 0
    };

    const formEl = document.getElementById('searchForm');
    const keywordEl = document.getElementById('keyword');
    const monthEl = document.getElementById('month');
    const yearEl = document.getElementById('year');
    const resultsEl = document.getElementById('results');
    const resultCountEl = document.getElementById('resultCount');
    const resultCountBottomEl = document.getElementById('resultCountBottom');
    const archiveStateEl = document.getElementById('archiveState');
    const prevPageEls = Array.from(document.querySelectorAll('[data-page-action="previous"]'));
    const nextPageEls = Array.from(document.querySelectorAll('[data-page-action="next"]'));
    const searchButtonEl = document.getElementById('searchButton');
    const resetButtonEl = document.getElementById('resetButton');
    const initialParams = new URLSearchParams(window.location.search);
    const pendingSnapshotKey = 'agaNewsArchivePendingSnapshot';
    const copyToolsEnabled = (() => {
      if (!initialParams.has('ej') && !initialParams.has('emailCopy') && !initialParams.has('copyTools')) return false;
      const value = (initialParams.get('ej') || initialParams.get('emailCopy') || initialParams.get('copyTools') || '1').trim().toLowerCase();
      return !['0', 'false', 'no', 'off'].includes(value);
    })();

    function parseSnapshotId(value) {
      const raw = value == null ? '' : String(value).trim();
      return /^\\d+$/.test(raw) ? Number(raw) : null;
    }

    function takePendingSnapshotId() {
      const maxAgeMs = 5 * 60 * 1000;
      const stores = [];
      try {
        stores.push(window.sessionStorage);
      } catch (error) {}
      try {
        stores.push(window.localStorage);
      } catch (error) {}
      for (const store of stores) {
        let raw = '';
        try {
          raw = store.getItem(pendingSnapshotKey) || '';
        } catch (error) {
          continue;
        }
        if (!raw) continue;
        try {
          store.removeItem(pendingSnapshotKey);
        } catch (error) {}
        try {
          const payload = JSON.parse(raw);
          const snapshotId = parseSnapshotId(payload && payload.snapshotId);
          const createdAt = Number(payload && payload.createdAt);
          if (snapshotId && (!createdAt || Date.now() - createdAt <= maxAgeMs)) {
            return snapshotId;
          }
        } catch (error) {
          const snapshotId = parseSnapshotId(raw);
          if (snapshotId) return snapshotId;
        }
      }
      return null;
    }

    const initialSnapshotId = (() => {
      return parseSnapshotId(initialParams.get('snapshotId') || initialParams.get('snapshot') || initialParams.get('sid'))
        || takePendingSnapshotId();
    })();

    keywordEl.value = initialParams.get('q') || initialParams.get('keyword') || '';
    if (/^(?:[1-9]|1[0-2])$/.test(initialParams.get('month') || '')) {
      monthEl.value = initialParams.get('month');
    }
    if (/^\\d{4}$/.test(initialParams.get('year') || '')) {
      yearEl.value = initialParams.get('year');
    }

    function text(value) {
      return value == null ? '' : String(value);
    }

    function escapeHtml(value) {
      return text(value).replace(/[&<>"']/g, character => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
      }[character]));
    }

    function withQuery(path, params = {}) {
      const query = new URLSearchParams(params);
      const suffix = query.toString();
      return suffix ? `${path}?${suffix}` : path;
    }

    const accessParams = (() => {
      const source = initialParams;
      const params = {};
      const flagEnabled = (...names) => names.some(name => {
        if (!source.has(name)) return false;
        const value = (source.get(name) || '').trim().toLowerCase();
        return !['0', 'false', 'no', 'off'].includes(value);
      });
      if (flagEnabled('includeInactive', 'include_inactive')) {
        params.includeInactive = '1';
      }
      if (flagEnabled('includeNotVisible', 'include_not_visible', 'includeHidden', 'include_hidden')) {
        params.includeNotVisible = '1';
      }
      return params;
    })();

    function filters() {
      return {
        q: keywordEl.value.trim(),
        month: monthEl.value,
        year: yearEl.value.trim()
      };
    }

    function requestParams(offset, options = {}) {
      const params = { offset: String(Math.max(0, offset || 0)), ...accessParams };
      if (options.anchorSnapshotId) {
        params.anchorSnapshotId = String(options.anchorSnapshotId);
      }
      for (const [name, value] of Object.entries(filters())) {
        if (value) params[name] = value;
      }
      return params;
    }

    function articleUrl(snapshotId) {
      return withQuery(`/api/news-archive/article/${snapshotId}`, accessParams);
    }

    function articleEmailHtmlUrl(snapshotId) {
      return withQuery(`/api/news-archive/article/${snapshotId}/email-html`, accessParams);
    }

    function articleListThumbnailUrl(article) {
      if (text(article.Category).trim() === 'External Go News Digest') {
        return '/api/news-archive/static/digest-card-image';
      }
      const thumbnailId = parseSnapshotId(article.Thumbnail_Asset_ID);
      if (thumbnailId) {
        return withQuery(`/api/news-archive/asset/${thumbnailId}`, accessParams);
      }
      return '/api/news-archive/static/default-card-image';
    }

    function cleanViewerArticleUrl(snapshotId) {
      return new URL(withQuery('/api/news-archive', { snapshotId: String(snapshotId) }), window.location.origin).toString();
    }

    function updateSelectedArticleUrl(snapshotId) {
      const parsedSnapshotId = parseSnapshotId(snapshotId);
      if (!parsedSnapshotId || !window.history || !window.history.replaceState) return;
      const url = new URL(window.location.href);
      url.searchParams.set('snapshotId', String(parsedSnapshotId));
      window.history.replaceState({}, '', url);
    }

    function formatDate(value) {
      const raw = text(value);
      if (!raw) return '';
      const datePart = raw.slice(0, 10);
      const parsed = new Date(`${datePart}T00:00:00Z`);
      if (Number.isNaN(parsed.getTime())) return raw;
      return parsed.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric', timeZone: 'UTC' });
    }

    function metaParts(article) {
      return [
        formatDate(article.Active_Date),
        article.Author,
        article.Category,
        `${text(article.Asset_Captured_Count || 0)} assets`
      ].map(text).filter(Boolean);
    }

    function pageBounds() {
      if (!state.totalCount || !state.articles.length) return { start: 0, end: 0 };
      return {
        start: state.offset + 1,
        end: state.offset + state.articles.length
      };
    }

    function setBusy(isBusy) {
      searchButtonEl.disabled = isBusy;
      resetButtonEl.disabled = isBusy;
      setPagerDisabled(prevPageEls, isBusy || !state.hasPrevious);
      setPagerDisabled(nextPageEls, isBusy || !state.hasNext);
      archiveStateEl.textContent = isBusy ? 'Loading archive...' : `${state.totalCount.toLocaleString()} articles`;
    }

    function renderPager() {
      const bounds = pageBounds();
      let resultText = '';
      if (!state.totalCount) {
        resultText = 'No articles';
      } else {
        resultText = `Showing ${bounds.start.toLocaleString()}-${bounds.end.toLocaleString()} of ${state.totalCount.toLocaleString()}`;
      }
      resultCountEl.textContent = resultText;
      resultCountBottomEl.textContent = resultText;
      setPagerDisabled(prevPageEls, !state.hasPrevious);
      setPagerDisabled(nextPageEls, !state.hasNext);
      archiveStateEl.textContent = `${state.totalCount.toLocaleString()} articles`;
    }

    function setPagerDisabled(buttons, disabled) {
      for (const button of buttons) button.disabled = disabled;
    }

    function renderList() {
      resultsEl.innerHTML = '';
      if (!state.articles.length) {
        resultsEl.innerHTML = '<div class="empty">No articles found.</div>';
        return;
      }
      for (const article of state.articles) {
        const isSelected = article.News_Article_Snapshot_ID === state.selectedId;
        const button = document.createElement('button');
        const title = text(article.Headline || `Item ${article.Item_ID}`);
        const summary = text(article.Summary).trim();
        button.className = 'article-card' + (isSelected ? ' selected' : '');
        button.type = 'button';
        button.innerHTML = `
          <span class="article-thumb"><img alt="" loading="lazy"></span>
          <span class="article-title"></span>
          <span class="summary">${escapeHtml(summary || 'No summary available.')}</span>
          <span class="article-meta">${metaParts(article).map(part => `<span>${escapeHtml(part)}</span>`).join('')}</span>
        `;
        button.querySelector('.article-thumb img').src = articleListThumbnailUrl(article);
        button.querySelector('.article-title').textContent = title;
        button.addEventListener('click', () => selectArticle(article));
        resultsEl.appendChild(button);
        if (isSelected) {
          resultsEl.appendChild(renderInlinePreview(article));
        }
      }
    }

    function renderInlinePreview(article) {
      const snapshotId = article.News_Article_Snapshot_ID;
      const title = text(article.Headline || `Item ${article.Item_ID}`).trim();
      const preview = document.createElement('div');
      preview.className = 'inline-preview';
      if (copyToolsEnabled) {
        const tools = document.createElement('div');
        tools.className = 'preview-tools';
        const status = document.createElement('span');
        status.className = 'copy-status';
        const linkButton = document.createElement('button');
        linkButton.type = 'button';
        linkButton.textContent = 'Copy article link';
        linkButton.addEventListener('click', () => copyArticleLink(snapshotId, linkButton, status));
        const titleButton = document.createElement('button');
        titleButton.type = 'button';
        titleButton.textContent = 'Copy title';
        titleButton.addEventListener('click', () => copyArticleTitle(title, titleButton, status));
        const htmlButton = document.createElement('button');
        htmlButton.type = 'button';
        htmlButton.textContent = 'Copy E-Journal HTML';
        htmlButton.addEventListener('click', () => copyArticleEmailHtml(snapshotId, htmlButton, status));
        tools.append(status, linkButton, titleButton, htmlButton);
        preview.appendChild(tools);
      }
      const iframe = document.createElement('iframe');
      iframe.title = 'Archived article preview';
      iframe.allow = 'web-share';
      iframe.src = articleUrl(snapshotId);
      attachInlinePreviewAutoResize(iframe);
      preview.appendChild(iframe);
      return preview;
    }

    function attachInlinePreviewAutoResize(iframe) {
      const resize = () => resizeInlinePreviewFrame(iframe);
      iframe.addEventListener('load', () => {
        resize();
        try {
          const doc = iframe.contentDocument;
          if (!doc) return;
          for (const image of doc.querySelectorAll('img')) {
            if (!image.complete) image.addEventListener('load', resize, { once: true });
          }
          if ('ResizeObserver' in window && doc.body) {
            const observer = new ResizeObserver(resize);
            observer.observe(doc.body);
            if (doc.documentElement) observer.observe(doc.documentElement);
            iframe.inlinePreviewResizeObserver = observer;
          }
        } catch (error) {}
        window.setTimeout(resize, 100);
        window.setTimeout(resize, 500);
        window.setTimeout(resize, 1500);
      });
    }

    function resizeInlinePreviewFrame(iframe) {
      try {
        const doc = iframe.contentDocument;
        if (!doc) return;
        const body = doc.body;
        const root = doc.documentElement;
        const height = Math.max(
          body ? body.scrollHeight : 0,
          body ? body.offsetHeight : 0,
          root ? root.scrollHeight : 0,
          root ? root.offsetHeight : 0
        );
        iframe.style.height = `${Math.max(180, Math.ceil(height) + 2)}px`;
      } catch (error) {}
    }

    async function copyArticleLink(snapshotId, button, status) {
      const url = cleanViewerArticleUrl(snapshotId);
      await runCopyAction(button, status, 'Copying link...', 'Article link copied.', async () => {
        await copyTextToClipboard(url);
      });
    }

    async function copyArticleTitle(title, button, status) {
      await runCopyAction(button, status, 'Copying title...', 'Article title copied.', async () => {
        await copyTextToClipboard(title);
      });
    }

    async function copyArticleEmailHtml(snapshotId, button, status) {
      await runCopyAction(button, status, 'Copying article HTML...', 'Article HTML copied for E-Journal paste.', async () => {
        const response = await fetch(articleEmailHtmlUrl(snapshotId));
        if (!response.ok) throw new Error(await response.text());
        const payload = await response.json();
        await copyHtmlToClipboard(payload.html || '', payload.text || '');
      });
    }

    async function runCopyAction(button, status, busyText, doneText, action) {
      const previousText = button.textContent;
      button.disabled = true;
      status.textContent = busyText;
      try {
        await action();
        status.textContent = doneText;
      } catch (error) {
        status.textContent = `Copy failed: ${error.message || error}`;
      } finally {
        button.disabled = false;
        button.textContent = previousText;
      }
    }

    async function copyTextToClipboard(value) {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(value);
        return;
      }
      copyTextWithSelection(value);
    }

    async function copyHtmlToClipboard(htmlValue, plainText) {
      if (!htmlValue) throw new Error('Article HTML is empty.');
      if (navigator.clipboard && window.ClipboardItem) {
        const item = new ClipboardItem({
          'text/html': new Blob([htmlValue], { type: 'text/html' }),
          'text/plain': new Blob([plainText || htmlValue], { type: 'text/plain' })
        });
        await navigator.clipboard.write([item]);
        return;
      }
      copyHtmlWithSelection(htmlValue);
    }

    function copyHtmlWithSelection(htmlValue) {
      const container = document.createElement('div');
      container.contentEditable = 'true';
      container.style.position = 'fixed';
      container.style.left = '-99999px';
      container.style.top = '0';
      container.style.width = '800px';
      container.innerHTML = htmlValue;
      document.body.appendChild(container);
      const selection = window.getSelection();
      const range = document.createRange();
      range.selectNodeContents(container);
      selection.removeAllRanges();
      selection.addRange(range);
      const ok = document.execCommand('copy');
      selection.removeAllRanges();
      container.remove();
      if (!ok) throw new Error('Clipboard permission was denied.');
    }

    function copyTextWithSelection(value) {
      const textarea = document.createElement('textarea');
      textarea.value = value;
      textarea.style.position = 'fixed';
      textarea.style.left = '-99999px';
      textarea.style.top = '0';
      document.body.appendChild(textarea);
      textarea.focus();
      textarea.select();
      const ok = document.execCommand('copy');
      textarea.remove();
      if (!ok) throw new Error('Clipboard permission was denied.');
    }

    function clearPreview() {
      state.selectedId = null;
    }

    function selectArticle(article) {
      state.selectedId = article.News_Article_Snapshot_ID;
      updateSelectedArticleUrl(state.selectedId);
      renderList();
    }

    function selectSnapshotId(snapshotId) {
      state.selectedId = snapshotId;
      updateSelectedArticleUrl(state.selectedId);
      renderList();
    }

    async function loadArticles(offset = state.offset, options = {}) {
      const requestId = ++state.requestId;
      setBusy(true);
      try {
        const response = await fetch(withQuery('/api/news-archive/articles', requestParams(offset, options)));
        if (!response.ok) throw new Error(await response.text());
        const payload = await response.json();
        if (requestId !== state.requestId) return;
        state.articles = payload.articles || [];
        state.page = Number(payload.page || 0);
        state.offset = Number(payload.offset || 0);
        state.pageSize = Number(payload.pageSize || 10);
        state.totalCount = Number(payload.totalCount || 0);
        state.hasPrevious = Boolean(payload.hasPrevious);
        state.hasNext = Boolean(payload.hasNext);
        renderPager();
        renderList();
        if (initialSnapshotId && !state.initialSnapshotHandled) {
          state.initialSnapshotHandled = true;
          const matching = state.articles.find(article => Number(article.News_Article_Snapshot_ID) === initialSnapshotId);
          if (matching) {
            selectArticle(matching);
          } else {
            selectSnapshotId(initialSnapshotId);
          }
        } else if (state.articles.length) {
          selectArticle(state.articles[0]);
        } else {
          clearPreview();
        }
      } catch (error) {
        if (requestId !== state.requestId) return;
        state.articles = [];
        state.totalCount = 0;
        state.hasPrevious = false;
        state.hasNext = false;
        renderPager();
        resultsEl.innerHTML = `<div class="empty error">${escapeHtml(error.message || String(error))}</div>`;
        clearPreview();
      } finally {
        if (requestId === state.requestId) setBusy(false);
      }
    }

    formEl.addEventListener('submit', event => {
      event.preventDefault();
      loadArticles(0);
    });

    resetButtonEl.addEventListener('click', () => {
      formEl.reset();
      loadArticles(0);
    });

    for (const button of prevPageEls) button.addEventListener('click', () => {
      if (state.hasPrevious) loadArticles(Math.max(0, state.offset - state.pageSize));
    });

    for (const button of nextPageEls) button.addEventListener('click', () => {
      if (state.hasNext) loadArticles(state.offset + state.pageSize);
    });

    loadArticles(0, initialSnapshotId ? { anchorSnapshotId: initialSnapshotId } : {});
  </script>
</body>
</html>
"""
