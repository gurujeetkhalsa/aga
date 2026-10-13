# Copyright 2026, American Go Association, All rights reserved

from __future__ import annotations

import argparse
import html
import json
import mimetypes
import os
import re
import sys
from datetime import date, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

try:
    from azure.storage.blob import BlobServiceClient
except Exception:  # pragma: no cover - exercised in deployment environments.
    BlobServiceClient = None


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bayrate.sql_adapter import SqlAdapter, get_sql_connection_string  # noqa: E402


DEFAULT_CONTAINER = "clubexpress-news-archive"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8767
DEFAULT_TOP = 200


LIST_ARTICLES_SQL = """
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
SELECT TOP ({top})
    s.[News_Article_Snapshot_ID],
    a.[News_Article_ID],
    a.[Item_ID],
    COALESCE(s.[Headline], a.[Headline]) AS [Headline],
    COALESCE(s.[Author], a.[Author]) AS [Author],
    COALESCE(s.[Category], a.[Category]) AS [Category],
    COALESCE(s.[Status], a.[Current_Status]) AS [Status],
    COALESCE(s.[Visibility], a.[Visibility]) AS [Visibility],
    COALESCE(s.[Active_Date], a.[Active_Date]) AS [Active_Date],
    s.[Rendered_Text_Length],
    s.[Rendered_Article_Image_Count],
    s.[Rendered_Article_Link_Count],
    s.[Asset_Captured_Count],
    s.[Blob_Prefix],
    r.[Run_Key]
FROM RankedSnapshots AS s
INNER JOIN [clubexpress].[news_articles] AS a
    ON a.[News_Article_ID] = s.[News_Article_ID]
INNER JOIN [clubexpress].[news_archive_runs] AS r
    ON r.[News_Archive_Run_ID] = s.[News_Archive_Run_ID]
WHERE s.[Snapshot_Rank] = 1
ORDER BY COALESCE(s.[Active_Date], a.[Active_Date]) DESC, a.[Item_ID] DESC;
"""


GET_SNAPSHOT_SQL = """
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
    r.[Run_Key],
    r.[Blob_Container]
FROM [clubexpress].[news_article_snapshots] AS s
INNER JOIN [clubexpress].[news_articles] AS a
    ON a.[News_Article_ID] = s.[News_Article_ID]
INNER JOIN [clubexpress].[news_archive_runs] AS r
    ON r.[News_Archive_Run_ID] = s.[News_Archive_Run_ID]
WHERE s.[News_Article_Snapshot_ID] = ?;
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


GET_ASSET_SQL = """
SELECT
    [News_Article_Asset_ID],
    [Blob_Path],
    [File_Name],
    [Content_Type]
FROM [clubexpress].[news_article_assets]
WHERE [News_Article_Asset_ID] = ?;
"""


class ArchiveStore:
    """Read archived ClubExpress news articles from Azure SQL and Blob Storage."""

    def __init__(
        self,
        *,
        sql_connection_string: str,
        storage_connection_string: str,
        container_name: str,
        top: int,
    ) -> None:
        """Initialize the archive store."""
        if BlobServiceClient is None:
            raise RuntimeError("azure-storage-blob is not available.")
        self.adapter = SqlAdapter(sql_connection_string)
        self.container_name = container_name
        self.top = top
        service = BlobServiceClient.from_connection_string(storage_connection_string)
        self.container = service.get_container_client(container_name)

    def list_articles(self) -> list[dict[str, Any]]:
        """Return latest archived article snapshots."""
        rows = self.adapter.query_rows(LIST_ARTICLES_SQL.format(top=self.top))
        return [json_safe_row(row) for row in rows]

    def get_snapshot(self, snapshot_id: int) -> dict[str, Any] | None:
        """Return one article snapshot."""
        rows = self.adapter.query_rows(GET_SNAPSHOT_SQL, (snapshot_id,))
        return json_safe_row(rows[0]) if rows else None

    def get_assets(self, snapshot_id: int) -> list[dict[str, Any]]:
        """Return assets for one snapshot."""
        return [json_safe_row(row) for row in self.adapter.query_rows(GET_ASSETS_SQL, (snapshot_id,))]

    def get_asset(self, asset_id: int) -> dict[str, Any] | None:
        """Return one asset row."""
        rows = self.adapter.query_rows(GET_ASSET_SQL, (asset_id,))
        return json_safe_row(rows[0]) if rows else None

    def read_blob_text(self, blob_path: str) -> str:
        """Read a UTF-8-ish text blob."""
        data = self.read_blob_bytes(blob_path)
        return data.decode("utf-8", errors="replace")

    def read_blob_bytes(self, blob_path: str) -> bytes:
        """Read a blob as bytes."""
        return self.container.download_blob(blob_path).readall()


class ArchiveViewerHandler(BaseHTTPRequestHandler):
    """Serve the local news archive test viewer."""

    store: ArchiveStore

    def do_GET(self) -> None:  # noqa: N802 - stdlib method name.
        """Handle GET requests."""
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/":
                self.send_html(INDEX_HTML)
                return
            if parsed.path == "/api/articles":
                self.send_json({"articles": self.store.list_articles()})
                return
            if parsed.path.startswith("/article/"):
                snapshot_id = parse_int_path(parsed.path, "/article/")
                self.send_article(snapshot_id)
                return
            if parsed.path.startswith("/asset/"):
                asset_id = parse_int_path(parsed.path, "/asset/")
                self.send_asset(asset_id)
                return
            if parsed.path == "/health":
                self.send_json({"ok": True})
                return
            if parsed.path == "/api/article":
                params = parse_qs(parsed.query)
                snapshot_id = int(params.get("id", ["0"])[0])
                self.send_article_json(snapshot_id)
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
        except ValueError:
            self.send_error(HTTPStatus.BAD_REQUEST, "Bad request")
        except Exception as exc:  # pragma: no cover - keeps local app debuggable.
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))

    def log_message(self, format_text: str, *args: Any) -> None:
        """Write compact request logs."""
        print(f"{self.address_string()} - {format_text % args}")

    def send_article_json(self, snapshot_id: int) -> None:
        """Send article metadata as JSON."""
        snapshot = self.store.get_snapshot(snapshot_id)
        if not snapshot:
            self.send_error(HTTPStatus.NOT_FOUND, "Article not found")
            return
        assets = self.store.get_assets(snapshot_id)
        self.send_json({"article": snapshot, "assets": assets})

    def send_article(self, snapshot_id: int) -> None:
        """Send the article display page."""
        snapshot = self.store.get_snapshot(snapshot_id)
        if not snapshot:
            self.send_error(HTTPStatus.NOT_FOUND, "Article not found")
            return
        blob_path = snapshot.get("Rendered_Article_Blob_Path") or snapshot.get("Rendered_Container_Blob_Path")
        if not blob_path:
            self.send_error(HTTPStatus.NOT_FOUND, "Article HTML blob not found")
            return
        article_html = self.store.read_blob_text(str(blob_path))
        article_html = rewrite_external_links(article_html)
        assets = self.store.get_assets(snapshot_id)
        article_html = rewrite_article_assets(article_html, assets)
        self.send_html(render_article_page(snapshot, article_html, assets))

    def send_asset(self, asset_id: int) -> None:
        """Send an archived asset blob."""
        asset = self.store.get_asset(asset_id)
        if not asset or not asset.get("Blob_Path"):
            self.send_error(HTTPStatus.NOT_FOUND, "Asset not found")
            return
        content = self.store.read_blob_bytes(str(asset["Blob_Path"]))
        content_type = (
            asset.get("Content_Type")
            or mimetypes.guess_type(str(asset.get("File_Name") or ""))[0]
            or "application/octet-stream"
        )
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", str(content_type))
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "private, max-age=300")
        self.end_headers()
        self.wfile.write(content)

    def send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        """Send a JSON response."""
        content = json.dumps(payload, default=json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content)

    def send_html(self, body: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        """Send an HTML response."""
        content = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content)


def main(argv: list[str] | None = None) -> int:
    """Run the local test viewer."""
    parser = build_parser()
    args = parser.parse_args(argv)
    sql_connection_string = args.connection_string or get_sql_connection_string()
    storage_connection_string = (
        args.storage_connection_string
        or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        or os.environ.get("AzureWebJobsStorage")
    )
    if not sql_connection_string:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2
    if not storage_connection_string:
        print(
            "Missing storage connection string. Set AZURE_STORAGE_CONNECTION_STRING, AzureWebJobsStorage, "
            "or pass --storage-connection-string.",
            file=sys.stderr,
        )
        return 2

    handler_class = ArchiveViewerHandler
    handler_class.store = ArchiveStore(
        sql_connection_string=sql_connection_string,
        storage_connection_string=storage_connection_string,
        container_name=args.container,
        top=args.top,
    )
    server = ThreadingHTTPServer((args.host, args.port), handler_class)
    print(f"News archive viewer listening on http://{args.host}:{args.port}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Stopping news archive viewer.")
    finally:
        server.server_close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--top", type=positive_int, default=DEFAULT_TOP)
    parser.add_argument("--container", default=DEFAULT_CONTAINER)
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING.")
    parser.add_argument(
        "--storage-connection-string",
        help="Blob Storage connection string. Defaults to AZURE_STORAGE_CONNECTION_STRING or AzureWebJobsStorage.",
    )
    return parser


def positive_int(value: str) -> int:
    """Parse a positive integer."""
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("Value must be positive.")
    return parsed


def parse_int_path(path: str, prefix: str) -> int:
    """Parse a numeric path suffix."""
    suffix = path[len(prefix) :].strip("/")
    if not re.fullmatch(r"\d+", suffix):
        raise ValueError(f"Expected numeric path after {prefix}")
    return int(suffix)


def render_article_page(snapshot: dict[str, Any], article_html: str, assets: list[dict[str, Any]]) -> str:
    """Render a single article display page."""
    headline = html.escape(str(snapshot.get("Headline") or "Untitled"))
    author = html.escape(str(snapshot.get("Author") or ""))
    active_date = html.escape(str(snapshot.get("Active_Date") or ""))
    status = html.escape(str(snapshot.get("Status") or ""))
    source_url = html.escape(str(snapshot.get("View_Url") or ""))
    image_count = int(snapshot.get("Rendered_Article_Image_Count") or 0)
    link_count = int(snapshot.get("Rendered_Article_Link_Count") or 0)
    captured_count = sum(1 for asset in assets if asset.get("Capture_Status") == "captured")
    source_link = f'<a href="{source_url}" target="_blank" rel="noopener">ClubExpress</a>' if source_url else ""
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{headline}</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #17202a;
      --muted: #64717f;
      --line: #d8dee6;
      --accent: #1b6f5c;
      --paper: #ffffff;
      --wash: #f6f8f9;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--paper);
      color: var(--ink);
      font: 15px/1.55 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    header {{
      border-bottom: 1px solid var(--line);
      padding: 18px 22px 14px;
      background: var(--wash);
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 24px;
      line-height: 1.2;
      letter-spacing: 0;
    }}
    .meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px 14px;
      color: var(--muted);
      font-size: 13px;
    }}
    .meta strong {{ color: var(--ink); font-weight: 600; }}
    main {{
      max-width: 900px;
      margin: 0 auto;
      padding: 22px;
    }}
    article :is(img, video) {{
      max-width: 100%;
      height: auto;
    }}
    article img {{
      border: 1px solid var(--line);
      background: white;
    }}
    article a {{
      color: var(--accent);
    }}
    @media (max-width: 720px) {{
      main {{ padding: 16px; }}
      h1 {{ font-size: 20px; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>{headline}</h1>
    <div class="meta">
      <span><strong>{author}</strong></span>
      <span>{active_date}</span>
      <span>{status}</span>
      <span>{image_count} images</span>
      <span>{captured_count} archived</span>
      <span>{link_count} links</span>
      <span>{source_link}</span>
    </div>
  </header>
  <main>
    <article>{article_html}</article>
  </main>
</body>
</html>"""


def rewrite_article_assets(article_html: str, assets: list[dict[str, Any]]) -> str:
    """Point captured article images at the local asset proxy."""
    rewritten = article_html
    for asset in assets:
        if asset.get("Capture_Status") != "captured" or not asset.get("Blob_Path"):
            continue
        source_url = normalize_url(str(asset.get("Source_Url") or ""))
        if not source_url:
            continue
        local_url = f"/asset/{asset['News_Article_Asset_ID']}"
        for variant in asset_url_variants(source_url):
            rewritten = rewritten.replace(variant, local_url)
            rewritten = rewritten.replace(html.escape(variant, quote=True), local_url)
    return rewritten


def rewrite_external_links(article_html: str) -> str:
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


def asset_url_variants(source_url: str) -> set[str]:
    """Return likely URL variants used by ClubExpress editors/renderers."""
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


def normalize_url(value: str) -> str:
    """Normalize common URL forms."""
    text = value.strip()
    if text.startswith("//"):
        return "https:" + text
    return text


def json_safe_row(row: dict[str, Any]) -> dict[str, Any]:
    """Convert row values to JSON-safe values."""
    return {key: json_default(value) for key, value in row.items()}


def json_default(value: Any) -> Any:
    """Serialize values not handled by JSON's default encoder."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>News Archive Test Viewer</title>
  <style>
    :root {
      color-scheme: light;
      --ink: #17202a;
      --muted: #667380;
      --line: #d9e0e7;
      --panel: #f6f8f9;
      --paper: #ffffff;
      --accent: #1b6f5c;
      --accent-soft: #e6f3ef;
      --warning: #8b5a12;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--ink);
      background: var(--paper);
      font: 14px/1.4 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    .shell {
      display: grid;
      grid-template-columns: minmax(300px, 380px) minmax(0, 1fr);
      min-height: 100vh;
    }
    aside {
      border-right: 1px solid var(--line);
      background: var(--panel);
      min-height: 100vh;
      display: flex;
      flex-direction: column;
    }
    .bar {
      padding: 14px;
      border-bottom: 1px solid var(--line);
      display: grid;
      gap: 10px;
    }
    h1 {
      margin: 0;
      font-size: 16px;
      letter-spacing: 0;
    }
    input {
      width: 100%;
      height: 34px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 0 10px;
      font: inherit;
      background: white;
      color: var(--ink);
    }
    .count {
      color: var(--muted);
      font-size: 12px;
    }
    .list {
      overflow: auto;
      padding: 8px;
      display: grid;
      gap: 6px;
    }
    .item {
      width: 100%;
      border: 1px solid transparent;
      border-radius: 8px;
      background: transparent;
      color: inherit;
      text-align: left;
      padding: 10px;
      cursor: pointer;
    }
    .item:hover {
      background: white;
      border-color: var(--line);
    }
    .item.selected {
      background: var(--accent-soft);
      border-color: #9bc8bb;
    }
    .headline {
      font-weight: 650;
      line-height: 1.25;
      overflow-wrap: anywhere;
    }
    .item-meta {
      margin-top: 5px;
      color: var(--muted);
      font-size: 12px;
      display: flex;
      flex-wrap: wrap;
      gap: 6px 10px;
    }
    .stage {
      min-width: 0;
      min-height: 100vh;
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
      background: white;
    }
    .stage-top {
      border-bottom: 1px solid var(--line);
      padding: 11px 14px;
      min-height: 45px;
      color: var(--muted);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    .status {
      color: var(--warning);
      font-size: 12px;
    }
    iframe {
      width: 100%;
      height: 100%;
      border: 0;
      background: white;
    }
    @media (max-width: 840px) {
      .shell {
        grid-template-columns: 1fr;
        grid-template-rows: minmax(220px, 40vh) minmax(0, 60vh);
      }
      aside {
        min-height: 0;
        border-right: 0;
        border-bottom: 1px solid var(--line);
      }
      .stage {
        min-height: 0;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <aside>
      <div class="bar">
        <h1>News Archive</h1>
        <input id="search" type="search" placeholder="Search articles" autocomplete="off">
        <div class="count" id="count"></div>
      </div>
      <div class="list" id="list"></div>
    </aside>
    <section class="stage">
      <div class="stage-top">
        <div id="selectedTitle">Loading archive...</div>
        <div class="status" id="status"></div>
      </div>
      <iframe id="frame" title="Archived article preview"></iframe>
    </section>
  </div>
  <script>
    const listEl = document.getElementById('list');
    const countEl = document.getElementById('count');
    const frameEl = document.getElementById('frame');
    const searchEl = document.getElementById('search');
    const selectedTitleEl = document.getElementById('selectedTitle');
    const statusEl = document.getElementById('status');
    let articles = [];
    let selectedId = null;

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

    function articleMatches(article, query) {
      const haystack = [
        article.Headline,
        article.Author,
        article.Category,
        article.Status,
        article.Active_Date,
        article.Item_ID
      ].map(text).join(' ').toLowerCase();
      return haystack.includes(query.toLowerCase());
    }

    function renderList() {
      const query = searchEl.value.trim();
      const filtered = query ? articles.filter(article => articleMatches(article, query)) : articles;
      countEl.textContent = `${filtered.length} of ${articles.length} articles`;
      listEl.innerHTML = '';
      for (const article of filtered) {
        const button = document.createElement('button');
        button.className = 'item' + (article.News_Article_Snapshot_ID === selectedId ? ' selected' : '');
        button.type = 'button';
        button.innerHTML = `
          <div class="headline"></div>
          <div class="item-meta">
            <span>${escapeHtml(article.Active_Date)}</span>
            <span>${escapeHtml(article.Author)}</span>
            <span>${escapeHtml(article.Status)}</span>
            <span>${escapeHtml(article.Asset_Captured_Count)} assets</span>
          </div>
        `;
        button.querySelector('.headline').textContent = text(article.Headline || `Item ${article.Item_ID}`);
        button.addEventListener('click', () => selectArticle(article));
        listEl.appendChild(button);
      }
    }

    function selectArticle(article) {
      selectedId = article.News_Article_Snapshot_ID;
      selectedTitleEl.textContent = article.Headline || `Item ${article.Item_ID}`;
      statusEl.textContent = `${text(article.Active_Date)} ${text(article.Status)}`.trim();
      frameEl.src = `/article/${selectedId}`;
      renderList();
    }

    async function loadArticles() {
      try {
        const response = await fetch('/api/articles');
        if (!response.ok) throw new Error(await response.text());
        const payload = await response.json();
        articles = payload.articles || [];
        renderList();
        if (articles.length) {
          selectArticle(articles[0]);
        } else {
          selectedTitleEl.textContent = 'No archived articles';
        }
      } catch (error) {
        selectedTitleEl.textContent = 'Archive unavailable';
        statusEl.textContent = error.message || String(error);
      }
    }

    searchEl.addEventListener('input', renderList);
    loadArticles();
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    raise SystemExit(main())
