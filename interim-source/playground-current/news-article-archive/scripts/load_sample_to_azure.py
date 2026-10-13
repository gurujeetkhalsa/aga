# Copyright 2026, American Go Association, All rights reserved

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import re
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib import error, request
from urllib.parse import urlparse

try:
    from azure.core.exceptions import ResourceExistsError
    from azure.identity import AzureCliCredential
    from azure.storage.blob import BlobServiceClient, ContentSettings
except Exception:  # pragma: no cover - exercised in deployment environments.
    ResourceExistsError = None
    AzureCliCredential = None
    BlobServiceClient = None
    ContentSettings = None


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bayrate import sql_adapter as bayrate_sql  # noqa: E402
from bayrate.sql_adapter import SqlAdapter, get_sql_connection_string  # noqa: E402


DEFAULT_SCHEMA_PATH = REPO_ROOT / "news-article-archive" / "sql" / "news_archive_schema.sql"
DEFAULT_SAMPLE_DIR = REPO_ROOT / "news-article-archive-samples" / "2026-07-04-recent-native-40"
DEFAULT_STORAGE_ACCOUNT = "agafunc0522"
DEFAULT_CONTAINER = "clubexpress-news-archive"
DEFAULT_SOURCE_MODE = "recent-native-clubexpress-era"


def main(argv: list[str] | None = None) -> int:
    """Load a captured ClubExpress news sample into Azure Blob Storage and SQL."""
    args = build_parser().parse_args(argv)
    sample_dir = resolve_path(args.sample_dir)
    schema_path = resolve_path(args.schema_file)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_key = args.run_key or f"{sample_dir.name}-{timestamp}"
    default_prefix_leaf = run_key if args.run_key else timestamp
    blob_prefix = clean_blob_path(args.blob_prefix or f"samples/{sample_dir.name}/{default_prefix_leaf}")

    if not sample_dir.exists():
        print(f"Sample directory not found: {sample_dir}", file=sys.stderr)
        return 2
    if args.apply_schema and not schema_path.exists():
        print(f"Schema file not found: {schema_path}", file=sys.stderr)
        return 2

    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2

    adapter = SqlAdapter(conn_str)
    uploaded_paths: dict[str, str] = {}
    upload_stats: dict[str, int] = {}

    if args.apply_schema:
        batch_count = apply_schema(adapter, schema_path)
        print(f"Applied schema batches: {batch_count}")

    existing_run = get_archive_run_by_key(adapter, run_key)
    if existing_run and not args.resume and not args.validate_only:
        print(f"Archive run key already exists. Use --resume to continue it: {run_key}", file=sys.stderr)
        return 2
    if existing_run and not args.blob_prefix:
        blob_prefix = clean_blob_path(str(existing_run.get("Blob_Prefix") or blob_prefix))

    storage_connection_string = (
        args.storage_connection_string
        or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        or os.environ.get("AzureWebJobsStorage")
    )
    container_client = None
    if not args.skip_upload or args.validate_blobs:
        container_client = build_container_client(
            storage_account=args.storage_account,
            storage_connection_string=storage_connection_string,
            container_name=args.container,
        )

    if args.validate_only:
        if not existing_run:
            print(f"Archive run key was not found for validation: {run_key}", file=sys.stderr)
            return 2
        report = validate_archive_run(
            adapter=adapter,
            sample_dir=sample_dir,
            run_id=int(existing_run["News_Archive_Run_ID"]),
            run_key=run_key,
            blob_prefix=blob_prefix,
            container_client=container_client,
            check_external_links=args.check_external_links,
            link_timeout_seconds=args.link_timeout_seconds,
        )
        report_path = write_validation_report(report, args.report_file, args.report_dir, run_key)
        print_validation_summary(report, report_path)
        return 0 if report["valid"] else 1

    if not args.skip_upload:
        uploaded_paths, upload_stats = upload_sample_tree(
            sample_dir=sample_dir,
            container_client=container_client,
            blob_prefix=blob_prefix,
            skip_matching_blobs=not args.no_skip_matching_blobs,
        )
        print(
            "Blob files: "
            f"{upload_stats.get('uploaded', 0)} uploaded, "
            f"{upload_stats.get('skipped', 0)} skipped, "
            f"{upload_stats.get('checked', 0)} checked"
        )

    result = index_sample(
        adapter=adapter,
        sample_dir=sample_dir,
        run_key=run_key,
        run_type=args.run_type,
        source_mode=args.source_mode,
        source_description=args.source_description,
        storage_account=args.storage_account,
        container=args.container,
        blob_prefix=blob_prefix,
        uploaded_paths=uploaded_paths,
        resume=args.resume,
    )

    report = validate_archive_run(
        adapter=adapter,
        sample_dir=sample_dir,
        run_id=int(result["run_id"]),
        run_key=run_key,
        blob_prefix=blob_prefix,
        container_client=container_client if args.validate_blobs else None,
        check_external_links=args.check_external_links,
        link_timeout_seconds=args.link_timeout_seconds,
    )
    report_path = write_validation_report(report, args.report_file, args.report_dir, run_key)

    print(f"Archive run ID: {result['run_id']}")
    print(f"Indexed articles: {result['article_count']}")
    if result.get("skipped_article_count"):
        print(f"Skipped existing articles: {result['skipped_article_count']}")
    print(f"Indexed assets: {result['asset_count']}")
    print(f"Captured assets: {result['captured_asset_count']}")
    print(f"Blob prefix: {blob_prefix}")
    print_validation_summary(report, report_path)
    return 0 if report["valid"] else 1


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample-dir", default=str(DEFAULT_SAMPLE_DIR), help="Captured sample directory to archive.")
    parser.add_argument("--schema-file", default=str(DEFAULT_SCHEMA_PATH), help="SQL schema file to apply.")
    parser.add_argument("--apply-schema", action="store_true", help="Apply the SQL schema before loading data.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    parser.add_argument("--storage-account", default=DEFAULT_STORAGE_ACCOUNT, help="Azure Storage account name.")
    parser.add_argument(
        "--storage-connection-string",
        help="Storage connection string. Defaults to AZURE_STORAGE_CONNECTION_STRING or AzureWebJobsStorage.",
    )
    parser.add_argument("--container", default=DEFAULT_CONTAINER, help="Blob container name.")
    parser.add_argument("--blob-prefix", help="Blob prefix for uploaded sample files.")
    parser.add_argument("--run-key", help="Stable key for this archive run. Defaults to sample name plus UTC timestamp.")
    parser.add_argument("--resume", action="store_true", help="Reuse an existing run key and skip already indexed snapshots.")
    parser.add_argument("--validate-only", action="store_true", help="Validate an existing run key without uploading or indexing.")
    parser.add_argument("--run-type", default="sample", choices=["sample", "manual", "backfill", "scheduled"])
    parser.add_argument("--source-mode", default=DEFAULT_SOURCE_MODE)
    parser.add_argument(
        "--source-description",
        default="Recent native ClubExpress 40-article sample captured on 2026-07-04.",
    )
    parser.add_argument("--skip-upload", action="store_true", help="Index the sample without uploading files.")
    parser.add_argument(
        "--no-skip-matching-blobs",
        action="store_true",
        help="Upload files even when the existing blob has matching sha256 metadata.",
    )
    parser.add_argument("--no-validate-blobs", dest="validate_blobs", action="store_false", help="Skip blob-count validation.")
    parser.set_defaults(validate_blobs=True)
    parser.add_argument("--report-file", help="Exact JSON validation report path.")
    parser.add_argument(
        "--report-dir",
        default=str(REPO_ROOT / "news-article-archive" / "reports"),
        help="Directory for validation reports when --report-file is not set.",
    )
    parser.add_argument("--check-external-links", action="store_true", help="Check rendered article links with HTTP HEAD/GET.")
    parser.add_argument("--link-timeout-seconds", type=float, default=10.0, help="Timeout for each external link check.")
    return parser


def resolve_path(path_text: str | Path) -> Path:
    """Resolve a path relative to the repository root."""
    path = Path(path_text)
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


def clean_blob_path(path_text: str) -> str:
    """Normalize a blob path fragment."""
    return path_text.replace("\\", "/").strip("/")


def apply_schema(adapter: SqlAdapter, schema_path: Path) -> int:
    """Apply a SQL Server schema file that uses GO batch separators."""
    text = schema_path.read_text(encoding="utf-8-sig")
    batches = [
        batch.strip()
        for batch in re.split(r"(?im)^\s*GO\s*;?\s*$", text)
        if batch.strip()
    ]
    for batch in batches:
        if requires_autocommit_schema_batch(batch):
            execute_autocommit_statement(adapter.connection_string, batch)
        else:
            adapter.execute_statements([(batch, ())])
    return len(batches)


def requires_autocommit_schema_batch(batch: str) -> bool:
    """Return whether a schema batch must run outside a user transaction."""
    return "CREATE FULLTEXT" in batch.upper()


def execute_autocommit_statement(connection_string: str, query: str) -> None:
    """Execute one SQL statement in autocommit mode."""
    if bayrate_sql.pyodbc is not None:
        try:
            conn = bayrate_sql.pyodbc.connect(connection_string, autocommit=True)
            try:
                cursor = conn.cursor()
                try:
                    cursor.execute(query)
                finally:
                    cursor.close()
            finally:
                conn.close()
            return
        except Exception:
            if bayrate_sql.pytds is None:
                raise
    if bayrate_sql.pytds is None:
        raise RuntimeError("Neither pyodbc nor python-tds is available for SQL access.")
    sql = bayrate_sql._parse_sql_connection_string(connection_string)
    kwargs = {
        "server": sql["server"],
        "port": sql["port"],
        "database": sql["database"],
        "user": sql["user"],
        "password": sql["password"],
        "validate_host": True,
        "enc_login_only": False,
        "autocommit": True,
        "timeout": 60,
        "as_dict": True,
    }
    if bayrate_sql.certifi is not None:
        kwargs["cafile"] = bayrate_sql.certifi.where()
    conn = bayrate_sql.pytds.connect(**kwargs)
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
        finally:
            cursor.close()
    finally:
        conn.close()


def build_container_client(
    *,
    storage_account: str,
    storage_connection_string: str | None,
    container_name: str,
) -> Any:
    """Build a Blob container client."""
    if AzureCliCredential is None or BlobServiceClient is None or ContentSettings is None:
        raise RuntimeError("Azure SDK packages are not available. Install azure-identity and azure-storage-blob.")

    if storage_connection_string:
        service = BlobServiceClient.from_connection_string(storage_connection_string)
    else:
        credential = AzureCliCredential()
        service = BlobServiceClient(
            account_url=f"https://{storage_account}.blob.core.windows.net",
            credential=credential,
        )
    container = service.get_container_client(container_name)
    try:
        container.create_container(public_access=None)
    except ResourceExistsError:
        pass
    return container


def upload_sample_tree(
    *,
    sample_dir: Path,
    container_client: Any,
    blob_prefix: str,
    skip_matching_blobs: bool,
) -> tuple[dict[str, str], dict[str, int]]:
    """Upload every file in the sample directory and return relative-path to blob-path map."""
    if container_client is None:
        raise RuntimeError("Blob container client is required for upload.")

    uploaded: dict[str, str] = {}
    stats = {
        "checked": 0,
        "uploaded": 0,
        "skipped": 0,
        "overwritten": 0,
    }
    for path in sorted(sample_dir.rglob("*")):
        if not path.is_file():
            continue
        stats["checked"] += 1
        relative_path = path.relative_to(sample_dir).as_posix()
        blob_path = clean_blob_path(f"{blob_prefix}/{relative_path}")
        file_hash = file_sha256(path)
        content_type = guess_content_type(path)
        blob_client = container_client.get_blob_client(blob_path)
        if skip_matching_blobs and blob_sha256_matches(blob_client, file_hash):
            uploaded[relative_path] = blob_path
            stats["skipped"] += 1
            continue
        existed = blob_exists(blob_client)
        with path.open("rb") as handle:
            blob_client.upload_blob(
                data=handle,
                overwrite=True,
                content_settings=ContentSettings(content_type=content_type),
                metadata={"sha256": file_hash or ""},
            )
        uploaded[relative_path] = blob_path
        stats["uploaded"] += 1
        if existed:
            stats["overwritten"] += 1
    return uploaded, stats


def blob_exists(blob_client: Any) -> bool:
    """Return whether a blob exists."""
    try:
        blob_client.get_blob_properties()
        return True
    except Exception:
        return False


def blob_sha256_matches(blob_client: Any, expected_sha256: str | None) -> bool:
    """Return whether existing blob metadata says it already matches the local file."""
    if not expected_sha256:
        return False
    try:
        properties = blob_client.get_blob_properties()
    except Exception:
        return False
    metadata = getattr(properties, "metadata", None) or {}
    return str(metadata.get("sha256") or "").lower() == expected_sha256.lower()


def guess_content_type(path: Path) -> str:
    """Guess a useful content type for blob uploads."""
    if path.suffix.lower() == ".json":
        return "application/json; charset=utf-8"
    if path.suffix.lower() in {".html", ".htm"}:
        return "text/html; charset=utf-8"
    if path.suffix.lower() == ".txt":
        return "text/plain; charset=utf-8"
    return mimetypes.guess_type(path.name)[0] or "application/octet-stream"


def index_sample(
    *,
    adapter: SqlAdapter,
    sample_dir: Path,
    run_key: str,
    run_type: str,
    source_mode: str,
    source_description: str,
    storage_account: str,
    container: str,
    blob_prefix: str,
    uploaded_paths: dict[str, str],
    resume: bool,
) -> dict[str, Any]:
    """Index a captured sample in SQL."""
    manifest = read_json(sample_dir / "sample-manifest.json")
    manifest_blob_path = uploaded_paths.get("sample-manifest.json")
    summary = {
        "sample_type": manifest.get("sampleType"),
        "requested_sample_size": manifest.get("requestedSampleSize"),
        "sample_size": manifest.get("sampleSize"),
        "completed_count": manifest.get("completedCount"),
        "extracted_at": manifest.get("extractedAt"),
    }
    run_id = insert_archive_run(
        adapter=adapter,
        run_key=run_key,
        run_type=run_type,
        source_mode=source_mode,
        source_description=source_description,
        storage_account=storage_account,
        container=container,
        blob_prefix=blob_prefix,
        manifest_blob_path=manifest_blob_path,
        summary=summary,
        resume=resume,
    )

    indexed_article_count = 0
    skipped_article_count = 0
    indexed_asset_count = 0
    indexed_captured_asset_count = 0
    failed = False
    try:
        for article in manifest.get("articles", []):
            folder_name = str(article.get("folder") or article.get("itemId") or "").strip()
            if not folder_name:
                continue
            folder = sample_dir / folder_name
            if not folder.exists():
                continue
            article_result = index_article(
                adapter=adapter,
                run_id=run_id,
                run_key=run_key,
                folder=folder,
                article_summary=article,
                uploaded_paths=uploaded_paths,
                resume=resume,
            )
            if article_result.get("skipped"):
                skipped_article_count += 1
            else:
                indexed_article_count += 1
                indexed_asset_count += article_result["asset_count"]
                indexed_captured_asset_count += article_result["captured_asset_count"]

        run_counts = query_run_counts(adapter, run_id)
        final_summary = {
            **summary,
            "article_count": run_counts["snapshot_count"],
            "asset_count": run_counts["asset_count"],
            "captured_asset_count": run_counts["captured_asset_count"],
            "indexed_article_count": indexed_article_count,
            "skipped_article_count": skipped_article_count,
            "loaded_at": datetime.now(timezone.utc).isoformat(),
        }
        update_archive_run(
            adapter=adapter,
            run_id=run_id,
            status="succeeded",
            article_count=run_counts["snapshot_count"],
            asset_count=run_counts["asset_count"],
            summary=final_summary,
            error_message=None,
        )
    except Exception as exc:
        failed = True
        run_counts = query_run_counts(adapter, run_id)
        update_archive_run(
            adapter=adapter,
            run_id=run_id,
            status="failed",
            article_count=run_counts["snapshot_count"],
            asset_count=run_counts["asset_count"],
            summary=summary,
            error_message=str(exc),
        )
        raise
    finally:
        if failed:
            print(f"Archive run marked failed: {run_id}", file=sys.stderr)

    run_counts = query_run_counts(adapter, run_id)
    return {
        "run_id": run_id,
        "article_count": run_counts["snapshot_count"],
        "asset_count": run_counts["asset_count"],
        "captured_asset_count": run_counts["captured_asset_count"],
        "indexed_article_count": indexed_article_count,
        "skipped_article_count": skipped_article_count,
    }


def get_archive_run_by_key(adapter: SqlAdapter, run_key: str) -> dict[str, Any] | None:
    """Return an archive run by key."""
    rows = adapter.query_rows(
        """
        SELECT
            [News_Archive_Run_ID],
            [Run_Key],
            [Run_Type],
            [Source_Mode],
            [Status],
            [Blob_Account],
            [Blob_Container],
            [Blob_Prefix],
            [Manifest_Blob_Path],
            [Article_Count],
            [Asset_Count]
        FROM [clubexpress].[news_archive_runs]
        WHERE [Run_Key] = ?
        """,
        (run_key,),
    )
    return rows[0] if rows else None


def insert_archive_run(
    *,
    adapter: SqlAdapter,
    run_key: str,
    run_type: str,
    source_mode: str,
    source_description: str,
    storage_account: str,
    container: str,
    blob_prefix: str,
    manifest_blob_path: str | None,
    summary: dict[str, Any],
    resume: bool,
) -> int:
    """Insert a news archive run."""
    existing = get_archive_run_by_key(adapter, run_key)
    if existing:
        if not resume:
            raise RuntimeError(f"Archive run key already exists: {run_key}")
        run_id = int(existing["News_Archive_Run_ID"])
        adapter.execute_statements(
            [
                (
                    """
                    UPDATE [clubexpress].[news_archive_runs]
                    SET
                        [Status] = N'running',
                        [Completed_At] = NULL,
                        [Blob_Account] = COALESCE([Blob_Account], ?),
                        [Blob_Container] = COALESCE([Blob_Container], ?),
                        [Blob_Prefix] = COALESCE([Blob_Prefix], ?),
                        [Manifest_Blob_Path] = COALESCE([Manifest_Blob_Path], ?),
                        [Summary_Json] = ?,
                        [Error_Message] = NULL
                    WHERE [News_Archive_Run_ID] = ?
                    """,
                    (
                        storage_account,
                        container,
                        blob_prefix,
                        manifest_blob_path,
                        json_dumps({**summary, "resumed_at": datetime.now(timezone.utc).isoformat()}),
                        run_id,
                    ),
                )
            ]
        )
        return run_id

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
            [Manifest_Blob_Path],
            [Summary_Json]
        )
        OUTPUT INSERTED.[News_Archive_Run_ID]
        VALUES (?, ?, ?, ?, N'running', ?, ?, ?, ?, ?)
        """,
        (
            run_key,
            run_type,
            source_mode,
            source_description,
            storage_account,
            container,
            blob_prefix,
            manifest_blob_path,
            json_dumps(summary),
        ),
    )
    return int(rows[0]["News_Archive_Run_ID"])


def query_run_counts(adapter: SqlAdapter, run_id: int) -> dict[str, int]:
    """Return current SQL counts for a run."""
    rows = adapter.query_rows(
        """
        SELECT
            COUNT_BIG(DISTINCT s.[News_Article_Snapshot_ID]) AS [Snapshot_Count],
            COUNT_BIG(DISTINCT asset.[News_Article_Asset_ID]) AS [Asset_Count],
            COUNT_BIG(DISTINCT CASE
                WHEN asset.[Capture_Status] = N'captured'
                THEN asset.[News_Article_Asset_ID]
                ELSE NULL
            END) AS [Captured_Asset_Count],
            COUNT_BIG(DISTINCT link.[News_Article_Link_ID]) AS [Link_Count]
        FROM [clubexpress].[news_article_snapshots] AS s
        LEFT JOIN [clubexpress].[news_article_assets] AS asset
            ON asset.[News_Article_Snapshot_ID] = s.[News_Article_Snapshot_ID]
        LEFT JOIN [clubexpress].[news_article_links] AS link
            ON link.[News_Article_Snapshot_ID] = s.[News_Article_Snapshot_ID]
        WHERE s.[News_Archive_Run_ID] = ?
        """,
        (run_id,),
    )
    row = rows[0] if rows else {}
    return {
        "snapshot_count": int(row.get("Snapshot_Count") or 0),
        "asset_count": int(row.get("Asset_Count") or 0),
        "captured_asset_count": int(row.get("Captured_Asset_Count") or 0),
        "link_count": int(row.get("Link_Count") or 0),
    }


def update_archive_run(
    *,
    adapter: SqlAdapter,
    run_id: int,
    status: str,
    article_count: int,
    asset_count: int,
    summary: dict[str, Any],
    error_message: str | None,
) -> None:
    """Update archive run completion state."""
    adapter.execute_statements(
        [
            (
                """
                UPDATE [clubexpress].[news_archive_runs]
                SET
                    [Completed_At] = SYSUTCDATETIME(),
                    [Status] = ?,
                    [Article_Count] = ?,
                    [Asset_Count] = ?,
                    [Summary_Json] = ?,
                    [Error_Message] = ?
                WHERE [News_Archive_Run_ID] = ?
                """,
                (status, article_count, asset_count, json_dumps(summary), error_message, run_id),
            )
        ]
    )


def index_article(
    *,
    adapter: SqlAdapter,
    run_id: int,
    run_key: str,
    folder: Path,
    article_summary: dict[str, Any],
    uploaded_paths: dict[str, str],
    resume: bool,
) -> dict[str, int]:
    """Index one article folder."""
    manager = read_json(folder / "manager-row.json")
    configure = read_json(folder / "configure-form.json")
    rendered = read_json(folder / "rendered-article.json")
    asset_manifest = read_json(folder / "asset-manifest.json")
    fields = configure.get("fields") or {}

    item_id = int(manager.get("itemId") or article_summary.get("itemId"))
    headline = fields.get("headline") or manager.get("headline") or article_summary.get("headline")
    author = fields.get("author") or manager.get("author") or article_summary.get("author")
    category = fields.get("category") or manager.get("category") or article_summary.get("category")
    status = manager.get("status") or article_summary.get("status")
    visibility = fields.get("visibility") or article_summary.get("visibility")
    active_date = parse_date(fields.get("activeDate") or manager.get("activeDate") or article_summary.get("activeDate"))
    expires_date = parse_date(fields.get("expires") or manager.get("expires"))
    article_id = upsert_article(
        adapter=adapter,
        item_id=item_id,
        headline=headline,
        author=author,
        subgroup=manager.get("subgroup"),
        category=category,
        current_status=status,
        visibility=visibility,
        active_date=active_date,
        expires_date=expires_date,
        view_url=manager.get("viewUrl") or rendered.get("url"),
        configure_url=manager.get("configureUrl") or configure.get("url"),
        edit_text_url=manager.get("editTextUrl"),
        run_id=run_id,
    )
    existing_snapshot = get_existing_snapshot(adapter, run_id, article_id)
    if existing_snapshot and resume:
        existing_counts = query_snapshot_counts(adapter, int(existing_snapshot["News_Article_Snapshot_ID"]))
        return {
            "asset_count": existing_counts["asset_count"],
            "captured_asset_count": existing_counts["captured_asset_count"],
            "skipped": 1,
        }

    folder_relative = folder.name
    file_blob = lambda name: uploaded_paths.get(f"{folder_relative}/{name}")
    folder_blob_prefix = clean_blob_path(f"{uploaded_prefix(uploaded_paths)}/{folder_relative}") if uploaded_paths else None
    editor_hash = file_sha256(folder / "editor-source.html")
    rendered_hash = file_sha256(folder / "rendered-article.html")
    container_hash = file_sha256(folder / "rendered-container.html")
    content_hash = sha256_text("|".join([value or "" for value in [editor_hash, rendered_hash, container_hash]]))
    rendered_images = rendered.get("articleImages") or []
    rendered_links = rendered.get("articleLinks") or []
    snapshot_id = insert_snapshot(
        adapter=adapter,
        article_id=article_id,
        run_id=run_id,
        run_key=run_key,
        item_id=item_id,
        headline=headline,
        author=author,
        category=category,
        status=status,
        visibility=visibility,
        active_date=active_date,
        expires_date=expires_date,
        summary=fields.get("summary"),
        search_text=build_search_text(
            headline=headline,
            author=author,
            summary=fields.get("summary"),
            tags_input=fields.get("tagsInput"),
            article_text=rendered.get("articleText"),
        ),
        tags_input=fields.get("tagsInput"),
        show_on_main_news_page=fields.get("showOnMainNewsPage"),
        show_active_date_on_story=fields.get("showActiveDateOnStory"),
        show_author_on_story=fields.get("showAuthorOnStory"),
        rendered_text_length=len(rendered.get("articleText") or ""),
        rendered_article_image_count=len(rendered_images),
        rendered_article_link_count=len(rendered_links),
        managed_image_count=len(asset_manifest.get("managedImageUrls") or []),
        external_image_count=len(asset_manifest.get("externalImageUrlsNotFetched") or []),
        asset_captured_count=len(asset_manifest.get("captured") or []),
        asset_failure_count=len(asset_manifest.get("failures") or []),
        manager_row_json=json_dumps(manager),
        configure_form_json=json_dumps(configure),
        rendered_metadata_json=json_dumps(rendered),
        asset_manifest_json=json_dumps(asset_manifest),
        blob_prefix=folder_blob_prefix,
        manager_row_blob_path=file_blob("manager-row.json"),
        configure_form_blob_path=file_blob("configure-form.json"),
        editor_source_blob_path=file_blob("editor-source.html"),
        editor_source_raw_blob_path=file_blob("editor-source.raw-urlencoded.txt"),
        rendered_article_blob_path=file_blob("rendered-article.html"),
        rendered_container_blob_path=file_blob("rendered-container.html"),
        asset_manifest_blob_path=file_blob("asset-manifest.json"),
        editor_source_sha256=editor_hash,
        rendered_article_sha256=rendered_hash,
        rendered_container_sha256=container_hash,
        content_sha256=content_hash,
    )

    image_lookup = build_image_lookup(rendered_images)
    asset_rows = build_asset_rows(
        snapshot_id=snapshot_id,
        article_id=article_id,
        folder_name=folder.name,
        asset_manifest=asset_manifest,
        image_lookup=image_lookup,
        uploaded_paths=uploaded_paths,
    )
    link_rows = build_link_rows(
        snapshot_id=snapshot_id,
        article_id=article_id,
        rendered_links=rendered_links,
    )
    if asset_rows:
        adapter.execute_statements(asset_rows)
        assign_snapshot_thumbnail_from_captured_images(adapter, snapshot_id)
    if link_rows:
        adapter.execute_statements(link_rows)

    return {
        "asset_count": len(asset_rows),
        "captured_asset_count": len(asset_manifest.get("captured") or []),
        "skipped": 0,
    }


def assign_snapshot_thumbnail_from_captured_images(adapter: SqlAdapter, snapshot_id: int) -> None:
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


def get_existing_snapshot(adapter: SqlAdapter, run_id: int, article_id: int) -> dict[str, Any] | None:
    """Return an existing snapshot for a run/article pair."""
    rows = adapter.query_rows(
        """
        SELECT [News_Article_Snapshot_ID]
        FROM [clubexpress].[news_article_snapshots]
        WHERE [News_Archive_Run_ID] = ?
          AND [News_Article_ID] = ?
        """,
        (run_id, article_id),
    )
    return rows[0] if rows else None


def query_snapshot_counts(adapter: SqlAdapter, snapshot_id: int) -> dict[str, int]:
    """Return current SQL counts for one snapshot."""
    rows = adapter.query_rows(
        """
        SELECT
            COUNT_BIG(DISTINCT asset.[News_Article_Asset_ID]) AS [Asset_Count],
            COUNT_BIG(DISTINCT CASE
                WHEN asset.[Capture_Status] = N'captured'
                THEN asset.[News_Article_Asset_ID]
                ELSE NULL
            END) AS [Captured_Asset_Count],
            COUNT_BIG(DISTINCT link.[News_Article_Link_ID]) AS [Link_Count]
        FROM [clubexpress].[news_article_snapshots] AS s
        LEFT JOIN [clubexpress].[news_article_assets] AS asset
            ON asset.[News_Article_Snapshot_ID] = s.[News_Article_Snapshot_ID]
        LEFT JOIN [clubexpress].[news_article_links] AS link
            ON link.[News_Article_Snapshot_ID] = s.[News_Article_Snapshot_ID]
        WHERE s.[News_Article_Snapshot_ID] = ?
        """,
        (snapshot_id,),
    )
    row = rows[0] if rows else {}
    return {
        "asset_count": int(row.get("Asset_Count") or 0),
        "captured_asset_count": int(row.get("Captured_Asset_Count") or 0),
        "link_count": int(row.get("Link_Count") or 0),
    }


def upsert_article(
    *,
    adapter: SqlAdapter,
    item_id: int,
    headline: str | None,
    author: str | None,
    subgroup: str | None,
    category: str | None,
    current_status: str | None,
    visibility: str | None,
    active_date: date | None,
    expires_date: date | None,
    view_url: str | None,
    configure_url: str | None,
    edit_text_url: str | None,
    run_id: int,
) -> int:
    """Insert or update the stable article row."""
    rows = adapter.query_rows(
        """
        SELECT [News_Article_ID]
        FROM [clubexpress].[news_articles]
        WHERE [Club_ID] = 454497
          AND [Item_ID] = ?
        """,
        (item_id,),
    )
    if rows:
        article_id = int(rows[0]["News_Article_ID"])
        adapter.execute_statements(
            [
                (
                    """
                    UPDATE [clubexpress].[news_articles]
                    SET
                        [Headline] = ?,
                        [Author] = ?,
                        [Subgroup] = ?,
                        [Category] = ?,
                        [Current_Status] = ?,
                        [Visibility] = ?,
                        [Active_Date] = ?,
                        [Expires_Date] = ?,
                        [View_Url] = ?,
                        [Configure_Url] = ?,
                        [Edit_Text_Url] = ?,
                        [Last_Seen_Run_ID] = ?,
                        [Last_Seen_At] = SYSUTCDATETIME(),
                        [Updated_At] = SYSUTCDATETIME()
                    WHERE [News_Article_ID] = ?
                    """,
                    (
                        headline,
                        author,
                        subgroup,
                        category,
                        current_status,
                        visibility,
                        active_date,
                        expires_date,
                        view_url,
                        configure_url,
                        edit_text_url,
                        run_id,
                        article_id,
                    ),
                )
            ]
        )
        return article_id

    inserted = adapter.query_rows(
        """
        INSERT INTO [clubexpress].[news_articles]
        (
            [Item_ID],
            [Headline],
            [Author],
            [Subgroup],
            [Category],
            [Current_Status],
            [Visibility],
            [Active_Date],
            [Expires_Date],
            [View_Url],
            [Configure_Url],
            [Edit_Text_Url],
            [First_Seen_Run_ID],
            [Last_Seen_Run_ID]
        )
        OUTPUT INSERTED.[News_Article_ID]
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            item_id,
            headline,
            author,
            subgroup,
            category,
            current_status,
            visibility,
            active_date,
            expires_date,
            view_url,
            configure_url,
            edit_text_url,
            run_id,
            run_id,
        ),
    )
    return int(inserted[0]["News_Article_ID"])


def insert_snapshot(
    *,
    adapter: SqlAdapter,
    article_id: int,
    run_id: int,
    run_key: str,
    item_id: int,
    headline: str | None,
    author: str | None,
    category: str | None,
    status: str | None,
    visibility: str | None,
    active_date: date | None,
    expires_date: date | None,
    summary: str | None,
    search_text: str | None,
    tags_input: str | None,
    show_on_main_news_page: bool | None,
    show_active_date_on_story: bool | None,
    show_author_on_story: bool | None,
    rendered_text_length: int,
    rendered_article_image_count: int,
    rendered_article_link_count: int,
    managed_image_count: int,
    external_image_count: int,
    asset_captured_count: int,
    asset_failure_count: int,
    manager_row_json: str,
    configure_form_json: str,
    rendered_metadata_json: str,
    asset_manifest_json: str,
    blob_prefix: str | None,
    manager_row_blob_path: str | None,
    configure_form_blob_path: str | None,
    editor_source_blob_path: str | None,
    editor_source_raw_blob_path: str | None,
    rendered_article_blob_path: str | None,
    rendered_container_blob_path: str | None,
    asset_manifest_blob_path: str | None,
    editor_source_sha256: str | None,
    rendered_article_sha256: str | None,
    rendered_container_sha256: str | None,
    content_sha256: str | None,
) -> int:
    """Insert a single article snapshot."""
    snapshot_key = f"{run_key}:{item_id}"
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
            [Tags_Input],
            [Show_On_Main_News_Page],
            [Show_Active_Date_On_Story],
            [Show_Author_On_Story],
            [Rendered_Text_Length],
            [Rendered_Article_Image_Count],
            [Rendered_Article_Link_Count],
            [Managed_Image_Count],
            [External_Image_Count],
            [Asset_Captured_Count],
            [Asset_Failure_Count],
            [Manager_Row_Json],
            [Configure_Form_Json],
            [Rendered_Metadata_Json],
            [Asset_Manifest_Json],
            [Blob_Prefix],
            [Manager_Row_Blob_Path],
            [Configure_Form_Blob_Path],
            [Editor_Source_Blob_Path],
            [Editor_Source_Raw_Blob_Path],
            [Rendered_Article_Blob_Path],
            [Rendered_Container_Blob_Path],
            [Asset_Manifest_Blob_Path],
            [Editor_Source_Sha256],
            [Rendered_Article_Sha256],
            [Rendered_Container_Sha256],
            [Content_Sha256]
        )
        OUTPUT INSERTED.[News_Article_Snapshot_ID]
        VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            article_id,
            run_id,
            snapshot_key,
            item_id,
            headline,
            author,
            category,
            status,
            visibility,
            active_date,
            expires_date,
            summary,
            search_text,
            tags_input,
            show_on_main_news_page,
            show_active_date_on_story,
            show_author_on_story,
            rendered_text_length,
            rendered_article_image_count,
            rendered_article_link_count,
            managed_image_count,
            external_image_count,
            asset_captured_count,
            asset_failure_count,
            manager_row_json,
            configure_form_json,
            rendered_metadata_json,
            asset_manifest_json,
            blob_prefix,
            manager_row_blob_path,
            configure_form_blob_path,
            editor_source_blob_path,
            editor_source_raw_blob_path,
            rendered_article_blob_path,
            rendered_container_blob_path,
            asset_manifest_blob_path,
            editor_source_sha256,
            rendered_article_sha256,
            rendered_container_sha256,
            content_sha256,
        ),
    )
    return int(rows[0]["News_Article_Snapshot_ID"])


def build_asset_rows(
    *,
    snapshot_id: int,
    article_id: int,
    folder_name: str,
    asset_manifest: dict[str, Any],
    image_lookup: dict[str, dict[str, Any]],
    uploaded_paths: dict[str, str],
) -> list[tuple[str, tuple[Any, ...]]]:
    """Build SQL insert statements for captured, skipped, and failed assets."""
    rows: list[tuple[str, tuple[Any, ...]]] = []
    insert_sql = """
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    for captured in asset_manifest.get("captured") or []:
        source_url = normalize_url(captured.get("sourceUrl"))
        rendered_image = image_lookup.get(source_url) or {}
        relative_asset_path = f"{folder_name}/{captured.get('file')}"
        rows.append(
            (
                insert_sql,
                (
                    snapshot_id,
                    article_id,
                    source_url,
                    sha256_text(source_url),
                    classify_asset_url(source_url),
                    "captured",
                    uploaded_paths.get(relative_asset_path),
                    captured.get("originalName"),
                    captured.get("contentType"),
                    captured.get("byteLength"),
                    captured.get("sha256"),
                    rendered_image.get("width"),
                    rendered_image.get("height"),
                    rendered_image.get("alt"),
                    rendered_image.get("title"),
                    None,
                ),
            )
        )
    for source_url in asset_manifest.get("externalImageUrlsNotFetched") or []:
        source_url = normalize_url(source_url)
        rendered_image = image_lookup.get(source_url) or {}
        rows.append(
            (
                insert_sql,
                (
                    snapshot_id,
                    article_id,
                    source_url,
                    sha256_text(source_url),
                    classify_asset_url(source_url),
                    "not_fetched",
                    None,
                    Path(urlparse(source_url).path).name or None,
                    None,
                    None,
                    None,
                    rendered_image.get("width"),
                    rendered_image.get("height"),
                    rendered_image.get("alt"),
                    rendered_image.get("title"),
                    None,
                ),
            )
        )
    for failure in asset_manifest.get("failures") or []:
        source_url = normalize_url(failure.get("sourceUrl") or failure.get("url") or "")
        rows.append(
            (
                insert_sql,
                (
                    snapshot_id,
                    article_id,
                    source_url,
                    sha256_text(source_url),
                    classify_asset_url(source_url),
                    "failed",
                    None,
                    failure.get("originalName"),
                    failure.get("contentType"),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    failure.get("error") or failure.get("message") or json_dumps(failure),
                ),
            )
        )
    return rows


def build_link_rows(
    *,
    snapshot_id: int,
    article_id: int,
    rendered_links: list[dict[str, Any]],
) -> list[tuple[str, tuple[Any, ...]]]:
    """Build SQL insert statements for article links."""
    rows: list[tuple[str, tuple[Any, ...]]] = []
    for link in rendered_links:
        href = link.get("href")
        if not href:
            continue
        rows.append(
            (
                """
                INSERT INTO [clubexpress].[news_article_links]
                (
                    [News_Article_Snapshot_ID],
                    [News_Article_ID],
                    [Href],
                    [Link_Text],
                    [Link_Source]
                )
                VALUES (?, ?, ?, ?, N'rendered_article')
                """,
                (snapshot_id, article_id, normalize_url(href), link.get("text")),
            )
        )
    return rows


def validate_archive_run(
    *,
    adapter: SqlAdapter,
    sample_dir: Path,
    run_id: int,
    run_key: str,
    blob_prefix: str,
    container_client: Any | None,
    check_external_links: bool,
    link_timeout_seconds: float,
) -> dict[str, Any]:
    """Build a validation report for a loaded archive run."""
    expected = collect_sample_expectations(sample_dir)
    sql_counts = query_run_counts(adapter, run_id)
    blob_count = count_blobs(container_client, blob_prefix) if container_client is not None else None
    asset_status_counts = query_asset_status_counts(adapter, run_id)
    link_report = build_external_link_report(sample_dir, link_timeout_seconds) if check_external_links else None

    checks = [
        make_check("articles", expected["article_count"], sql_counts["snapshot_count"]),
        make_check("assets", expected["asset_count"], sql_counts["asset_count"]),
        make_check("captured_assets", expected["captured_asset_count"], sql_counts["captured_asset_count"]),
        make_check("links", expected["link_count"], sql_counts["link_count"]),
    ]
    if blob_count is not None:
        checks.append(make_check("blobs", expected["file_count"], blob_count))

    link_status_counts: dict[str, int] = {}
    if link_report is not None:
        for item in link_report["links"]:
            link_status_counts[item["status"]] = link_status_counts.get(item["status"], 0) + 1
        bad_links = [item for item in link_report["links"] if not item["valid"]]
        checks.append(
            {
                "name": "external_links",
                "expected": 0,
                "actual": len(bad_links),
                "ok": not bad_links,
            }
        )

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "run_key": run_key,
        "blob_prefix": blob_prefix,
        "sample_dir": str(sample_dir),
        "expected": expected,
        "actual": {
            "sql": sql_counts,
            "blob_count": blob_count,
            "asset_status_counts": asset_status_counts,
            "external_link_status_counts": link_status_counts or None,
        },
        "checks": checks,
        "external_links": link_report,
    }
    report["valid"] = all(check["ok"] for check in checks)
    return json_safe_value(report)


def collect_sample_expectations(sample_dir: Path) -> dict[str, int]:
    """Collect expected counts from a local captured sample."""
    manifest = read_json(sample_dir / "sample-manifest.json")
    article_count = 0
    asset_count = 0
    captured_asset_count = 0
    link_count = 0
    external_image_count = 0
    asset_failure_count = 0
    for article in manifest.get("articles", []):
        folder_name = str(article.get("folder") or article.get("itemId") or "").strip()
        if not folder_name:
            continue
        folder = sample_dir / folder_name
        if not folder.exists():
            continue
        article_count += 1
        rendered = read_json(folder / "rendered-article.json")
        asset_manifest = read_json(folder / "asset-manifest.json")
        captured = len(asset_manifest.get("captured") or [])
        external_images = len(asset_manifest.get("externalImageUrlsNotFetched") or [])
        failures = len(asset_manifest.get("failures") or [])
        captured_asset_count += captured
        external_image_count += external_images
        asset_failure_count += failures
        asset_count += captured + external_images + failures
        link_count += len(rendered.get("articleLinks") or [])
    return {
        "article_count": article_count,
        "asset_count": asset_count,
        "captured_asset_count": captured_asset_count,
        "external_image_count": external_image_count,
        "asset_failure_count": asset_failure_count,
        "link_count": link_count,
        "file_count": sum(1 for path in sample_dir.rglob("*") if path.is_file()),
    }


def make_check(name: str, expected: int, actual: int | None) -> dict[str, Any]:
    """Build one validation check."""
    return {
        "name": name,
        "expected": expected,
        "actual": actual,
        "ok": actual == expected,
    }


def query_asset_status_counts(adapter: SqlAdapter, run_id: int) -> dict[str, int]:
    """Return asset counts by capture status."""
    rows = adapter.query_rows(
        """
        SELECT
            asset.[Capture_Status],
            COUNT_BIG(*) AS [Count]
        FROM [clubexpress].[news_article_assets] AS asset
        INNER JOIN [clubexpress].[news_article_snapshots] AS s
            ON s.[News_Article_Snapshot_ID] = asset.[News_Article_Snapshot_ID]
        WHERE s.[News_Archive_Run_ID] = ?
        GROUP BY asset.[Capture_Status]
        """,
        (run_id,),
    )
    return {str(row["Capture_Status"]): int(row["Count"] or 0) for row in rows}


def count_blobs(container_client: Any, blob_prefix: str) -> int:
    """Count blobs under a prefix."""
    return sum(1 for _ in container_client.list_blobs(name_starts_with=blob_prefix))


def build_external_link_report(sample_dir: Path, timeout_seconds: float) -> dict[str, Any]:
    """Check all unique rendered article links."""
    links = collect_external_links(sample_dir)
    checked = []
    for link in links:
        checked.append(check_external_link(link, timeout_seconds))
    return {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "unique_link_count": len(links),
        "links": checked,
    }


def collect_external_links(sample_dir: Path) -> list[dict[str, Any]]:
    """Collect unique external links from rendered article metadata."""
    manifest = read_json(sample_dir / "sample-manifest.json")
    links_by_url: dict[str, dict[str, Any]] = {}
    for article in manifest.get("articles", []):
        folder_name = str(article.get("folder") or article.get("itemId") or "").strip()
        if not folder_name:
            continue
        rendered = read_json(sample_dir / folder_name / "rendered-article.json")
        for link in rendered.get("articleLinks") or []:
            href = normalize_url(link.get("href"))
            if not href or not is_external_http_url(href):
                continue
            entry = links_by_url.setdefault(
                href,
                {
                    "url": href,
                    "text_samples": [],
                    "articles": [],
                },
            )
            text = str(link.get("text") or "").strip()
            if text and text not in entry["text_samples"] and len(entry["text_samples"]) < 3:
                entry["text_samples"].append(text)
            entry["articles"].append(
                {
                    "item_id": article.get("itemId"),
                    "headline": article.get("headline"),
                    "folder": folder_name,
                }
            )
    return sorted(links_by_url.values(), key=lambda item: item["url"])


def is_external_http_url(url: str) -> bool:
    """Return whether a URL should be checked as an external HTTP(S) link."""
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    return bool(parsed.netloc)


def check_external_link(link: dict[str, Any], timeout_seconds: float) -> dict[str, Any]:
    """Check one external link with HEAD and GET fallback."""
    url = link["url"]
    result = {
        **link,
        "status": "unchecked",
        "valid": False,
        "http_status": None,
        "method": None,
        "final_url": None,
        "error": None,
    }
    for method in ("HEAD", "GET"):
        try:
            req = request.Request(url, method=method)
            req.add_header("User-Agent", "AGA-News-Archive-Validator/1.0")
            req.add_header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
            if method == "GET":
                req.add_header("Range", "bytes=0-0")
            with request.urlopen(req, timeout=timeout_seconds) as response:
                status_code = int(response.status)
                result.update(
                    {
                        "status": classify_link_status(status_code),
                        "valid": is_link_status_valid(status_code),
                        "http_status": status_code,
                        "method": method,
                        "final_url": response.geturl(),
                        "error": None,
                    }
                )
                return result
        except error.HTTPError as exc:
            status_code = int(exc.code)
            if method == "HEAD" and status_code in {403, 405, 429, 500, 501}:
                continue
            result.update(
                {
                    "status": classify_link_status(status_code),
                    "valid": is_link_status_valid(status_code),
                    "http_status": status_code,
                    "method": method,
                    "final_url": exc.geturl(),
                    "error": str(exc),
                }
            )
            return result
        except Exception as exc:
            if method == "HEAD":
                continue
            result.update(
                {
                    "status": "error",
                    "valid": False,
                    "method": method,
                    "error": str(exc),
                }
            )
            return result
    return result


def classify_link_status(status_code: int) -> str:
    """Classify an HTTP link check status."""
    if 200 <= status_code <= 399:
        return "ok"
    if status_code in {401, 403, 429}:
        return "protected_or_rate_limited"
    if status_code in {404, 410}:
        return "missing"
    return "http_error"


def is_link_status_valid(status_code: int) -> bool:
    """Return whether a link status counts as valid enough for archive validation."""
    return 200 <= status_code <= 399


def write_validation_report(
    report: dict[str, Any],
    report_file: str | None,
    report_dir: str | Path,
    run_key: str,
) -> Path:
    """Write validation report JSON and return its path."""
    if report_file:
        path = resolve_path(report_file)
    else:
        safe_run_key = re.sub(r"[^A-Za-z0-9_.-]+", "-", run_key).strip("-")
        path = resolve_path(report_dir) / f"{safe_run_key}-validation.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(json_safe_value(report), indent=2, sort_keys=True), encoding="utf-8")
    return path


def print_validation_summary(report: dict[str, Any], report_path: Path) -> None:
    """Print a compact validation summary."""
    status = "passed" if report["valid"] else "failed"
    print(f"Validation {status}: {report_path}")
    for check in report.get("checks") or []:
        marker = "OK" if check["ok"] else "MISMATCH"
        print(f"  {marker} {check['name']}: expected {check['expected']}, actual {check['actual']}")


def build_image_lookup(images: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Map normalized image URLs to rendered image metadata."""
    lookup: dict[str, dict[str, Any]] = {}
    for image in images:
        for key in ("src", "rawSrc"):
            value = image.get(key)
            if value:
                lookup[normalize_url(value)] = image
    return lookup


def classify_asset_url(source_url: str) -> str:
    """Classify an article image source."""
    parsed = urlparse(source_url)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    if host == "images.clubexpress.com":
        return "managed"
    if host == "s3.amazonaws.com" and "/clubexpressclubfiles/" in path:
        return "managed"
    if host == "wp-cdn.usgo.org":
        return "legacy_external"
    if host:
        return "external"
    return "unknown"


def uploaded_prefix(uploaded_paths: dict[str, str]) -> str:
    """Infer the common uploaded blob prefix from the manifest path."""
    manifest_path = uploaded_paths.get("sample-manifest.json")
    if manifest_path:
        return manifest_path.rsplit("/", 1)[0]
    return ""


def normalize_url(value: Any) -> str:
    """Normalize common URL forms captured from ClubExpress pages."""
    text = str(value or "").strip()
    if text.startswith("//"):
        return "https:" + text
    return text


def parse_date(value: Any) -> date | None:
    """Parse ClubExpress date text."""
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return None


def build_search_text(
    *,
    headline: Any,
    author: Any,
    summary: Any,
    tags_input: Any,
    article_text: Any,
) -> str | None:
    """Build the full-text search document for an archived article."""
    parts = [
        normalize_whitespace(headline),
        normalize_whitespace(author),
        normalize_whitespace(summary),
        normalize_whitespace(tags_input),
        normalize_whitespace(article_text),
    ]
    text = "\n".join(part for part in parts if part)
    return text or None


def normalize_whitespace(value: Any) -> str:
    """Normalize whitespace for search text."""
    return re.sub(r"\s+", " ", str(value or "")).strip()


def read_json(path: Path) -> dict[str, Any]:
    """Read a JSON object from disk."""
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return data


def file_sha256(path: Path) -> str | None:
    """Return a file's SHA-256 hex digest."""
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_text(value: str) -> str:
    """Return a text value's SHA-256 hex digest."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def json_dumps(value: Any) -> str:
    """Serialize JSON consistently for SQL storage."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def json_safe_value(value: Any) -> Any:
    """Convert values to JSON-safe equivalents."""
    if isinstance(value, dict):
        return {str(key): json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe_value(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe_value(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


if __name__ == "__main__":
    raise SystemExit(main())
