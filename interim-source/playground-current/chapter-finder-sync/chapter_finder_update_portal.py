# Copyright 2026, American Go Association, All rights reserved

"""Manage the Chapter Finder update portal schema and private access links."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any


AGA_ROOT = Path(__file__).resolve().parents[1]
PORTAL_APP_ROOT = AGA_ROOT / "chapter-finder-update-app"
for import_root in (AGA_ROOT, PORTAL_APP_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from bayrate.sql_adapter import SqlAdapter, get_sql_connection_string  # noqa: E402
from chapter_finder_update_support import create_access_link, json_safe, revoke_access_link  # noqa: E402


DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parent / "sql" / "chapter_finder_update_portal_schema.sql"


def main(argv: list[str] | None = None) -> int:
    """Run a portal management command."""
    args = build_parser().parse_args(argv)
    connection_string = args.connection_string or get_sql_connection_string()
    if not connection_string:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2
    adapter = SqlAdapter(connection_string)

    if args.command == "apply-schema":
        batch_count = apply_schema(adapter, Path(args.schema_file).resolve())
        print(json.dumps({"applied_schema_batches": batch_count}, indent=2))
        return 0
    if args.command == "create-link":
        base_url = args.base_url or os.environ.get("CHAPTER_UPDATE_PUBLIC_BASE_URL")
        if not base_url:
            print("--base-url or CHAPTER_UPDATE_PUBLIC_BASE_URL is required.", file=sys.stderr)
            return 2
        result = create_access_link(
            adapter,
            base_url=base_url,
            chapter_finder_id=args.chapter_finder_id,
            clubexpress_chapter_id=args.clubexpress_chapter_id,
            expires_days=None if args.no_expiration else args.expires_days,
            created_by=args.created_by,
            label=args.label,
        )
        print(json.dumps(result, default=json_safe, indent=2))
        return 0
    if args.command == "revoke-link":
        revoke_access_link(adapter, args.access_link_id)
        print(json.dumps({"revoked_access_link_id": args.access_link_id}, indent=2))
        return 0
    if args.command == "list-links":
        rows = adapter.query_rows(
            """
SELECT
    link.[Chapter_Finder_Update_Access_Link_ID], link.[Chapter_Finder_ID],
    entry.[ClubExpress_Chapter_ID], entry.[Chapter_Name], link.[Link_Label],
    link.[Created_By], link.[Expires_At], link.[Is_Active], link.[Last_Used_At],
    link.[Use_Count], link.[Last_Submitted_At], link.[Created_At]
FROM [clubexpress].[chapter_finder_update_access_links] AS link
INNER JOIN [clubexpress].[chapter_finder_entries] AS entry
    ON entry.[Chapter_Finder_Entry_ID] = link.[Chapter_Finder_Entry_ID]
ORDER BY link.[Created_At] DESC, link.[Chapter_Finder_Update_Access_Link_ID] DESC;
"""
        )
        print(json.dumps(rows, default=json_safe, indent=2))
        return 0
    if args.command == "grant-reviewer":
        rows = adapter.query_rows(
            """
EXEC [ratings].[sp_grant_admin_permission]
    @Principal_Name = ?,
    @Permission_Code = N'chapter_finder_updates',
    @Principal_Id = ?,
    @Display_Name = ?,
    @Granted_By = ?;
""",
            (args.principal_name, args.principal_id, args.display_name, args.granted_by),
        )
        print(json.dumps(rows, default=json_safe, indent=2))
        return 0
    if args.command == "list-reviewers":
        rows = adapter.query_rows(
            """
SELECT [AdminPermissionID], [Principal_Name], [Principal_Id], [Display_Name],
       [Permission_Code], [Is_Active], [Created_At], [Revoked_At]
FROM [ratings].[admin_permissions]
WHERE [Permission_Code] IN (N'chapter_finder_updates', N'admin_all')
ORDER BY [Permission_Code], [Principal_Name];
"""
        )
        print(json.dumps(rows, default=json_safe, indent=2))
        return 0
    raise RuntimeError(f"Unhandled command: {args.command}")


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local settings.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    apply_parser = subparsers.add_parser("apply-schema", help="Apply the update portal schema.")
    apply_parser.add_argument("--schema-file", default=str(DEFAULT_SCHEMA_PATH))

    create_parser = subparsers.add_parser("create-link", help="Create a private chapter update link.")
    selector = create_parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--chapter-finder-id", type=int)
    selector.add_argument("--clubexpress-chapter-id", type=int)
    create_parser.add_argument("--base-url")
    create_parser.add_argument("--expires-days", type=int, default=365)
    create_parser.add_argument("--no-expiration", action="store_true")
    create_parser.add_argument("--created-by")
    create_parser.add_argument("--label")

    revoke_parser = subparsers.add_parser("revoke-link", help="Disable a private chapter update link.")
    revoke_parser.add_argument("access_link_id", type=int)
    subparsers.add_parser("list-links", help="List links without exposing raw tokens.")

    grant_parser = subparsers.add_parser("grant-reviewer", help="Grant Chapter Finder review permission.")
    grant_parser.add_argument("--principal-name", required=True)
    grant_parser.add_argument("--principal-id")
    grant_parser.add_argument("--display-name")
    grant_parser.add_argument("--granted-by")
    subparsers.add_parser("list-reviewers", help="List scoped reviewers and admin_all principals.")
    return parser


def apply_schema(adapter: SqlAdapter, schema_path: Path) -> int:
    """Apply a SQL Server schema file using GO batch separators."""
    text = schema_path.read_text(encoding="utf-8-sig")
    batches = [batch.strip() for batch in re.split(r"(?im)^\s*GO\s*;?\s*$", text) if batch.strip()]
    for batch in batches:
        adapter.execute_statements([(batch, ())])
    return len(batches)


if __name__ == "__main__":
    raise SystemExit(main())
