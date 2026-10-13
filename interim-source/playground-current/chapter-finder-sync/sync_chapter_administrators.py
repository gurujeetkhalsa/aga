# Copyright 2026, American Go Association, All rights reserved

"""Import private ClubExpress chapter-administrator assignments into Azure SQL."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bayrate.sql_adapter import SqlAdapter, get_sql_connection_string  # noqa: E402


CLUB_ID = 454497
SOURCE_URL_TEMPLATE = f"https://usgo.org/popup.aspx?page_id=383&club_id={CLUB_ID}&item_id={{chapter_finder_id}}"
DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parent / "sql" / "chapter_finder_administrator_schema.sql"


@dataclass(frozen=True)
class DesiredAssignment:
    """Represent one current private chapter-administrator assignment."""

    chapter_finder_entry_id: int
    chapter_finder_id: int
    chapter_name: str
    administrator_display_name: str
    normalized_display_name: str
    duplicate_ordinal: int
    assignment_key_sha256: str
    membership_agaid: int | None
    member_match_status: str
    member_match_candidate_count: int


def main(argv: list[str] | None = None) -> int:
    """Run a schema application or assignment import."""
    args = build_parser().parse_args(argv)
    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2
    adapter = SqlAdapter(conn_str)

    if args.command == "apply-schema":
        batches = apply_schema(adapter, Path(args.schema_file).resolve())
        print(json.dumps({"applied_schema_batches": batches}, indent=2))
        return 0

    if args.command == "import":
        payload = load_payload(Path(args.input).resolve())
        result = import_assignments(
            adapter=adapter,
            payload=payload,
            run_key=args.run_key or f"chapter-administrator-sync-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
            dry_run=args.dry_run,
        )
        print(json.dumps(result, default=json_safe, indent=2))
        return 0 if result["status"] in {"dry_run", "succeeded", "partial"} else 1

    if args.command == "status":
        print(json.dumps(load_status(adapter), default=json_safe, indent=2))
        return 0

    raise RuntimeError(f"Unhandled command: {args.command}")


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    schema_parser = subparsers.add_parser("apply-schema", help="Apply the private administrator-assignment schema.")
    schema_parser.add_argument("--schema-file", default=str(DEFAULT_SCHEMA_PATH))

    import_parser = subparsers.add_parser("import", help="Import an authorized Chrome extraction JSON file.")
    import_parser.add_argument("--input", required=True, help="Path to the private assignment extraction JSON.")
    import_parser.add_argument("--run-key", help="Stable unique run key.")
    import_parser.add_argument("--dry-run", action="store_true", help="Validate and compare without writing SQL.")
    subparsers.add_parser("status", help="Show private assignment and history counts.")
    return parser


def load_payload(path: Path) -> dict[str, Any]:
    """Load and validate a private assignment extraction."""
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict) or not isinstance(payload.get("chapters"), list):
        raise ValueError("Input JSON must contain a chapters array.")
    seen_ids: set[int] = set()
    for chapter in payload["chapters"]:
        if not isinstance(chapter, dict):
            raise ValueError("Each chapters item must be an object.")
        chapter_id = int(chapter.get("chapterFinderId") or 0)
        if chapter_id <= 0 or chapter_id in seen_ids:
            raise ValueError(f"Invalid or duplicate chapterFinderId: {chapter_id}")
        seen_ids.add(chapter_id)
        if not isinstance(chapter.get("administratorNames"), list):
            raise ValueError(f"Chapter {chapter_id} must contain an administratorNames array.")
    return payload


def import_assignments(adapter: SqlAdapter, payload: dict[str, Any], run_key: str, dry_run: bool) -> dict[str, Any]:
    """Synchronize extracted current assignments into private SQL tables."""
    chapters = load_current_chapters(adapter)
    member_index = build_member_index(adapter)
    desired = build_desired_assignments(payload, chapters, member_index)
    existing = load_existing_assignments(adapter)
    comparison = compare_assignments(desired, existing)
    match_counts = count_match_statuses(desired)
    chapter_count = len(payload["chapters"])
    chapters_with_admins = sum(1 for chapter in payload["chapters"] if chapter["administratorNames"])

    summary = {
        "status": "dry_run" if dry_run else "running",
        "run_key": run_key,
        "chapter_count": chapter_count,
        "chapters_with_administrators_count": chapters_with_admins,
        "assignment_count": len(desired),
        **comparison,
        **match_counts,
    }
    if dry_run:
        return summary

    extracted_at = parse_extracted_at(payload.get("extractedAt"))
    run_id = start_run(
        adapter,
        run_key=run_key,
        extracted_at=extracted_at,
        chapter_count=chapter_count,
        chapters_with_admins=chapters_with_admins,
        assignment_count=len(desired),
    )
    try:
        apply_desired_assignments(adapter, run_id, desired, existing)
        status = "partial" if match_counts["ambiguous_count"] or match_counts["unmatched_count"] else "succeeded"
        complete_run(adapter, run_id, status, comparison, match_counts)
        summary["status"] = status
        summary["run_id"] = run_id
        return summary
    except Exception as exc:
        fail_run(adapter, run_id, str(exc))
        raise


def load_current_chapters(adapter: SqlAdapter) -> dict[int, dict[str, Any]]:
    """Load current Chapter Finder entries keyed by private organization id."""
    rows = adapter.query_rows(
        """
SELECT [Chapter_Finder_Entry_ID], [Chapter_Finder_ID], [Chapter_Name]
FROM [clubexpress].[chapter_finder_entries]
WHERE [Club_ID] = ?
  AND [Is_Current] = 1;
""",
        (CLUB_ID,),
    )
    return {int(row["Chapter_Finder_ID"]): row for row in rows}


def build_member_index(adapter: SqlAdapter) -> dict[str, set[int]]:
    """Build conservative exact-name variants for membership resolution."""
    rows = adapter.query_rows(
        """
SELECT [AGAID], [FirstName], [MiddleInitial], [LastName]
FROM [membership].[members]
WHERE [AGAID] IS NOT NULL;
"""
    )
    index: dict[str, set[int]] = defaultdict(set)
    for row in rows:
        first = clean_name_part(row.get("FirstName"))
        middle = clean_name_part(row.get("MiddleInitial"))
        last = clean_name_part(row.get("LastName"))
        if not first or not last:
            continue
        variants = {normalize_name(f"{first} {last}")}
        if middle:
            variants.add(normalize_name(f"{first} {middle} {last}"))
            variants.add(normalize_name(f"{first} {middle.rstrip('.')} {last}"))
            variants.add(normalize_name(f"{first} {middle.rstrip('.')}. {last}"))
        for variant in variants:
            index[variant].add(int(row["AGAID"]))
    return index


def build_desired_assignments(
    payload: dict[str, Any],
    chapters: dict[int, dict[str, Any]],
    member_index: dict[str, set[int]],
) -> list[DesiredAssignment]:
    """Normalize extracted assignments and resolve unambiguous member ids."""
    desired: list[DesiredAssignment] = []
    unknown_ids: list[int] = []
    input_ids = {int(chapter["chapterFinderId"]) for chapter in payload["chapters"]}
    missing_ids = sorted(set(chapters) - input_ids)
    if missing_ids:
        raise ValueError(f"Input omits current Chapter Finder ids: {missing_ids}")
    for chapter in payload["chapters"]:
        chapter_finder_id = int(chapter["chapterFinderId"])
        current = chapters.get(chapter_finder_id)
        if current is None:
            unknown_ids.append(chapter_finder_id)
            continue
        duplicate_counts: dict[str, int] = defaultdict(int)
        for raw_name in chapter["administratorNames"]:
            display_name = clean_display_name(raw_name)
            if not display_name:
                raise ValueError(f"Chapter {chapter_finder_id} contains a blank administrator name.")
            normalized = normalize_name(display_name)
            duplicate_counts[normalized] += 1
            duplicate_ordinal = duplicate_counts[normalized]
            candidates = sorted(member_index.get(normalized, set()))
            if len(candidates) == 1:
                membership_agaid = candidates[0]
                match_status = "matched_unique"
            elif candidates:
                membership_agaid = None
                match_status = "ambiguous"
            else:
                membership_agaid = None
                match_status = "unmatched"
            assignment_key = hashlib.sha256(
                f"{chapter_finder_id}\0{normalized}\0{duplicate_ordinal}".encode("utf-8")
            ).hexdigest()
            desired.append(
                DesiredAssignment(
                    chapter_finder_entry_id=int(current["Chapter_Finder_Entry_ID"]),
                    chapter_finder_id=chapter_finder_id,
                    chapter_name=str(current["Chapter_Name"]),
                    administrator_display_name=display_name,
                    normalized_display_name=normalized,
                    duplicate_ordinal=duplicate_ordinal,
                    assignment_key_sha256=assignment_key,
                    membership_agaid=membership_agaid,
                    member_match_status=match_status,
                    member_match_candidate_count=len(candidates),
                )
            )
    if unknown_ids:
        raise ValueError(f"Input contains Chapter Finder ids not present in current SQL: {unknown_ids}")
    return desired


def load_existing_assignments(adapter: SqlAdapter) -> dict[tuple[int, str], dict[str, Any]]:
    """Load all prior assignments so inactive rows can be reactivated."""
    rows = adapter.query_rows(
        """
SELECT *
FROM [clubexpress].[chapter_finder_administrator_assignments]
WHERE [Club_ID] = ?;
""",
        (CLUB_ID,),
    )
    return {
        (int(row["Chapter_Finder_Entry_ID"]), str(row["Assignment_Key_Sha256"])): row
        for row in rows
    }


def load_status(adapter: SqlAdapter) -> dict[str, Any]:
    """Return aggregate private assignment status without exposing names."""
    assignment_rows = adapter.query_rows(
        """
SELECT
    COUNT_BIG(*) AS [Current_Assignment_Count],
    COUNT_BIG(DISTINCT [Chapter_Finder_ID]) AS [Current_Chapter_Count],
    SUM(CASE WHEN [Member_Match_Status] = N'matched_unique' THEN 1 ELSE 0 END) AS [Matched_Unique_Count],
    SUM(CASE WHEN [Member_Match_Status] = N'ambiguous' THEN 1 ELSE 0 END) AS [Ambiguous_Count],
    SUM(CASE WHEN [Member_Match_Status] = N'unmatched' THEN 1 ELSE 0 END) AS [Unmatched_Count]
FROM [clubexpress].[chapter_finder_administrator_assignments]
WHERE [Club_ID] = ?
  AND [Is_Current] = 1;
""",
        (CLUB_ID,),
    )
    multiplicity_rows = adapter.query_rows(
        """
WITH [counts] AS
(
    SELECT [Chapter_Finder_ID], COUNT_BIG(*) AS [Administrator_Count]
    FROM [clubexpress].[chapter_finder_administrator_assignments]
    WHERE [Club_ID] = ?
      AND [Is_Current] = 1
    GROUP BY [Chapter_Finder_ID]
)
SELECT
    MAX([Administrator_Count]) AS [Maximum_Administrators_Per_Chapter],
    SUM(CASE WHEN [Administrator_Count] > 1 THEN 1 ELSE 0 END) AS [Chapters_With_Multiple_Administrators]
FROM [counts];
""",
        (CLUB_ID,),
    )
    history_rows = adapter.query_rows(
        """
SELECT
    (SELECT COUNT_BIG(*) FROM [clubexpress].[chapter_finder_administrator_assignment_versions]) AS [Version_Count],
    (SELECT COUNT_BIG(*) FROM [clubexpress].[chapter_finder_administrator_sync_runs]) AS [Sync_Run_Count];
"""
    )
    return {
        **(assignment_rows[0] if assignment_rows else {}),
        **(multiplicity_rows[0] if multiplicity_rows else {}),
        **(history_rows[0] if history_rows else {}),
    }


def compare_assignments(
    desired: list[DesiredAssignment],
    existing: dict[tuple[int, str], dict[str, Any]],
) -> dict[str, int]:
    """Count inserts, updates, unchanged rows, and removals."""
    inserted = updated = unchanged = 0
    desired_keys: set[tuple[int, str]] = set()
    for item in desired:
        key = (item.chapter_finder_entry_id, item.assignment_key_sha256)
        desired_keys.add(key)
        current = existing.get(key)
        if current is None or not bool(current["Is_Current"]):
            inserted += 1
        elif assignment_changed(current, item):
            updated += 1
        else:
            unchanged += 1
    marked_inactive = sum(
        1 for key, row in existing.items() if bool(row["Is_Current"]) and key not in desired_keys
    )
    return {
        "inserted_count": inserted,
        "updated_count": updated,
        "unchanged_count": unchanged,
        "marked_inactive_count": marked_inactive,
    }


def assignment_changed(current: dict[str, Any], desired: DesiredAssignment) -> bool:
    """Return whether a current assignment's private resolution changed."""
    return any(
        (
            str(current["Administrator_Display_Name"]) != desired.administrator_display_name,
            str(current["Normalized_Display_Name"]) != desired.normalized_display_name,
            int(current["Duplicate_Ordinal"]) != desired.duplicate_ordinal,
            nullable_int(current.get("Membership_AGAID")) != desired.membership_agaid,
            str(current["Member_Match_Status"]) != desired.member_match_status,
            int(current["Member_Match_Candidate_Count"]) != desired.member_match_candidate_count,
        )
    )


def count_match_statuses(desired: list[DesiredAssignment]) -> dict[str, int]:
    """Count membership-resolution outcomes."""
    return {
        "matched_unique_count": sum(1 for item in desired if item.member_match_status == "matched_unique"),
        "ambiguous_count": sum(1 for item in desired if item.member_match_status == "ambiguous"),
        "unmatched_count": sum(1 for item in desired if item.member_match_status == "unmatched"),
    }


def start_run(
    adapter: SqlAdapter,
    run_key: str,
    extracted_at: datetime | None,
    chapter_count: int,
    chapters_with_admins: int,
    assignment_count: int,
) -> int:
    """Insert one private administrator sync run."""
    rows = adapter.query_rows(
        """
INSERT INTO [clubexpress].[chapter_finder_administrator_sync_runs]
(
    [Run_Key], [Club_ID], [Source_Url_Template], [Extracted_At], [Status],
    [Chapter_Count], [Chapters_With_Administrators_Count], [Assignment_Count]
)
OUTPUT inserted.[Chapter_Finder_Administrator_Sync_Run_ID]
VALUES (?, ?, ?, ?, N'running', ?, ?, ?);
""",
        (run_key, CLUB_ID, SOURCE_URL_TEMPLATE, extracted_at, chapter_count, chapters_with_admins, assignment_count),
    )
    return int(rows[0]["Chapter_Finder_Administrator_Sync_Run_ID"])


def apply_desired_assignments(
    adapter: SqlAdapter,
    run_id: int,
    desired: list[DesiredAssignment],
    existing: dict[tuple[int, str], dict[str, Any]],
) -> None:
    """Apply current assignment state and append versions for real changes."""
    desired_keys: set[tuple[int, str]] = set()
    for item in desired:
        key = (item.chapter_finder_entry_id, item.assignment_key_sha256)
        desired_keys.add(key)
        current = existing.get(key)
        if current is None:
            assignment_id = insert_assignment(adapter, run_id, item)
            insert_version(adapter, assignment_id, run_id, "inserted", item, True)
            continue
        assignment_id = int(current["Chapter_Finder_Administrator_Assignment_ID"])
        if not bool(current["Is_Current"]):
            update_assignment(adapter, assignment_id, run_id, item, changed=True, is_current=True)
            insert_version(adapter, assignment_id, run_id, "reactivated", item, True)
        elif assignment_changed(current, item):
            update_assignment(adapter, assignment_id, run_id, item, changed=True, is_current=True)
            insert_version(adapter, assignment_id, run_id, "resolution_changed", item, True)
        else:
            update_assignment(adapter, assignment_id, run_id, item, changed=False, is_current=True)

    for key, current in existing.items():
        if not bool(current["Is_Current"]) or key in desired_keys:
            continue
        assignment_id = int(current["Chapter_Finder_Administrator_Assignment_ID"])
        adapter.execute_statements(
            [
                (
                    """
UPDATE [clubexpress].[chapter_finder_administrator_assignments]
SET [Last_Seen_Run_ID] = ?,
    [Last_Changed_At] = SYSUTCDATETIME(),
    [Is_Current] = 0,
    [Updated_At] = SYSUTCDATETIME()
WHERE [Chapter_Finder_Administrator_Assignment_ID] = ?;
""",
                    (run_id, assignment_id),
                )
            ]
        )
        insert_version_from_row(adapter, assignment_id, run_id, "marked_inactive", current)


def insert_assignment(adapter: SqlAdapter, run_id: int, item: DesiredAssignment) -> int:
    """Insert one newly observed current assignment."""
    rows = adapter.query_rows(
        """
INSERT INTO [clubexpress].[chapter_finder_administrator_assignments]
(
    [Chapter_Finder_Entry_ID], [Club_ID], [Chapter_Finder_ID],
    [Administrator_Display_Name], [Normalized_Display_Name], [Duplicate_Ordinal],
    [Assignment_Key_Sha256], [Membership_AGAID], [Member_Match_Status],
    [Member_Match_Candidate_Count], [First_Seen_Run_ID], [Last_Seen_Run_ID], [Is_Current]
)
OUTPUT inserted.[Chapter_Finder_Administrator_Assignment_ID]
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1);
""",
        assignment_params(item, run_id),
    )
    return int(rows[0]["Chapter_Finder_Administrator_Assignment_ID"])


def update_assignment(
    adapter: SqlAdapter,
    assignment_id: int,
    run_id: int,
    item: DesiredAssignment,
    changed: bool,
    is_current: bool,
) -> None:
    """Refresh one existing assignment and its membership resolution."""
    changed_sql = "[Last_Changed_At] = SYSUTCDATETIME()," if changed else ""
    adapter.execute_statements(
        [
            (
                f"""
UPDATE [clubexpress].[chapter_finder_administrator_assignments]
SET [Administrator_Display_Name] = ?,
    [Normalized_Display_Name] = ?,
    [Duplicate_Ordinal] = ?,
    [Membership_AGAID] = ?,
    [Member_Match_Status] = ?,
    [Member_Match_Candidate_Count] = ?,
    [Last_Seen_Run_ID] = ?,
    [Last_Seen_At] = SYSUTCDATETIME(),
    {changed_sql}
    [Is_Current] = ?,
    [Updated_At] = SYSUTCDATETIME()
WHERE [Chapter_Finder_Administrator_Assignment_ID] = ?;
""",
                (
                    item.administrator_display_name,
                    item.normalized_display_name,
                    item.duplicate_ordinal,
                    item.membership_agaid,
                    item.member_match_status,
                    item.member_match_candidate_count,
                    run_id,
                    is_current,
                    assignment_id,
                ),
            )
        ]
    )


def assignment_params(item: DesiredAssignment, run_id: int) -> tuple[Any, ...]:
    """Build SQL parameters for a new assignment."""
    return (
        item.chapter_finder_entry_id,
        CLUB_ID,
        item.chapter_finder_id,
        item.administrator_display_name,
        item.normalized_display_name,
        item.duplicate_ordinal,
        item.assignment_key_sha256,
        item.membership_agaid,
        item.member_match_status,
        item.member_match_candidate_count,
        run_id,
        run_id,
    )


def insert_version(
    adapter: SqlAdapter,
    assignment_id: int,
    run_id: int,
    change_type: str,
    item: DesiredAssignment,
    is_current: bool,
) -> None:
    """Append an assignment version from desired state."""
    adapter.execute_statements(
        [
            (
                """
INSERT INTO [clubexpress].[chapter_finder_administrator_assignment_versions]
(
    [Chapter_Finder_Administrator_Assignment_ID], [Chapter_Finder_Administrator_Sync_Run_ID],
    [Change_Type], [Chapter_Finder_Entry_ID], [Club_ID], [Chapter_Finder_ID],
    [Administrator_Display_Name], [Normalized_Display_Name], [Duplicate_Ordinal],
    [Assignment_Key_Sha256], [Membership_AGAID], [Member_Match_Status],
    [Member_Match_Candidate_Count], [Is_Current]
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
""",
                (
                    assignment_id,
                    run_id,
                    change_type,
                    item.chapter_finder_entry_id,
                    CLUB_ID,
                    item.chapter_finder_id,
                    item.administrator_display_name,
                    item.normalized_display_name,
                    item.duplicate_ordinal,
                    item.assignment_key_sha256,
                    item.membership_agaid,
                    item.member_match_status,
                    item.member_match_candidate_count,
                    is_current,
                ),
            )
        ]
    )


def insert_version_from_row(
    adapter: SqlAdapter,
    assignment_id: int,
    run_id: int,
    change_type: str,
    row: dict[str, Any],
) -> None:
    """Append a removal version from an existing row."""
    item = DesiredAssignment(
        chapter_finder_entry_id=int(row["Chapter_Finder_Entry_ID"]),
        chapter_finder_id=int(row["Chapter_Finder_ID"]),
        chapter_name="",
        administrator_display_name=str(row["Administrator_Display_Name"]),
        normalized_display_name=str(row["Normalized_Display_Name"]),
        duplicate_ordinal=int(row["Duplicate_Ordinal"]),
        assignment_key_sha256=str(row["Assignment_Key_Sha256"]),
        membership_agaid=nullable_int(row.get("Membership_AGAID")),
        member_match_status=str(row["Member_Match_Status"]),
        member_match_candidate_count=int(row["Member_Match_Candidate_Count"]),
    )
    insert_version(adapter, assignment_id, run_id, change_type, item, False)


def complete_run(
    adapter: SqlAdapter,
    run_id: int,
    status: str,
    comparison: dict[str, int],
    match_counts: dict[str, int],
) -> None:
    """Complete a successful or partial sync run."""
    adapter.execute_statements(
        [
            (
                """
UPDATE [clubexpress].[chapter_finder_administrator_sync_runs]
SET [Completed_At] = SYSUTCDATETIME(),
    [Status] = ?,
    [Inserted_Count] = ?,
    [Updated_Count] = ?,
    [Unchanged_Count] = ?,
    [Marked_Inactive_Count] = ?,
    [Matched_Unique_Count] = ?,
    [Ambiguous_Count] = ?,
    [Unmatched_Count] = ?
WHERE [Chapter_Finder_Administrator_Sync_Run_ID] = ?;
""",
                (
                    status,
                    comparison["inserted_count"],
                    comparison["updated_count"],
                    comparison["unchanged_count"],
                    comparison["marked_inactive_count"],
                    match_counts["matched_unique_count"],
                    match_counts["ambiguous_count"],
                    match_counts["unmatched_count"],
                    run_id,
                ),
            )
        ]
    )


def fail_run(adapter: SqlAdapter, run_id: int, error_message: str) -> None:
    """Mark a private sync run failed."""
    adapter.execute_statements(
        [
            (
                """
UPDATE [clubexpress].[chapter_finder_administrator_sync_runs]
SET [Completed_At] = SYSUTCDATETIME(),
    [Status] = N'failed',
    [Error_Message] = ?
WHERE [Chapter_Finder_Administrator_Sync_Run_ID] = ?;
""",
                (error_message[:4000], run_id),
            )
        ]
    )


def apply_schema(adapter: SqlAdapter, schema_path: Path) -> int:
    """Apply a SQL Server schema file using GO batch separators."""
    text = schema_path.read_text(encoding="utf-8-sig")
    batches = [batch.strip() for batch in re.split(r"(?im)^\s*GO\s*;?\s*$", text) if batch.strip()]
    for batch in batches:
        adapter.execute_statements([(batch, ())])
    return len(batches)


def clean_display_name(value: Any) -> str:
    """Normalize display whitespace without altering the visible name."""
    return re.sub(r"\s+", " ", str(value or "")).strip()


def clean_name_part(value: Any) -> str:
    """Normalize one membership name component."""
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_name(value: str) -> str:
    """Build a conservative case-insensitive exact-name key."""
    text = re.sub(r"\s+", " ", value).strip().casefold()
    return re.sub(r"(?<=\b[a-z])\.(?=\s|$)", "", text)


def nullable_int(value: Any) -> int | None:
    """Convert a nullable SQL value to int."""
    return None if value is None else int(value)


def parse_extracted_at(value: Any) -> datetime | None:
    """Parse an ISO extraction timestamp."""
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    return datetime.fromisoformat(text).astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)


def json_safe(value: Any) -> Any:
    """Convert values for JSON output."""
    if isinstance(value, datetime):
        return value.isoformat()
    return value


if __name__ == "__main__":
    raise SystemExit(main())
