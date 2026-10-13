# Copyright 2026, American Go Association, All rights reserved

"""Sync the public USGO Chapter Finder into Azure SQL."""

from __future__ import annotations

import argparse
import hashlib
import html
import http.cookiejar
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib import error, parse, request


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from bayrate import sql_adapter as bayrate_sql  # noqa: E402
from bayrate.sql_adapter import SqlAdapter, get_sql_connection_string  # noqa: E402


CLUB_ID = 454497
SOURCE_PAGE_URL = f"https://usgo.org/content.aspx?page_id=225&club_id={CLUB_ID}"
DETAIL_URL = "https://usgo.org/basic_modules/organization/cs.aspx?c={chapter_finder_id}"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)
DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parent / "sql" / "chapter_finder_schema.sql"

CURRENT_ENTRY_COLUMNS = [
    "Club_ID",
    "Chapter_Finder_ID",
    "ClubExpress_Chapter_ID",
    "Chapter_Name",
    "Marker_Name",
    "Latitude",
    "Longitude",
    "Map_Icon",
    "On_ClubExpress",
    "Address_Text",
    "Postal_Text",
    "City",
    "State_Province",
    "Postal_Code",
    "Country",
    "Contact_Name",
    "Contact_Email",
    "Contact_Phone",
    "Website_Url",
    "Website_Label",
    "Description_Text",
    "Organization_Parent",
    "Org_Chart_Url",
    "Raw_Point_Json",
    "Raw_Detail_Html",
    "Content_Sha256",
]

VERSION_COLUMNS = [
    "Chapter_Finder_Entry_ID",
    "Chapter_Finder_Sync_Run_ID",
    "Change_Type",
    *CURRENT_ENTRY_COLUMNS,
]


@dataclass(frozen=True)
class ChapterPoint:
    """Represent one map marker from the Chapter Finder page."""

    chapter_finder_id: int
    clubexpress_chapter_id: int | None
    marker_name: str
    chapter_name: str
    latitude: Decimal | None
    longitude: Decimal | None
    map_icon: int | None
    on_clubexpress: bool

    def as_json_payload(self) -> dict[str, Any]:
        """Return a JSON-safe representation of the point."""
        return {
            "chapter_finder_id": self.chapter_finder_id,
            "clubexpress_chapter_id": self.clubexpress_chapter_id,
            "marker_name": self.marker_name,
            "chapter_name": self.chapter_name,
            "latitude": str(self.latitude) if self.latitude is not None else None,
            "longitude": str(self.longitude) if self.longitude is not None else None,
            "map_icon": self.map_icon,
            "on_clubexpress": self.on_clubexpress,
        }


@dataclass(frozen=True)
class ChapterEntry:
    """Represent one normalized Chapter Finder entry."""

    club_id: int
    chapter_finder_id: int
    clubexpress_chapter_id: int | None
    chapter_name: str
    marker_name: str | None
    latitude: Decimal | None
    longitude: Decimal | None
    map_icon: int | None
    on_clubexpress: bool
    address_text: str | None
    postal_text: str | None
    city: str | None
    state_province: str | None
    postal_code: str | None
    country: str | None
    contact_name: str | None
    contact_email: str | None
    contact_phone: str | None
    website_url: str | None
    website_label: str | None
    description_text: str | None
    organization_parent: str | None
    org_chart_url: str | None
    raw_point_json: str
    raw_detail_html: str
    content_sha256: str

    def current_values(self) -> dict[str, Any]:
        """Return values keyed by SQL column name."""
        return {
            "Club_ID": self.club_id,
            "Chapter_Finder_ID": self.chapter_finder_id,
            "ClubExpress_Chapter_ID": self.clubexpress_chapter_id,
            "Chapter_Name": self.chapter_name,
            "Marker_Name": self.marker_name,
            "Latitude": self.latitude,
            "Longitude": self.longitude,
            "Map_Icon": self.map_icon,
            "On_ClubExpress": self.on_clubexpress,
            "Address_Text": self.address_text,
            "Postal_Text": self.postal_text,
            "City": self.city,
            "State_Province": self.state_province,
            "Postal_Code": self.postal_code,
            "Country": self.country,
            "Contact_Name": self.contact_name,
            "Contact_Email": self.contact_email,
            "Contact_Phone": self.contact_phone,
            "Website_Url": self.website_url,
            "Website_Label": self.website_label,
            "Description_Text": self.description_text,
            "Organization_Parent": self.organization_parent,
            "Org_Chart_Url": self.org_chart_url,
            "Raw_Point_Json": self.raw_point_json,
            "Raw_Detail_Html": self.raw_detail_html,
            "Content_Sha256": self.content_sha256,
        }


class ChapterFinderClient:
    """Fetch Chapter Finder pages with a preserved ClubExpress session."""

    def __init__(self, source_page_url: str, timeout_seconds: float) -> None:
        """Initialize the client."""
        self.source_page_url = source_page_url
        self.timeout_seconds = timeout_seconds
        cookie_jar = http.cookiejar.CookieJar()
        self.opener = request.build_opener(request.HTTPCookieProcessor(cookie_jar))

    def fetch_source_page(self) -> str:
        """Fetch the Chapter Finder map page."""
        return self._get(self.source_page_url)

    def fetch_detail(self, point: ChapterPoint) -> str:
        """Fetch one chapter detail fragment."""
        url = DETAIL_URL.format(chapter_finder_id=point.chapter_finder_id)
        if not point.on_clubexpress:
            url += "&notOnCE=true"
        return self._get(
            url,
            headers={
                "Referer": self.source_page_url,
                "X-Requested-With": "XMLHttpRequest",
            },
        )

    def _get(self, url: str, headers: dict[str, str] | None = None) -> str:
        """Execute one GET request."""
        request_headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        request_headers.update(headers or {})
        req = request.Request(url, headers=request_headers)
        with self.opener.open(req, timeout=self.timeout_seconds) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")


def main(argv: list[str] | None = None) -> int:
    """Run the sync."""
    args = build_parser().parse_args(argv)
    schema_path = Path(args.schema_file).resolve()
    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str and not args.dry_run:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2

    adapter = SqlAdapter(conn_str) if conn_str else None
    if args.apply_schema:
        if adapter is None:
            print("--apply-schema requires a SQL connection.", file=sys.stderr)
            return 2
        batch_count = apply_schema(adapter, schema_path)
        print(f"Applied schema batches: {batch_count}")

    client = ChapterFinderClient(args.source_page_url, args.timeout_seconds)
    source_html = client.fetch_source_page()
    points = parse_points(source_html)
    if args.limit:
        points = points[: args.limit]

    entries: list[ChapterEntry] = []
    failures: list[dict[str, Any]] = []
    for index, point in enumerate(points, start=1):
        try:
            detail_html = client.fetch_detail(point)
            if "session has expired" in detail_html.lower():
                raise RuntimeError("ClubExpress returned a session-expired detail fragment.")
            entries.append(parse_chapter_entry(point, detail_html, args.source_page_url))
            if args.verbose:
                print(f"{index:3d}/{len(points):3d} ok {point.chapter_finder_id} {point.chapter_name}")
        except Exception as exc:
            failures.append(
                {
                    "chapter_finder_id": point.chapter_finder_id,
                    "chapter_name": point.chapter_name,
                    "error": str(exc),
                }
            )
            print(f"{index:3d}/{len(points):3d} failed {point.chapter_finder_id} {point.chapter_name}: {exc}", file=sys.stderr)

    if args.dry_run:
        print(
            json.dumps(
                {
                    "point_count": len(points),
                    "parsed_count": len(entries),
                    "failure_count": len(failures),
                    "sample": [entry.current_values() for entry in entries[: min(3, len(entries))]],
                    "failures": failures,
                },
                default=json_safe,
                indent=2,
            )
        )
        return 0 if not failures else 1

    assert adapter is not None
    result = sync_entries(
        adapter=adapter,
        entries=entries,
        failures=failures,
        run_key=args.run_key or f"chapter-finder-sync-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        run_type=args.run_type,
        source_page_url=args.source_page_url,
        point_count=len(points),
        mark_missing_inactive=not args.no_mark_missing_inactive,
    )
    print(json.dumps(result, default=json_safe, indent=2))
    return 0 if result["status"] in {"succeeded", "partial"} else 1


def build_parser() -> argparse.ArgumentParser:
    """Build command line parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-page-url", default=SOURCE_PAGE_URL, help="Public ClubExpress Chapter Finder map page.")
    parser.add_argument("--schema-file", default=str(DEFAULT_SCHEMA_PATH), help="SQL schema file to apply.")
    parser.add_argument("--apply-schema", action="store_true", help="Apply SQL schema before syncing.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    parser.add_argument("--run-key", help="Stable sync run key. Defaults to a UTC timestamp.")
    parser.add_argument("--run-type", default="manual", choices=["manual", "scheduled", "backfill"], help="Sync run type.")
    parser.add_argument("--timeout-seconds", type=float, default=30.0, help="HTTP request timeout.")
    parser.add_argument("--limit", type=int, help="Only process the first N map points. Useful for testing.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and parse without writing SQL.")
    parser.add_argument("--no-mark-missing-inactive", action="store_true", help="Do not mark previously seen missing entries inactive.")
    parser.add_argument("--verbose", action="store_true", help="Print each successfully parsed chapter.")
    return parser


def parse_points(page_html: str) -> list[ChapterPoint]:
    """Parse the JavaScript map point array."""
    match = re.search(r"\bvar\s+points\s*=\s*(\[[\s\S]*?\]);", page_html)
    if not match:
        raise ValueError("Could not find Chapter Finder points array.")

    points_text = match.group(1)
    pattern = re.compile(
        r"\{lat:(?P<lat>-?\d+(?:\.\d+)?),"
        r"lng:(?P<lng>-?\d+(?:\.\d+)?),"
        r"id:'(?P<id>\d+)',"
        r"icon:(?P<icon>\d+),"
        r"name:'(?P<name>(?:\\'|[^'])*)',"
        r"onCE:'(?P<on_ce>true|false)'\}",
    )
    points: list[ChapterPoint] = []
    for item in pattern.finditer(points_text):
        marker_name = js_string_unescape(item.group("name"))
        chapter_name, clubexpress_chapter_id = split_marker_name(marker_name)
        points.append(
            ChapterPoint(
                chapter_finder_id=int(item.group("id")),
                clubexpress_chapter_id=clubexpress_chapter_id,
                marker_name=marker_name,
                chapter_name=chapter_name,
                latitude=Decimal(item.group("lat")),
                longitude=Decimal(item.group("lng")),
                map_icon=int(item.group("icon")),
                on_clubexpress=item.group("on_ce") == "true",
            )
        )

    if not points:
        raise ValueError("Chapter Finder points array was found but no points could be parsed.")
    return points


def split_marker_name(marker_name: str) -> tuple[str, int | None]:
    """Split marker label into chapter name and ClubExpress chapter id."""
    match = re.match(r"^(?P<name>.*?)\s*\((?P<id>\d+)\)\s*$", marker_name)
    if not match:
        return marker_name.strip(), None
    return match.group("name").strip(), int(match.group("id"))


def parse_chapter_entry(point: ChapterPoint, detail_html: str, source_page_url: str) -> ChapterEntry:
    """Normalize a detail fragment into one ChapterEntry."""
    name = clean_text(block_text(detail_html, "name_panel")) or point.chapter_name
    address_text = clean_text(block_text(detail_html, "address_block"))
    postal_text = clean_text(block_text(detail_html, "postal_block"))
    city, state_province, postal_code = parse_postal_line(postal_text)
    contact_name = clean_text(block_text(detail_html, "contact_name_block"))
    contact_email = extract_email(detail_html)
    contact_phone = clean_text(block_text(detail_html, "phone_block"))
    website_url, website_label = extract_website(detail_html, source_page_url)
    description_text = clean_text(block_text(detail_html, "text_block"))
    organization_parent = clean_text(block_text(detail_html, "parent_label"))
    org_chart_url = extract_org_chart_url(detail_html, source_page_url)
    raw_point_json = json.dumps(point.as_json_payload(), sort_keys=True, ensure_ascii=False)
    content_payload = {
        "point": point.as_json_payload(),
        "name": name,
        "address_text": address_text,
        "postal_text": postal_text,
        "city": city,
        "state_province": state_province,
        "postal_code": postal_code,
        "contact_name": contact_name,
        "contact_email": contact_email,
        "contact_phone": contact_phone,
        "website_url": website_url,
        "website_label": website_label,
        "description_text": description_text,
        "organization_parent": organization_parent,
        "org_chart_url": org_chart_url,
    }
    content_sha256 = sha256_json(content_payload)
    return ChapterEntry(
        club_id=CLUB_ID,
        chapter_finder_id=point.chapter_finder_id,
        clubexpress_chapter_id=point.clubexpress_chapter_id,
        chapter_name=name,
        marker_name=point.marker_name,
        latitude=point.latitude,
        longitude=point.longitude,
        map_icon=point.map_icon,
        on_clubexpress=point.on_clubexpress,
        address_text=address_text,
        postal_text=postal_text,
        city=city,
        state_province=state_province,
        postal_code=postal_code,
        country=None,
        contact_name=contact_name,
        contact_email=contact_email,
        contact_phone=contact_phone,
        website_url=website_url,
        website_label=website_label,
        description_text=description_text,
        organization_parent=organization_parent,
        org_chart_url=org_chart_url,
        raw_point_json=raw_point_json,
        raw_detail_html=detail_html.strip(),
        content_sha256=content_sha256,
    )


def block_text(fragment: str, element_id: str) -> str | None:
    """Extract text for an element by id."""
    raw = block_html(fragment, element_id)
    if raw is None:
        return None
    return strip_html(raw)


def block_html(fragment: str, element_id: str) -> str | None:
    """Extract inner HTML for a simple element by id."""
    pattern = re.compile(
        rf"<(?P<tag>div|span)\b[^>]*\bid=['\"]{re.escape(element_id)}['\"][^>]*>(?P<body>[\s\S]*?)</(?P=tag)>",
        re.IGNORECASE,
    )
    match = pattern.search(fragment)
    return match.group("body") if match else None


def strip_html(value: str) -> str:
    """Turn a small HTML fragment into readable text."""
    text = re.sub(r"<br\s*/?>", "\n", value, flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return html.unescape(text)


def clean_text(value: str | None) -> str | None:
    """Normalize optional text."""
    if value is None:
        return None
    lines = [re.sub(r"[ \t\r\f\v]+", " ", line).strip() for line in value.splitlines()]
    text = "\n".join(line for line in lines if line)
    return text or None


def extract_email(fragment: str) -> str | None:
    """Extract contact email."""
    block = block_html(fragment, "email_block")
    if not block:
        return None
    href, text = first_anchor(block)
    if href and href.lower().startswith("mailto:"):
        return parse.unquote(href[7:].split("?", 1)[0]).strip() or None
    return clean_text(strip_html(text or block))


def extract_website(fragment: str, base_url: str) -> tuple[str | None, str | None]:
    """Extract website URL and display label."""
    block = block_html(fragment, "website_block")
    if not block:
        return None, None
    href, text = first_anchor(block)
    if not href:
        return None, clean_text(strip_html(block))
    return absolute_url(href, base_url), clean_text(strip_html(text or ""))


def extract_org_chart_url(fragment: str, base_url: str) -> str | None:
    """Extract the org chart link."""
    for href, text in all_anchors(fragment):
        if "View in Org. Chart" in strip_html(text):
            return absolute_url(href, base_url)
    return None


def first_anchor(fragment: str) -> tuple[str | None, str | None]:
    """Return the first anchor href/text pair."""
    anchors = all_anchors(fragment)
    return anchors[0] if anchors else (None, None)


def all_anchors(fragment: str) -> list[tuple[str, str]]:
    """Return anchor href/text pairs from a fragment."""
    anchors: list[tuple[str, str]] = []
    for match in re.finditer(r"<a\b(?P<attrs>[^>]*)>(?P<text>[\s\S]*?)</a>", fragment, flags=re.IGNORECASE):
        href_match = re.search(r"\bhref\s*=\s*(['\"])(?P<href>.*?)\1", match.group("attrs"), flags=re.IGNORECASE)
        if href_match:
            anchors.append((html.unescape(href_match.group("href")), match.group("text")))
    return anchors


def parse_postal_line(postal_text: str | None) -> tuple[str | None, str | None, str | None]:
    """Parse common US postal line text."""
    if not postal_text:
        return None, None, None
    line = postal_text.splitlines()[0].strip().strip(",")
    match = re.match(
        r"^(?P<city>.*?),\s*(?P<state>[A-Z]{2})(?:\s+(?P<postal>[A-Za-z0-9][A-Za-z0-9 -]*))?$",
        line,
    )
    if not match:
        return None, None, None
    postal_code = (match.group("postal") or "").strip().strip(",")
    return match.group("city").strip(), match.group("state").strip(), postal_code or None


def absolute_url(url: str, base_url: str) -> str:
    """Resolve a URL against the source page."""
    return parse.urljoin(base_url, html.unescape(url).strip())


def js_string_unescape(value: str) -> str:
    """Unescape the small subset of JS string escapes seen in ClubExpress points."""
    return value.replace("\\'", "'").replace('\\"', '"').replace("\\\\", "\\")


def sha256_json(payload: dict[str, Any]) -> str:
    """Hash normalized JSON payload."""
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=json_safe).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def sync_entries(
    adapter: SqlAdapter,
    entries: list[ChapterEntry],
    failures: list[dict[str, Any]],
    run_key: str,
    run_type: str,
    source_page_url: str,
    point_count: int,
    mark_missing_inactive: bool,
) -> dict[str, Any]:
    """Write fetched entries to Azure SQL."""
    run_id = create_sync_run(adapter, run_key, run_type, source_page_url, point_count)
    inserted_count = 0
    updated_count = 0
    unchanged_count = 0
    marked_inactive_count = 0
    changed_ids: list[int] = []

    try:
        seen_ids = [entry.chapter_finder_id for entry in entries]
        for entry in entries:
            outcome = upsert_entry(adapter, run_id, entry)
            if outcome == "inserted":
                inserted_count += 1
                changed_ids.append(entry.chapter_finder_id)
            elif outcome == "updated":
                updated_count += 1
                changed_ids.append(entry.chapter_finder_id)
            else:
                unchanged_count += 1

        if mark_missing_inactive:
            marked_inactive_count = mark_missing_entries_inactive(adapter, run_id, CLUB_ID, seen_ids)

        status = "succeeded" if not failures else ("partial" if entries else "failed")
        summary = {
            "run_key": run_key,
            "point_count": point_count,
            "detail_success_count": len(entries),
            "detail_failure_count": len(failures),
            "inserted_count": inserted_count,
            "updated_count": updated_count,
            "unchanged_count": unchanged_count,
            "marked_inactive_count": marked_inactive_count,
            "changed_chapter_finder_ids": changed_ids,
            "failures": failures,
        }
        complete_sync_run(
            adapter,
            run_id,
            status,
            point_count,
            len(entries),
            len(failures),
            inserted_count,
            updated_count,
            unchanged_count,
            marked_inactive_count,
            summary,
            None if status != "failed" else json.dumps(failures, ensure_ascii=False),
        )
        summary["run_id"] = run_id
        summary["status"] = status
        return summary
    except Exception as exc:
        complete_sync_run(
            adapter,
            run_id,
            "failed",
            point_count,
            len(entries),
            len(failures),
            inserted_count,
            updated_count,
            unchanged_count,
            marked_inactive_count,
            {"failures": failures, "exception": str(exc)},
            str(exc),
        )
        raise


def create_sync_run(adapter: SqlAdapter, run_key: str, run_type: str, source_page_url: str, point_count: int) -> int:
    """Create sync run row."""
    rows = adapter.query_rows(
        """
INSERT INTO [clubexpress].[chapter_finder_sync_runs]
(
    [Run_Key],
    [Run_Type],
    [Source_Page_Url],
    [Point_Count]
)
OUTPUT inserted.[Chapter_Finder_Sync_Run_ID]
VALUES (?, ?, ?, ?);
""",
        (run_key, run_type, source_page_url, point_count),
    )
    return int(rows[0]["Chapter_Finder_Sync_Run_ID"])


def complete_sync_run(
    adapter: SqlAdapter,
    run_id: int,
    status: str,
    point_count: int,
    detail_success_count: int,
    detail_failure_count: int,
    inserted_count: int,
    updated_count: int,
    unchanged_count: int,
    marked_inactive_count: int,
    summary: dict[str, Any],
    error_message: str | None,
) -> None:
    """Complete sync run row."""
    adapter.execute_statements(
        [
            (
                """
UPDATE [clubexpress].[chapter_finder_sync_runs]
SET
    [Completed_At] = SYSUTCDATETIME(),
    [Status] = ?,
    [Point_Count] = ?,
    [Detail_Success_Count] = ?,
    [Detail_Failure_Count] = ?,
    [Inserted_Count] = ?,
    [Updated_Count] = ?,
    [Unchanged_Count] = ?,
    [Marked_Inactive_Count] = ?,
    [Summary_Json] = ?,
    [Error_Message] = ?
WHERE [Chapter_Finder_Sync_Run_ID] = ?;
""",
                (
                    status,
                    point_count,
                    detail_success_count,
                    detail_failure_count,
                    inserted_count,
                    updated_count,
                    unchanged_count,
                    marked_inactive_count,
                    json.dumps(summary, ensure_ascii=False, default=json_safe),
                    error_message,
                    run_id,
                ),
            )
        ]
    )


def upsert_entry(adapter: SqlAdapter, run_id: int, entry: ChapterEntry) -> str:
    """Insert, update, or touch one current entry."""
    rows = adapter.query_rows(
        """
SELECT [Chapter_Finder_Entry_ID], [Content_Sha256]
FROM [clubexpress].[chapter_finder_entries]
WHERE [Club_ID] = ? AND [Chapter_Finder_ID] = ?;
""",
        (entry.club_id, entry.chapter_finder_id),
    )
    values = entry.current_values()
    if not rows:
        columns = ", ".join(f"[{column}]" for column in CURRENT_ENTRY_COLUMNS)
        placeholders = ", ".join("?" for _ in CURRENT_ENTRY_COLUMNS)
        inserted = adapter.query_rows(
            f"""
INSERT INTO [clubexpress].[chapter_finder_entries]
(
    {columns},
    [First_Seen_Run_ID],
    [Last_Seen_Run_ID]
)
OUTPUT inserted.[Chapter_Finder_Entry_ID]
VALUES ({placeholders}, ?, ?);
""",
            tuple(values[column] for column in CURRENT_ENTRY_COLUMNS) + (run_id, run_id),
        )
        entry_id = int(inserted[0]["Chapter_Finder_Entry_ID"])
        insert_version(adapter, run_id, entry_id, "inserted", entry)
        return "inserted"

    entry_id = int(rows[0]["Chapter_Finder_Entry_ID"])
    if str(rows[0]["Content_Sha256"]).lower() == entry.content_sha256.lower():
        adapter.execute_statements(
            [
                (
                    """
UPDATE [clubexpress].[chapter_finder_entries]
SET
    [Last_Seen_Run_ID] = ?,
    [Last_Seen_At] = SYSUTCDATETIME(),
    [Is_Current] = 1,
    [Updated_At] = SYSUTCDATETIME()
WHERE [Chapter_Finder_Entry_ID] = ?;
""",
                    (run_id, entry_id),
                )
            ]
        )
        return "unchanged"

    assignments = ",\n    ".join(f"[{column}] = ?" for column in CURRENT_ENTRY_COLUMNS)
    adapter.execute_statements(
        [
            (
                f"""
UPDATE [clubexpress].[chapter_finder_entries]
SET
    {assignments},
    [Last_Seen_Run_ID] = ?,
    [Last_Seen_At] = SYSUTCDATETIME(),
    [Last_Changed_At] = SYSUTCDATETIME(),
    [Is_Current] = 1,
    [Updated_At] = SYSUTCDATETIME()
WHERE [Chapter_Finder_Entry_ID] = ?;
""",
                tuple(values[column] for column in CURRENT_ENTRY_COLUMNS) + (run_id, entry_id),
            )
        ]
    )
    insert_version(adapter, run_id, entry_id, "updated", entry)
    return "updated"


def insert_version(adapter: SqlAdapter, run_id: int, entry_id: int, change_type: str, entry: ChapterEntry) -> None:
    """Insert one version row."""
    values = {
        "Chapter_Finder_Entry_ID": entry_id,
        "Chapter_Finder_Sync_Run_ID": run_id,
        "Change_Type": change_type,
        **entry.current_values(),
    }
    columns = ", ".join(f"[{column}]" for column in VERSION_COLUMNS)
    placeholders = ", ".join("?" for _ in VERSION_COLUMNS)
    adapter.execute_statements(
        [
            (
                f"""
INSERT INTO [clubexpress].[chapter_finder_entry_versions]
(
    {columns}
)
VALUES ({placeholders});
""",
                tuple(values[column] for column in VERSION_COLUMNS),
            )
        ]
    )


def mark_missing_entries_inactive(adapter: SqlAdapter, run_id: int, club_id: int, seen_ids: list[int]) -> int:
    """Mark current rows missing from a successful source fetch inactive."""
    if not seen_ids:
        return 0
    placeholders = ", ".join("?" for _ in seen_ids)
    rows = adapter.query_rows(
        f"""
SELECT *
FROM [clubexpress].[chapter_finder_entries]
WHERE [Club_ID] = ?
  AND [Is_Current] = 1
  AND [Chapter_Finder_ID] NOT IN ({placeholders});
""",
        (club_id, *seen_ids),
    )
    for row in rows:
        adapter.execute_statements(
            [
                (
                    """
UPDATE [clubexpress].[chapter_finder_entries]
SET
    [Is_Current] = 0,
    [Last_Seen_Run_ID] = ?,
    [Updated_At] = SYSUTCDATETIME()
WHERE [Chapter_Finder_Entry_ID] = ?;
""",
                    (run_id, row["Chapter_Finder_Entry_ID"]),
                )
            ]
        )
        insert_version_for_existing_row(adapter, run_id, "marked_inactive", row)
    return len(rows)


def insert_version_for_existing_row(adapter: SqlAdapter, run_id: int, change_type: str, row: dict[str, Any]) -> None:
    """Insert a version row from an existing current row."""
    values = {
        "Chapter_Finder_Entry_ID": row["Chapter_Finder_Entry_ID"],
        "Chapter_Finder_Sync_Run_ID": run_id,
        "Change_Type": change_type,
        **{column: row[column] for column in CURRENT_ENTRY_COLUMNS},
    }
    columns = ", ".join(f"[{column}]" for column in VERSION_COLUMNS)
    placeholders = ", ".join("?" for _ in VERSION_COLUMNS)
    adapter.execute_statements(
        [
            (
                f"""
INSERT INTO [clubexpress].[chapter_finder_entry_versions]
(
    {columns}
)
VALUES ({placeholders});
""",
                tuple(values[column] for column in VERSION_COLUMNS),
            )
        ]
    )


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


def json_safe(value: Any) -> Any:
    """Return JSON-safe values."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


if __name__ == "__main__":
    raise SystemExit(main())
