from __future__ import annotations

import csv
import hashlib
import json
import re
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlsplit

import bulk_upload_sgf_csv as bulk_upload

from .build_leago_sgf_manifest import MANIFEST_COLUMNS, normalize_date, rows_from_index
from .download_leago_aga_sgfs import (
    API_ROOT,
    DEFAULT_AGA_IDENTIFIERS,
    clean_identifier,
    download_tournament,
    event_tournament_inputs,
)


MAX_REFERENCE_COUNT = 20
REFERENCE_KEY_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def parse_leago_references(raw: Any, default_kind: str = "tournament") -> list[dict[str, str]]:
    references = _split_reference_input(raw)
    if not references:
        raise ValueError("At least one Leago event or tournament reference is required.")
    if len(references) > MAX_REFERENCE_COUNT:
        raise ValueError(f"Import at most {MAX_REFERENCE_COUNT} Leago references at a time.")
    parsed = [_parse_one_reference(reference, default_kind=default_kind) for reference in references]
    seen: set[tuple[str, str]] = set()
    unique: list[dict[str, str]] = []
    for item in parsed:
        key = (item["kind"], item["key"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def preview_leago_import(
    conn_str: str,
    raw_references: Any,
    bearer_token: str,
    *,
    default_kind: str = "tournament",
    replace_existing: bool = False,
    allow_aga_keys: list[str] | None = None,
    api_root: str = API_ROOT,
) -> dict[str, Any]:
    return _run_leago_import(
        conn_str,
        raw_references,
        bearer_token,
        mode="preview",
        default_kind=default_kind,
        replace_existing=replace_existing,
        allow_aga_keys=allow_aga_keys or [],
        api_root=api_root,
    )


def apply_leago_import(
    conn_str: str,
    raw_references: Any,
    bearer_token: str,
    *,
    expected_plan_hash: str,
    default_kind: str = "tournament",
    replace_existing: bool = False,
    allow_aga_keys: list[str] | None = None,
    api_root: str = API_ROOT,
) -> dict[str, Any]:
    return _run_leago_import(
        conn_str,
        raw_references,
        bearer_token,
        mode="apply",
        expected_plan_hash=expected_plan_hash,
        default_kind=default_kind,
        replace_existing=replace_existing,
        allow_aga_keys=allow_aga_keys or [],
        api_root=api_root,
    )


def _run_leago_import(
    conn_str: str,
    raw_references: Any,
    bearer_token: str,
    *,
    mode: str,
    default_kind: str,
    replace_existing: bool,
    allow_aga_keys: list[str],
    api_root: str,
    expected_plan_hash: str | None = None,
) -> dict[str, Any]:
    token = str(bearer_token or "").strip()
    if not token:
        raise ValueError("Leago bearer token is required.")
    references = parse_leago_references(raw_references, default_kind=default_kind)

    with tempfile.TemporaryDirectory(prefix="ratings-leago-sgf-") as tmp_dir:
        output_root = Path(tmp_dir)
        index = _download_index(
            references,
            token,
            output_root=output_root,
            allow_aga_keys=allow_aga_keys,
            api_root=api_root,
        )
        rows = rows_from_index(index, replace_existing=replace_existing)
        _resolve_rows_with_sql(conn_str, rows)
        _fill_stable_sgf_codes(rows)

        csv_path = output_root / "leago-sgf-manifest.csv"
        _write_manifest(csv_path, rows)
        plans = [bulk_upload._build_plan_row(conn_str, csv_path, index, row) for index, row in enumerate(rows, start=2)]
        planned_results = [bulk_upload._plan_to_result(plan) for plan in plans]
        plan_hash = _plan_hash(planned_results)

        if mode == "apply":
            expected = str(expected_plan_hash or "").strip()
            if not expected:
                raise ValueError("plan_hash is required before applying.")
            if expected != plan_hash:
                raise ValueError("The Leago import plan changed. Run preview again before applying.")
            if any(item.get("status") == "error" for item in planned_results):
                raise ValueError("Resolve plan errors before applying.")
            if not any(item.get("status") == "ready" for item in planned_results):
                raise ValueError("There are no ready SGF rows to apply.")
            results = bulk_upload._apply_plan(conn_str, plans)
        else:
            results = planned_results

        return {
            "ok": True,
            "mode": mode,
            "references": references,
            "replace_existing": replace_existing,
            "download": _download_summary(index),
            "manifest": {
                "rows": [_public_manifest_row(row) for row in rows],
                "columns": list(MANIFEST_COLUMNS),
            },
            "results": [_public_result(result) for result in results],
            "summary": _summarize(results),
            "plan_hash": plan_hash,
        }


def _split_reference_input(raw: Any) -> list[str]:
    if isinstance(raw, list):
        values = [str(item or "").strip() for item in raw]
    else:
        text = str(raw or "").strip()
        values = [item.strip() for item in re.split(r"[\s,]+", text)]
    return [value for value in values if value]


def _parse_one_reference(reference: str, *, default_kind: str) -> dict[str, str]:
    text = reference.strip()
    lowered_default = str(default_kind or "tournament").strip().lower()
    if lowered_default not in {"event", "tournament"}:
        raise ValueError("source_type must be event or tournament.")

    prefixed = re.match(r"^(event|tournament)\s*:\s*(.+)$", text, flags=re.IGNORECASE)
    if prefixed:
        kind = prefixed.group(1).lower()
        key = prefixed.group(2).strip()
        return _reference_payload(kind, key, reference)

    parsed = urlsplit(text)
    path = parsed.path if parsed.scheme and parsed.netloc else text
    for kind, pattern in (
        ("event", r"(?:^|/)(?:api/v\d+/)?events/([^/?#]+)"),
        ("tournament", r"(?:^|/)(?:api/v\d+/)?tournaments/([^/?#]+)"),
    ):
        match = re.search(pattern, path, flags=re.IGNORECASE)
        if match:
            return _reference_payload(kind, match.group(1), reference)

    return _reference_payload(lowered_default, text, reference)


def _reference_payload(kind: str, key: str, raw: str) -> dict[str, str]:
    clean_key = str(key or "").strip().strip("/")
    if not REFERENCE_KEY_RE.match(clean_key):
        raise ValueError(f"Could not parse a Leago {kind} key from {raw!r}.")
    return {"kind": kind, "key": clean_key, "input": raw}


def _download_index(
    references: list[dict[str, str]],
    token: str,
    *,
    output_root: Path,
    allow_aga_keys: list[str],
    api_root: str,
) -> dict[str, Any]:
    event_keys = [item["key"] for item in references if item["kind"] == "event"]
    tournament_keys = [item["key"] for item in references if item["kind"] == "tournament"]
    args = SimpleNamespace(
        api_root=api_root,
        event_key=event_keys,
        tournament_key=tournament_keys,
        tournaments_file=None,
        matches_file=None,
        output_dir=str(output_root),
        allow_aga_key=allow_aga_keys,
        skip_xml=False,
        skip_aga_export=False,
    )

    allowed_aga_ids = set(DEFAULT_AGA_IDENTIFIERS)
    allowed_aga_ids.update(clean_identifier(value) for value in allow_aga_keys)

    items: list[Any] = []
    if event_keys:
        items.extend(event_tournament_inputs(args, token))
    items.extend(tournament_keys)

    records = [download_tournament(args, item, token, allowed_aga_ids, {}) for item in items]
    return {"aga_only": True, "records": records}


def _resolve_rows_with_sql(conn_str: str, rows: list[dict[str, str]]) -> None:
    for row in rows:
        if row.get("Game_ID"):
            continue
        p1 = row.get("Pin_Player_1")
        p2 = row.get("Pin_Player_2")
        round_text = row.get("Round")
        if not (p1 and p2 and round_text):
            continue
        tournament_code = row.get("Tournament_Code") or ""
        game_date = row.get("Game_Date") or ""
        matches = bulk_upload.explorer.query_rows(
            conn_str,
            """
SELECT
    g.[Game_ID],
    g.[Tournament_Code],
    g.[Game_Date],
    g.[Round],
    g.[Pin_Player_1],
    g.[Pin_Player_2]
FROM [ratings].[games] AS g
WHERE ((g.[Pin_Player_1] = ? AND g.[Pin_Player_2] = ?) OR (g.[Pin_Player_1] = ? AND g.[Pin_Player_2] = ?))
  AND g.[Round] = ?
  AND (? = '' OR g.[Tournament_Code] = ?)
  AND (? = '' OR CAST(g.[Game_Date] AS date) = ?)
ORDER BY g.[Game_Date] DESC, g.[Game_ID] DESC
""",
            [p1, p2, p2, p1, round_text, tournament_code, tournament_code, game_date, game_date],
        )
        if len(matches) != 1:
            continue
        match = matches[0]
        row["Game_ID"] = str(match.get("Game_ID") or "")
        row["Tournament_Code"] = str(match.get("Tournament_Code") or "")
        row["Round"] = str(match.get("Round") or "")
        row["Pin_Player_1"] = str(match.get("Pin_Player_1") or "")
        row["Pin_Player_2"] = str(match.get("Pin_Player_2") or "")
        row["Game_Date"] = normalize_date(str(match.get("Game_Date") or ""))


def _fill_stable_sgf_codes(rows: list[dict[str, str]]) -> None:
    for row in rows:
        if row.get("Sgf_Code") or not row.get("Game_ID"):
            continue
        source_name = Path(row.get("Sgf_File") or "").name
        digest = hashlib.sha256(source_name.encode("utf-8")).hexdigest()[:10]
        tournament_code = _slug(row.get("Tournament_Code") or "leago")
        row["Sgf_Code"] = f"leago-{tournament_code}-{row['Game_ID']}-{digest}"


def _write_manifest(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _public_manifest_row(row: dict[str, str]) -> dict[str, str]:
    public = dict(row)
    public["Sgf_File"] = _display_sgf_path(public.get("Sgf_File"))
    return public


def _public_result(result: dict[str, Any]) -> dict[str, Any]:
    public = dict(result)
    public["sgf_file"] = _display_sgf_path(public.get("sgf_file"))
    return public


def _display_sgf_path(value: Any) -> str | None:
    if not value:
        return None
    path = Path(str(value))
    parts = path.parts
    if len(parts) >= 3 and parts[-2].lower() == "sgf":
        return "/".join(parts[-3:])
    return path.name


def _plan_hash(results: list[dict[str, Any]]) -> str:
    stable_items = []
    for result in results:
        stable_items.append(
            {
                "row_number": result.get("row_number"),
                "status": result.get("status"),
                "action": result.get("action"),
                "game_id": result.get("game_id"),
                "tournament_code": result.get("tournament_code"),
                "game_date": result.get("game_date"),
                "round": result.get("round"),
                "existing_sgf_code": result.get("existing_sgf_code"),
                "new_sgf_code": result.get("new_sgf_code"),
                "sgf_file": _display_sgf_path(result.get("sgf_file")),
            }
        )
    payload = json.dumps(stable_items, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _download_summary(index: dict[str, Any]) -> dict[str, Any]:
    records = index.get("records") or []
    return {
        "tournaments": len(records),
        "sgfs": sum(1 for record in records for sgf in record.get("sgfs") or [] if sgf.get("status") == "downloaded"),
        "errors": sum(1 for record in records for sgf in record.get("sgfs") or [] if sgf.get("status") == "error"),
        "records": [
            {
                "event_key": record.get("event_key"),
                "event_title": record.get("event_title"),
                "tournament_key": record.get("tournament_key"),
                "tournament_title": record.get("tournament_title"),
                "sgfs": sum(1 for sgf in record.get("sgfs") or [] if sgf.get("status") == "downloaded"),
                "errors": sum(1 for sgf in record.get("sgfs") or [] if sgf.get("status") == "error"),
            }
            for record in records
        ],
    }


def _summarize(results: list[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for result in results:
        status = str(result.get("status") or "unknown")
        summary[status] = summary.get(status, 0) + 1
    return summary


def _slug(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text or "leago"
