from __future__ import annotations

import base64
import binascii
import csv
import hashlib
import io
import re
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

import bulk_upload_sgf_csv as bulk_upload

from .build_leago_sgf_manifest import MANIFEST_COLUMNS, normalize_date
from .workflow import _plan_hash, _public_result, _summarize, _slug


MAX_ARCHIVE_BYTES = 25 * 1024 * 1024
MAX_ARCHIVE_ENTRIES = 2000
MAX_ARCHIVE_UNCOMPRESSED_BYTES = 100 * 1024 * 1024
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_SGF_BYTES = 5 * 1024 * 1024
MAX_MANIFEST_ROWS = 1000
ARCHIVE_MANIFEST_NAME = "ratings-explorer-manifest.csv"
REQUIRED_ARCHIVE_COLUMNS = (
    "Game_ID",
    "Tournament_Code",
    "Round",
    "Pin_Player_1",
    "Pin_Player_2",
    "Game_Date",
    "Sgf_File",
    "Replace_Existing",
)
SAFE_TOURNAMENT_CODE_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
SAFE_SGF_CODE_RE = re.compile(r"^[A-Za-z0-9._-]{1,180}$")


def preview_archive_import(
    conn_str: str,
    archive_base64: str,
    *,
    archive_name: str = "sgf-archive.zip",
    replace_existing: bool = False,
) -> dict[str, Any]:
    return _run_archive_import(
        conn_str,
        archive_base64,
        archive_name=archive_name,
        mode="preview",
        replace_existing=replace_existing,
    )


def apply_archive_import(
    conn_str: str,
    archive_base64: str,
    *,
    archive_name: str = "sgf-archive.zip",
    expected_plan_hash: str,
    replace_existing: bool = False,
) -> dict[str, Any]:
    return _run_archive_import(
        conn_str,
        archive_base64,
        archive_name=archive_name,
        mode="apply",
        expected_plan_hash=expected_plan_hash,
        replace_existing=replace_existing,
    )


def _run_archive_import(
    conn_str: str,
    archive_base64: str,
    *,
    archive_name: str,
    mode: str,
    replace_existing: bool,
    expected_plan_hash: str | None = None,
) -> dict[str, Any]:
    payload = _decode_archive(archive_base64)
    display_name = _clean_archive_name(archive_name)
    archive_sha256 = hashlib.sha256(payload).hexdigest()

    with tempfile.TemporaryDirectory(prefix="ratings-archive-sgf-") as tmp_dir:
        prepared = _prepare_archive(
            payload,
            Path(tmp_dir),
            replace_existing=replace_existing,
        )
        csv_path = prepared["manifest_path"]
        rows = prepared["rows"]
        plans = [
            bulk_upload._build_plan_row(conn_str, csv_path, index, row)
            for index, row in enumerate(rows, start=2)
        ]
        planned_results = [bulk_upload._plan_to_result(plan) for plan in plans]
        plan_hash = _archive_plan_hash(archive_sha256, planned_results)

        if mode == "apply":
            expected = str(expected_plan_hash or "").strip()
            if not expected:
                raise ValueError("plan_hash is required before applying.")
            if expected != plan_hash:
                raise ValueError("The archive import plan changed. Run preview again before applying.")
            if any(item.get("status") == "error" for item in planned_results):
                raise ValueError("Resolve plan errors before applying.")
            if not any(item.get("status") == "ready" for item in planned_results):
                raise ValueError("There are no ready SGF rows to apply.")
            results = bulk_upload._apply_plan(conn_str, plans)
        else:
            results = planned_results

        display_paths = prepared["display_paths"]
        public_results = []
        for result in results:
            public = _public_result(result)
            public["sgf_file"] = display_paths.get(int(result.get("row_number") or 0), public.get("sgf_file"))
            public_results.append(public)

        tournament_counts: dict[str, int] = {}
        for row in rows:
            code = str(row.get("Tournament_Code") or "")
            tournament_counts[code] = tournament_counts.get(code, 0) + 1

        return {
            "ok": True,
            "mode": mode,
            "source_type": "archive",
            "replace_existing": replace_existing,
            "archive": {
                "name": display_name,
                "bytes": len(payload),
                "sha256": archive_sha256,
                "manifest": prepared["manifest_display_path"],
                "rows": len(rows),
            },
            "download": {
                "tournaments": len(tournament_counts),
                "sgfs": len(rows),
                "errors": 0,
                "records": [
                    {"tournament_code": code, "sgfs": count, "errors": 0}
                    for code, count in sorted(tournament_counts.items())
                ],
            },
            "manifest": {
                "rows": prepared["public_rows"],
                "columns": list(MANIFEST_COLUMNS),
            },
            "results": public_results,
            "summary": _summarize(results),
            "plan_hash": plan_hash,
        }


def _decode_archive(value: str) -> bytes:
    encoded = str(value or "").strip()
    if not encoded:
        raise ValueError("Archive ZIP data is required.")
    if len(encoded) > (MAX_ARCHIVE_BYTES * 2):
        raise ValueError(f"Archive ZIP must be {MAX_ARCHIVE_BYTES // (1024 * 1024)} MB or smaller.")
    try:
        payload = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("Archive ZIP data is not valid base64.") from exc
    if not payload or len(payload) > MAX_ARCHIVE_BYTES:
        raise ValueError(f"Archive ZIP must be {MAX_ARCHIVE_BYTES // (1024 * 1024)} MB or smaller.")
    if not zipfile.is_zipfile(io.BytesIO(payload)):
        raise ValueError("Uploaded archive is not a valid ZIP file.")
    return payload


def _clean_archive_name(value: str) -> str:
    name = Path(str(value or "sgf-archive.zip")).name.strip()
    if not name or len(name) > 200 or not name.lower().endswith(".zip"):
        raise ValueError("Archive filename must be a .zip name of 200 characters or fewer.")
    return name


def _prepare_archive(payload: bytes, output_root: Path, *, replace_existing: bool) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(payload), "r") as archive:
        file_infos = [info for info in archive.infolist() if not info.is_dir()]
        if not file_infos:
            raise ValueError("Archive ZIP contains no files.")
        if len(file_infos) > MAX_ARCHIVE_ENTRIES:
            raise ValueError(f"Archive ZIP contains more than {MAX_ARCHIVE_ENTRIES} files.")

        total_size = 0
        entries: dict[str, zipfile.ZipInfo] = {}
        canonical_names: dict[str, str] = {}
        for info in file_infos:
            if info.flag_bits & 0x1:
                raise ValueError("Encrypted ZIP entries are not supported.")
            normalized = _normalize_archive_path(info.filename)
            folded = normalized.casefold()
            if folded in entries:
                raise ValueError(f"Archive ZIP contains duplicate path {normalized!r}.")
            total_size += int(info.file_size)
            if total_size > MAX_ARCHIVE_UNCOMPRESSED_BYTES:
                raise ValueError(
                    f"Archive ZIP expands beyond {MAX_ARCHIVE_UNCOMPRESSED_BYTES // (1024 * 1024)} MB."
                )
            entries[folded] = info
            canonical_names[folded] = normalized

        manifest_matches = [
            canonical_names[key]
            for key in entries
            if PurePosixPath(canonical_names[key]).name.casefold() == ARCHIVE_MANIFEST_NAME.casefold()
        ]
        if len(manifest_matches) != 1:
            raise ValueError(f"Archive ZIP must contain exactly one {ARCHIVE_MANIFEST_NAME} file.")
        manifest_name = manifest_matches[0]
        manifest_info = entries[manifest_name.casefold()]
        if manifest_info.file_size > MAX_MANIFEST_BYTES:
            raise ValueError("Archive manifest is too large.")
        manifest_text = _decode_text(archive.read(manifest_info), "archive manifest")
        reader = csv.DictReader(io.StringIO(manifest_text, newline=""))
        fieldnames = list(reader.fieldnames or [])
        missing = [name for name in REQUIRED_ARCHIVE_COLUMNS if name not in fieldnames]
        if missing:
            raise ValueError(f"Archive manifest is missing required column(s): {', '.join(missing)}.")
        raw_rows = [
            {key: str(value or "").strip() for key, value in row.items()}
            for row in reader
            if any(str(value or "").strip() for value in row.values())
        ]
        if not raw_rows:
            raise ValueError("Archive manifest contains no data rows.")
        if len(raw_rows) > MAX_MANIFEST_ROWS:
            raise ValueError(f"Archive manifest contains more than {MAX_MANIFEST_ROWS} rows.")

        manifest_parent = PurePosixPath(manifest_name).parent
        rows: list[dict[str, str]] = []
        public_rows: list[dict[str, str]] = []
        display_paths: dict[int, str] = {}
        seen_game_ids: set[int] = set()
        seen_sgf_paths: set[str] = set()
        seen_sgf_codes: set[str] = set()

        for row_number, raw_row in enumerate(raw_rows, start=2):
            game_id = _positive_int(raw_row.get("Game_ID"), "Game_ID", row_number)
            if game_id in seen_game_ids:
                raise ValueError(f"Manifest row {row_number}: duplicate Game_ID {game_id}.")
            seen_game_ids.add(game_id)
            _positive_int(raw_row.get("Round"), "Round", row_number)
            _positive_int(raw_row.get("Pin_Player_1"), "Pin_Player_1", row_number)
            _positive_int(raw_row.get("Pin_Player_2"), "Pin_Player_2", row_number)

            tournament_code = str(raw_row.get("Tournament_Code") or "").strip()
            if not SAFE_TOURNAMENT_CODE_RE.fullmatch(tournament_code):
                raise ValueError(f"Manifest row {row_number}: Tournament_Code is invalid.")
            game_date = normalize_date(raw_row.get("Game_Date") or "")
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", game_date):
                raise ValueError(f"Manifest row {row_number}: Game_Date must be YYYY-MM-DD.")

            relative_sgf = _normalize_archive_path(raw_row.get("Sgf_File") or "")
            combined = _normalize_archive_path(str(manifest_parent / PurePosixPath(relative_sgf)))
            combined_folded = combined.casefold()
            if combined_folded in seen_sgf_paths:
                raise ValueError(f"Manifest row {row_number}: duplicate Sgf_File {relative_sgf!r}.")
            seen_sgf_paths.add(combined_folded)
            info = entries.get(combined_folded)
            if info is None:
                raise ValueError(f"Manifest row {row_number}: SGF file was not found in the archive: {relative_sgf}")
            if info.file_size <= 0 or info.file_size > MAX_SGF_BYTES:
                raise ValueError(f"Manifest row {row_number}: SGF file size is invalid: {relative_sgf}")
            sgf_text = _decode_text(archive.read(info), f"SGF file {relative_sgf}")
            cleaned_sgf = sgf_text.lstrip("\ufeff \t\r\n")
            if not cleaned_sgf.startswith("(;") or not cleaned_sgf.rstrip().endswith(")"):
                raise ValueError(f"Manifest row {row_number}: file does not look like a complete SGF: {relative_sgf}")

            desired_code = str(raw_row.get("Sgf_Code") or "").strip()
            if desired_code and not SAFE_SGF_CODE_RE.fullmatch(desired_code):
                raise ValueError(f"Manifest row {row_number}: Sgf_Code is invalid.")
            if not desired_code:
                digest = hashlib.sha256(sgf_text.encode("utf-8")).hexdigest()[:12]
                desired_code = f"archive-{_slug(tournament_code)}-{game_id}-{digest}"
            desired_code_folded = desired_code.casefold()
            if desired_code_folded in seen_sgf_codes:
                raise ValueError(f"Manifest row {row_number}: duplicate Sgf_Code {desired_code!r}.")
            seen_sgf_codes.add(desired_code_folded)

            local_name = f"{row_number:05d}-{Path(relative_sgf).name}"
            local_path = output_root / "archive" / "sgf" / local_name
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_text(sgf_text, encoding="utf-8")

            row_requested_replace = _parse_bool(raw_row.get("Replace_Existing"))
            effective_replace = bool(replace_existing and row_requested_replace)
            row = {name: str(raw_row.get(name) or "").strip() for name in MANIFEST_COLUMNS}
            row.update(
                {
                    "Game_ID": str(game_id),
                    "Tournament_Code": tournament_code,
                    "Game_Date": game_date,
                    "Sgf_File": str(local_path),
                    "Replace_Existing": "true" if effective_replace else "false",
                    "Sgf_Code": desired_code,
                }
            )
            rows.append(row)
            public_row = dict(row)
            public_row["Sgf_File"] = relative_sgf
            public_rows.append(public_row)
            display_paths[row_number] = relative_sgf

    manifest_path = output_root / ARCHIVE_MANIFEST_NAME
    with manifest_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return {
        "manifest_path": manifest_path,
        "manifest_display_path": manifest_name,
        "rows": rows,
        "public_rows": public_rows,
        "display_paths": display_paths,
    }


def _normalize_archive_path(value: str) -> str:
    text = str(value or "").strip().replace("\\", "/")
    if not text or "\x00" in text or text.startswith("/") or re.match(r"^[A-Za-z]:", text):
        raise ValueError(f"Unsafe or empty archive path: {value!r}.")
    path = PurePosixPath(text)
    if any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(f"Unsafe archive path: {value!r}.")
    return "/".join(path.parts)


def _decode_text(payload: bytes, label: str) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode {label}.")


def _positive_int(value: Any, field_name: str, row_number: int) -> int:
    try:
        parsed = int(str(value or "").strip())
    except ValueError as exc:
        raise ValueError(f"Manifest row {row_number}: {field_name} must be numeric.") from exc
    if parsed <= 0:
        raise ValueError(f"Manifest row {row_number}: {field_name} must be positive.")
    return parsed


def _parse_bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _archive_plan_hash(archive_sha256: str, planned_results: list[dict[str, Any]]) -> str:
    base_plan_hash = _plan_hash(planned_results)
    return hashlib.sha256(f"{archive_sha256}:{base_plan_hash}".encode("ascii")).hexdigest()
