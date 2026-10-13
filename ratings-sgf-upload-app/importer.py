from __future__ import annotations

import base64
import binascii
import csv
import hashlib
import io
import json
import re
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import storage


MAX_ARCHIVE_BYTES = 25 * 1024 * 1024
MAX_ARCHIVE_ENTRIES = 2000
MAX_ARCHIVE_UNCOMPRESSED_BYTES = 100 * 1024 * 1024
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_SGF_BYTES = 5 * 1024 * 1024
MAX_MANIFEST_ROWS = 1000
MANIFEST_NAME = "ratings-explorer-manifest.csv"
REQUIRED_COLUMNS = (
    "Game_ID", "Tournament_Code", "Round", "Pin_Player_1", "Pin_Player_2",
    "Game_Date", "Sgf_File", "Replace_Existing",
)
SAFE_CODE = re.compile(r"^[A-Za-z0-9._-]{1,180}$")


@dataclass
class Plan:
    row_number: int
    manifest: dict[str, str]
    game: dict[str, Any] | None
    sgf_text: str
    sgf_file: str
    status: str
    action: str
    message: str
    new_sgf_code: str | None


def preview(archive_base64: str, *, archive_name: str, replace_existing: bool = False) -> dict[str, Any]:
    return _run(archive_base64, archive_name=archive_name, replace_existing=replace_existing, mode="preview")


def apply(
    archive_base64: str,
    *,
    archive_name: str,
    expected_plan_hash: str,
    replace_existing: bool = False,
) -> dict[str, Any]:
    return _run(
        archive_base64,
        archive_name=archive_name,
        replace_existing=replace_existing,
        mode="apply",
        expected_plan_hash=expected_plan_hash,
    )


def _run(
    archive_base64: str,
    *,
    archive_name: str,
    replace_existing: bool,
    mode: str,
    expected_plan_hash: str | None = None,
) -> dict[str, Any]:
    payload = _decode_archive(archive_base64)
    clean_name = _clean_archive_name(archive_name)
    archive_sha = hashlib.sha256(payload).hexdigest()
    manifest_name, rows = _read_archive(payload)
    game_ids = [int(row["Game_ID"]) for row in rows]
    games = storage.load_games(game_ids)
    plans = [_plan_row(index, row, games.get(int(row["Game_ID"])), replace_existing) for index, row in enumerate(rows, 2)]
    public_before = [_public_result(plan) for plan in plans]
    plan_hash = _plan_hash(archive_sha, public_before)

    if mode == "apply":
        if not expected_plan_hash:
            raise ValueError("plan_hash is required before applying.")
        if expected_plan_hash != plan_hash:
            raise ValueError("The archive import plan changed. Run preview again before applying.")
        if any(plan.status == "error" for plan in plans):
            raise ValueError("Resolve plan errors before applying.")
        if not any(plan.status == "ready" for plan in plans):
            raise ValueError("There are no ready SGF rows to apply.")
        _apply_plans(plans)

    results = [_public_result(plan) for plan in plans]
    tournaments: dict[str, int] = {}
    for row in rows:
        code = row["Tournament_Code"]
        tournaments[code] = tournaments.get(code, 0) + 1
    return {
        "ok": True,
        "mode": mode,
        "source_type": "archive",
        "replace_existing": replace_existing,
        "archive": {
            "name": clean_name,
            "bytes": len(payload),
            "sha256": archive_sha,
            "manifest": manifest_name,
            "rows": len(rows),
        },
        "tournaments": [{"tournament_code": code, "sgfs": count} for code, count in sorted(tournaments.items())],
        "results": results,
        "summary": _summarize(results),
        "plan_hash": plan_hash,
    }


def _decode_archive(value: str) -> bytes:
    encoded = str(value or "").strip()
    if not encoded:
        raise ValueError("Archive ZIP data is required.")
    if len(encoded) > MAX_ARCHIVE_BYTES * 2:
        raise ValueError("Archive ZIP is too large.")
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


def _read_archive(payload: bytes) -> tuple[str, list[dict[str, str]]]:
    with zipfile.ZipFile(io.BytesIO(payload), "r") as archive:
        infos = [info for info in archive.infolist() if not info.is_dir()]
        if not infos or len(infos) > MAX_ARCHIVE_ENTRIES:
            raise ValueError("Archive ZIP has an invalid number of files.")
        entries: dict[str, tuple[str, zipfile.ZipInfo]] = {}
        expanded = 0
        for info in infos:
            if info.flag_bits & 0x1:
                raise ValueError("Encrypted ZIP entries are not supported.")
            name = _safe_path(info.filename)
            folded = name.casefold()
            if folded in entries:
                raise ValueError(f"Archive ZIP contains duplicate path {name!r}.")
            expanded += int(info.file_size)
            if expanded > MAX_ARCHIVE_UNCOMPRESSED_BYTES:
                raise ValueError("Archive ZIP expands beyond 100 MB.")
            entries[folded] = (name, info)

        manifests = [name for name, _ in entries.values() if PurePosixPath(name).name.casefold() == MANIFEST_NAME.casefold()]
        if len(manifests) != 1:
            raise ValueError(f"Archive ZIP must contain exactly one {MANIFEST_NAME} file.")
        manifest_name = manifests[0]
        manifest_info = entries[manifest_name.casefold()][1]
        if manifest_info.file_size > MAX_MANIFEST_BYTES:
            raise ValueError("Archive manifest is too large.")
        manifest_text = _decode_text(archive.read(manifest_info), "archive manifest")
        reader = csv.DictReader(io.StringIO(manifest_text, newline=""))
        fields = list(reader.fieldnames or [])
        missing = [name for name in REQUIRED_COLUMNS if name not in fields]
        if missing:
            raise ValueError(f"Archive manifest is missing required column(s): {', '.join(missing)}.")
        raw_rows = [
            {key: str(value or "").strip() for key, value in row.items()}
            for row in reader
            if any(str(value or "").strip() for value in row.values())
        ]
        if not raw_rows or len(raw_rows) > MAX_MANIFEST_ROWS:
            raise ValueError("Archive manifest has an invalid number of rows.")

        parent = PurePosixPath(manifest_name).parent
        seen_ids: set[int] = set()
        seen_paths: set[str] = set()
        seen_codes: set[str] = set()
        rows: list[dict[str, str]] = []
        for row_number, row in enumerate(raw_rows, 2):
            game_id = _positive_int(row.get("Game_ID"), "Game_ID", row_number)
            if game_id in seen_ids:
                raise ValueError(f"Manifest row {row_number}: duplicate Game_ID {game_id}.")
            seen_ids.add(game_id)
            round_number = _positive_int(row.get("Round"), "Round", row_number)
            player_1 = _positive_int(row.get("Pin_Player_1"), "Pin_Player_1", row_number)
            player_2 = _positive_int(row.get("Pin_Player_2"), "Pin_Player_2", row_number)
            tournament_code = str(row.get("Tournament_Code") or "").strip()
            if not SAFE_CODE.fullmatch(tournament_code):
                raise ValueError(f"Manifest row {row_number}: Tournament_Code is invalid.")
            game_date = _date_text(row.get("Game_Date"))
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", game_date):
                raise ValueError(f"Manifest row {row_number}: Game_Date must be YYYY-MM-DD.")
            relative_path = _safe_path(row.get("Sgf_File") or "")
            archive_path = _safe_path(str(parent / PurePosixPath(relative_path)))
            if archive_path.casefold() in seen_paths:
                raise ValueError(f"Manifest row {row_number}: duplicate Sgf_File {relative_path!r}.")
            seen_paths.add(archive_path.casefold())
            entry = entries.get(archive_path.casefold())
            if entry is None:
                raise ValueError(f"Manifest row {row_number}: SGF file was not found: {relative_path}")
            info = entry[1]
            if info.file_size <= 0 or info.file_size > MAX_SGF_BYTES:
                raise ValueError(f"Manifest row {row_number}: SGF file size is invalid: {relative_path}")
            sgf_text = _decode_text(archive.read(info), f"SGF file {relative_path}")
            if not sgf_text.lstrip("\ufeff \t\r\n").startswith("(;") or not sgf_text.rstrip().endswith(")"):
                raise ValueError(f"Manifest row {row_number}: file is not a complete SGF: {relative_path}")
            requested_code = str(row.get("Sgf_Code") or "").strip()
            if requested_code and not SAFE_CODE.fullmatch(requested_code):
                raise ValueError(f"Manifest row {row_number}: Sgf_Code is invalid.")
            code = requested_code or f"archive-{_slug(tournament_code)}-{game_id}-{hashlib.sha256(sgf_text.encode('utf-8')).hexdigest()[:12]}"
            if code.casefold() in seen_codes:
                raise ValueError(f"Manifest row {row_number}: duplicate Sgf_Code {code!r}.")
            seen_codes.add(code.casefold())
            rows.append({
                **row,
                "Game_ID": str(game_id),
                "Round": str(round_number),
                "Pin_Player_1": str(player_1),
                "Pin_Player_2": str(player_2),
                "Tournament_Code": tournament_code,
                "Game_Date": game_date,
                "Sgf_File": relative_path,
                "Sgf_Code": code,
                "_Sgf_Text": sgf_text,
            })
        return manifest_name, rows


def _plan_row(row_number: int, row: dict[str, str], game: dict[str, Any] | None, allow_replace: bool) -> Plan:
    sgf_text = row["_Sgf_Text"]
    sgf_file = row["Sgf_File"]
    if game is None:
        return Plan(row_number, row, None, sgf_text, sgf_file, "error", "none", f"No game found for Game_ID {row['Game_ID']}.", None)
    checks = {
        "Tournament_Code": str(game.get("Tournament_Code") or "").strip(),
        "Round": str(game.get("Round") or "").strip(),
        "Pin_Player_1": str(game.get("Pin_Player_1") or "").strip(),
        "Pin_Player_2": str(game.get("Pin_Player_2") or "").strip(),
        "Game_Date": _date_text(game.get("Game_Date")),
    }
    mismatches = [f"{name} expected '{row[name]}' but found '{actual}'" for name, actual in checks.items() if row[name] != actual]
    if mismatches:
        return Plan(row_number, row, game, sgf_text, sgf_file, "error", "none", "; ".join(mismatches), None)
    existing = str(game.get("Sgf_Code") or "").strip()
    row_replace = _truthy(row.get("Replace_Existing"))
    if existing and not (allow_replace and row_replace):
        return Plan(row_number, row, game, sgf_text, sgf_file, "skipped", "none", f"Game already has Sgf_Code '{existing}'.", None)
    action = "replace" if existing else "create"
    return Plan(row_number, row, game, sgf_text, sgf_file, "ready", action, "Validated.", row["Sgf_Code"])


def _apply_plans(plans: list[Plan]) -> None:
    for plan in plans:
        if plan.status != "ready" or not plan.game or not plan.new_sgf_code:
            continue
        try:
            storage.upload_sgf(plan.new_sgf_code, plan.sgf_text)
            storage.update_game_sgf_code(int(plan.game["Game_ID"]), plan.new_sgf_code)
            plan.game["Sgf_Code"] = plan.new_sgf_code
            plan.status = "applied"
            plan.message = "Uploaded SGF and updated ratings.games.Sgf_Code."
        except Exception as exc:
            plan.status = "error"
            plan.message = str(exc)


def _public_result(plan: Plan) -> dict[str, Any]:
    game = plan.game or {}
    return {
        "row_number": plan.row_number,
        "status": plan.status,
        "action": plan.action,
        "message": plan.message,
        "game_id": game.get("Game_ID") or int(plan.manifest["Game_ID"]),
        "tournament_code": game.get("Tournament_Code") or plan.manifest["Tournament_Code"],
        "game_date": _date_text(game.get("Game_Date") or plan.manifest["Game_Date"]),
        "round": game.get("Round") or int(plan.manifest["Round"]),
        "existing_sgf_code": game.get("Sgf_Code"),
        "new_sgf_code": plan.new_sgf_code,
        "sgf_file": plan.sgf_file,
    }


def _plan_hash(archive_sha: str, results: list[dict[str, Any]]) -> str:
    canonical = json.dumps(results, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(f"{archive_sha}:{canonical}".encode("utf-8")).hexdigest()


def _summarize(results: list[dict[str, Any]]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for result in results:
        status = str(result["status"])
        summary[status] = summary.get(status, 0) + 1
    return summary


def _safe_path(value: str) -> str:
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


def _positive_int(value: Any, field: str, row_number: int) -> int:
    try:
        parsed = int(str(value or "").strip())
    except ValueError as exc:
        raise ValueError(f"Manifest row {row_number}: {field} must be numeric.") from exc
    if parsed <= 0:
        raise ValueError(f"Manifest row {row_number}: {field} must be positive.")
    return parsed


def _date_text(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    return str(value or "").strip()[:10]


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "tournament"
