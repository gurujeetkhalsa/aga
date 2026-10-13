import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen


API_ROOT = "https://api.leago.gg"
DEFAULT_OUTPUT_DIR = Path("data/leago-sgf/downloads")
DEFAULT_AGA_IDENTIFIERS = {
    "aga",
    "american go association",
    "american-go-association",
    "american_go_association",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download SGFs from Leago tournaments, refusing non-AGA tournament metadata."
    )
    parser.add_argument("--api-root", default=API_ROOT)
    parser.add_argument("--token-env", default="LEAGO_BEARER_TOKEN")
    parser.add_argument("--event-key", action="append", default=[])
    parser.add_argument("--tournament-key", action="append", default=[])
    parser.add_argument("--tournaments-file")
    parser.add_argument("--matches-file", help="Optional JSON file with match objects/keys when tournament metadata omits matches.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--allow-aga-key", action="append", default=[])
    parser.add_argument("--skip-xml", action="store_true")
    parser.add_argument("--skip-aga-export", action="store_true")
    return parser.parse_args()


def clean_identifier(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def safe_path_component(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r"[^A-Za-z0-9._-]+", "-", text).strip("-")
    return text or "unknown"


def read_json_file(path: str | None) -> Any:
    if not path:
        return None
    return json.loads(Path(path).read_text(encoding="utf-8"))


def bearer_headers(token: str, accept: str = "application/json") -> dict[str, str]:
    return {
        "Accept": accept,
        "Authorization": f"Bearer {token}",
        "User-Agent": "aga-ratings-explorer-sgf-import/1.0",
    }


def request_bytes(api_root: str, path_or_url: str, token: str, accept: str = "application/json") -> tuple[int, bytes, dict[str, str]]:
    url = path_or_url if path_or_url.startswith("http") else urljoin(api_root.rstrip("/") + "/", path_or_url.lstrip("/"))
    request = Request(url, headers=bearer_headers(token, accept=accept))
    try:
        with urlopen(request, timeout=60) as response:
            return response.status, response.read(), dict(response.headers.items())
    except HTTPError as exc:
        payload = exc.read()
        return exc.code, payload, dict(exc.headers.items())
    except URLError as exc:
        raise RuntimeError(f"Request failed for {url}: {exc}") from exc


def request_json(api_root: str, path_or_url: str, token: str) -> Any:
    status, payload, _headers = request_bytes(api_root, path_or_url, token)
    if status >= 400:
        raise RuntimeError(f"Leago request failed with HTTP {status}: {payload[:300]!r}")
    return json.loads(payload.decode("utf-8"))


def candidate_values(value: Any) -> list[str]:
    values: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            lowered_key = clean_identifier(key)
            if any(fragment in lowered_key for fragment in ("org", "arena", "association")):
                if isinstance(item, (str, int)):
                    values.append(clean_identifier(item))
                elif isinstance(item, dict):
                    values.extend(candidate_values(item))
        for key in ("orgKey", "organizationKey", "arenaKey", "orgName", "organizationName", "arenaName", "name", "title"):
            if key in value and isinstance(value[key], (str, int)):
                values.append(clean_identifier(value[key]))
    elif isinstance(value, list):
        for item in value:
            values.extend(candidate_values(item))
    return [item for item in values if item]


def assert_aga_tournament(tournament: dict[str, Any], allowed: set[str], inherited_metadata: dict[str, Any] | None = None) -> None:
    values = set(candidate_values(tournament))
    if inherited_metadata:
        values.update(candidate_values(inherited_metadata))
    if values.intersection(allowed):
        return
    title = tournament.get("title") or tournament.get("name") or tournament.get("key") or "unknown tournament"
    sample = ", ".join(sorted(values)[:12])
    raise ValueError(
        f"Refusing non-AGA Leago tournament '{title}'. "
        f"Known metadata values were: {sample or 'none'}."
    )


def tournament_key_from_item(item: Any) -> str:
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        for key in ("key", "tournamentKey", "id"):
            if item.get(key):
                return str(item[key])
    raise ValueError(f"Could not find tournament key in {item!r}")


def event_tournament_inputs(args: argparse.Namespace, token: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for event_key in args.event_key:
        event = request_json(args.api_root, f"/api/v1/events/{event_key}", token)
        if not isinstance(event, dict):
            raise ValueError(f"Event metadata for {event_key} was not an object.")
        event["key"] = event.get("key") or event_key
        tournaments = request_json(args.api_root, f"/api/v1/events/{event_key}/tournaments", token)
        if not isinstance(tournaments, list):
            raise ValueError(f"Event tournaments for {event_key} were not a list.")
        for tournament in tournaments:
            if not isinstance(tournament, dict):
                continue
            tournament["_leago_parent_event"] = event
            items.append(tournament)
    return items


def load_tournament_inputs(args: argparse.Namespace, token: str) -> list[Any]:
    items: list[Any] = event_tournament_inputs(args, token)
    items.extend(args.tournament_key)
    from_file = read_json_file(args.tournaments_file)
    if isinstance(from_file, list):
        items.extend(from_file)
    elif isinstance(from_file, dict):
        for key in ("tournaments", "items", "results"):
            if isinstance(from_file.get(key), list):
                items.extend(from_file[key])
                break
        else:
            items.append(from_file)
    if not items:
        raise SystemExit("Provide --event-key, --tournament-key, or --tournaments-file.")
    return items


def find_match_items(value: Any) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    if isinstance(value, dict):
        if value.get("key") and any(key in value for key in ("player1", "player2", "black", "white", "matchSetKey", "round")):
            matches.append(value)
        for item in value.values():
            matches.extend(find_match_items(item))
    elif isinstance(value, list):
        for item in value:
            matches.extend(find_match_items(item))
    return matches


def match_key_from_item(item: Any) -> str | None:
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        for key in ("key", "matchKey", "id"):
            if item.get(key):
                return str(item[key])
    return None


def list_tournament_matches(api_root: str, token: str, tournament_key: str) -> list[dict[str, Any]]:
    payload = request_json(api_root, f"/api/v1/tournaments/{tournament_key}/matches", token)
    return payload if isinstance(payload, list) else []


def list_match_records(api_root: str, token: str, match_key: str) -> list[dict[str, Any]]:
    payload = request_json(api_root, f"/api/v1/matches/{match_key}/records", token)
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        value = payload.get("value")
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def write_if_success(path: Path, status: int, payload: bytes) -> bool:
    if status < 200 or status >= 300 or not payload:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return True


def download_tournament(args: argparse.Namespace, item: Any, token: str, allowed_aga_ids: set[str], matches_by_tournament: dict[str, list[Any]]) -> dict[str, Any]:
    tournament_key = tournament_key_from_item(item)
    tournament = item if isinstance(item, dict) else request_json(args.api_root, f"/api/v1/tournaments/{tournament_key}", token)
    if not isinstance(tournament, dict):
        raise ValueError(f"Tournament metadata for {tournament_key} was not an object.")
    parent_event = tournament.get("_leago_parent_event") if isinstance(tournament.get("_leago_parent_event"), dict) else None
    assert_aga_tournament(tournament, allowed_aga_ids, parent_event)

    output_root = Path(args.output_dir).resolve()
    tournament_dir = output_root / safe_path_component(tournament_key)
    tournament_dir.mkdir(parents=True, exist_ok=True)
    (tournament_dir / "tournament.json").write_text(json.dumps(tournament, indent=2), encoding="utf-8")

    record: dict[str, Any] = {
        "tournament_key": tournament_key,
        "tournament_title": tournament.get("title") or tournament.get("name"),
        "event_key": parent_event.get("key") if parent_event else None,
        "event_title": parent_event.get("title") or parent_event.get("name") if parent_event else None,
        "tournament_dir": str(tournament_dir),
        "aga_only": True,
        "exports": {},
        "sgfs": [],
    }

    if not args.skip_aga_export:
        status, payload, _headers = request_bytes(args.api_root, f"/api/v1/tournaments/{tournament_key}/aga", token, accept="text/plain")
        export_path = tournament_dir / "aga-export.txt"
        if write_if_success(export_path, status, payload):
            record["exports"]["aga"] = str(export_path)

    if not args.skip_xml:
        status, payload, _headers = request_bytes(args.api_root, f"/api/v1/tournaments/{tournament_key}/xml", token, accept="application/xml")
        xml_path = tournament_dir / "tournament.xml"
        if write_if_success(xml_path, status, payload):
            record["exports"]["xml"] = str(xml_path)

    match_items = matches_by_tournament.get(tournament_key) or find_match_items(tournament)
    if not match_items:
        match_items = list_tournament_matches(args.api_root, token, tournament_key)
    seen_match_keys: set[str] = set()
    for match in match_items:
        match_key = match_key_from_item(match)
        if not match_key or match_key in seen_match_keys:
            continue
        seen_match_keys.add(match_key)
        for game_record in list_match_records(args.api_root, token, match_key):
            record_key = str(game_record.get("key") or "").strip()
            record_url = str(game_record.get("url") or "").strip()
            if not record_key.lower().endswith(".sgf"):
                continue
            status, payload, headers = request_bytes(
                args.api_root,
                record_url or f"/api/v1/matches/{match_key}/records/{record_key}",
                token,
                accept="application/x-go-sgf,text/plain,*/*",
            )
            if status < 200 or status >= 300:
                record["sgfs"].append(
                    {
                        "match_key": match_key,
                        "record_key": record_key,
                        "status": "error",
                        "http_status": status,
                    }
                )
                continue
            sgf_path = tournament_dir / "sgf" / safe_path_component(record_key)
            sgf_path.parent.mkdir(parents=True, exist_ok=True)
            sgf_path.write_bytes(payload)
            record["sgfs"].append(
                {
                    "match_key": match_key,
                    "record_key": record_key,
                    "status": "downloaded",
                    "path": str(sgf_path),
                    "bytes": len(payload),
                    "content_type": headers.get("Content-Type"),
                    "match": match if isinstance(match, dict) else None,
                    "leago_record": game_record,
                }
            )
            time.sleep(0.1)

    return record


def group_matches_by_tournament(path: str | None) -> dict[str, list[Any]]:
    payload = read_json_file(path)
    grouped: dict[str, list[Any]] = {}
    if payload is None:
        return grouped
    items = payload if isinstance(payload, list) else payload.get("matches") or payload.get("items") or []
    for item in items:
        if not isinstance(item, dict):
            continue
        tournament_key = item.get("tournamentKey") or item.get("tournament_key")
        if tournament_key:
            grouped.setdefault(str(tournament_key), []).append(item)
    return grouped


def main() -> int:
    args = parse_args()
    token = os.environ.get(args.token_env)
    if not token:
        raise SystemExit(f"Missing bearer token. Set ${args.token_env} first.")

    allowed_aga_ids = set(DEFAULT_AGA_IDENTIFIERS)
    allowed_aga_ids.update(clean_identifier(value) for value in args.allow_aga_key)
    matches_by_tournament = group_matches_by_tournament(args.matches_file)

    records = []
    for item in load_tournament_inputs(args, token):
        records.append(download_tournament(args, item, token, allowed_aga_ids, matches_by_tournament))

    output_root = Path(args.output_dir).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    index_path = output_root / "leago-download-index.json"
    index_path.write_text(json.dumps({"aga_only": True, "records": records}, indent=2), encoding="utf-8")
    print(json.dumps({"tournaments": len(records), "sgfs": sum(len(record["sgfs"]) for record in records), "index": str(index_path)}, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
