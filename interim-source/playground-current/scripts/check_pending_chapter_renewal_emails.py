"""Check Gmail sent mail for Chapter Rewards pending renewal digests."""

from __future__ import annotations

import argparse
import base64
import json
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib import error, parse, request


GMAIL_API_BASE_URL = "https://gmail.googleapis.com/gmail/v1"
GMAIL_TOKEN_URL = "https://oauth2.googleapis.com/token"


def load_function_app_settings(app_name: str, resource_group: str) -> dict[str, str]:
    az_executable = shutil.which("az") or shutil.which("az.cmd") or "az"
    completed = subprocess.run(
        [
            az_executable,
            "functionapp",
            "config",
            "appsettings",
            "list",
            "--name",
            app_name,
            "--resource-group",
            resource_group,
            "-o",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return {item["name"]: item.get("value") or "" for item in json.loads(completed.stdout)}


def require(settings: dict[str, str], name: str) -> str:
    value = settings.get(name, "")
    if not value:
        raise RuntimeError(f"Missing required setting: {name}")
    return value


def gmail_access_token(settings: dict[str, str]) -> str:
    payload = parse.urlencode(
        {
            "client_id": require(settings, "GOOGLE_WORKSPACE_CLIENT_ID"),
            "client_secret": require(settings, "GOOGLE_WORKSPACE_CLIENT_SECRET"),
            "refresh_token": require(settings, "GOOGLE_WORKSPACE_REFRESH_TOKEN"),
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")
    req = request.Request(GMAIL_TOKEN_URL, data=payload, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with request.urlopen(req, timeout=30) as response:
        body = json.loads(response.read().decode("utf-8"))
    return body["access_token"]


def gmail_json(access_token: str, path: str, query: dict[str, str] | None = None) -> dict:
    url = GMAIL_API_BASE_URL + path
    if query:
        url += "?" + parse.urlencode(query)
    req = request.Request(url, method="GET")
    req.add_header("Authorization", f"Bearer {access_token}")
    req.add_header("Accept", "application/json")
    try:
        with request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Gmail request failed: {exc.code} {detail}") from exc


def header_value(message: dict, name: str) -> str:
    for header in message.get("payload", {}).get("headers", []):
        if header.get("name", "").lower() == name.lower():
            return header.get("value", "")
    return ""


def decode_body_part(part: dict) -> str:
    data = part.get("body", {}).get("data")
    if not data:
        return ""
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("ascii")).decode("utf-8", errors="replace")


def text_body(payload: dict) -> str:
    if payload.get("mimeType") == "text/plain":
        return decode_body_part(payload)
    for part in payload.get("parts", []) or []:
        body = text_body(part)
        if body:
            return body
    return ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--app", default="aga-chapter-rewards-automation")
    parser.add_argument("--resource-group", default="aga-data-platform")
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--query", default='subject:"Pending chapter renewals after rewards debit"')
    args = parser.parse_args()

    settings = load_function_app_settings(args.app, args.resource_group)
    mailbox = require(settings, "GOOGLE_WORKSPACE_MAILBOX")
    token = gmail_access_token(settings)

    after = (datetime.now(timezone.utc).date() - timedelta(days=args.days)).strftime("%Y/%m/%d")
    query = f'in:sent ({args.query}) after:{after}'
    listed = gmail_json(
        token,
        f"/users/{parse.quote(mailbox)}/messages",
        {"q": query, "maxResults": "10"},
    )
    messages = listed.get("messages") or []
    print(f"mailbox={mailbox}")
    print(f"query={query}")
    print(f"sent_count={len(messages)}")

    for item in messages:
        message = gmail_json(
            token,
            f"/users/{parse.quote(mailbox)}/messages/{item['id']}",
            {"format": "full"},
        )
        subject = header_value(message, "Subject")
        to_value = header_value(message, "To")
        date_value = header_value(message, "Date")
        body = text_body(message.get("payload", {}))
        pending_line = next((line.strip() for line in body.splitlines() if line.startswith("Pending debited chapters:")), "")
        print(f"- date={date_value} to={to_value} subject={subject} {pending_line}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
