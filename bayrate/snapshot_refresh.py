# SPDX-FileCopyrightText: 2010 Philip Waldron
# SPDX-FileCopyrightText: 2026 American Go Association
# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import annotations

import json
import os
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_SNAPSHOT_REFRESH_URL = (
    "https://aga-ratings-explorer-display.azurewebsites.net/api/ratings-explorer/snapshot-refresh"
)
SNAPSHOT_REFRESH_URL_SETTING = "RATINGS_EXPLORER_SNAPSHOT_REFRESH_URL"
SNAPSHOT_REFRESH_KEY_SETTING = "RATINGS_EXPLORER_SNAPSHOT_REFRESH_KEY"


class SnapshotRefreshError(RuntimeError):
    """Raised when BayRate cannot queue a Ratings Explorer snapshot refresh."""


def queue_ratings_explorer_snapshot_refresh(
    *,
    endpoint_url: str | None = None,
    function_key: str | None = None,
    timeout_seconds: float = 15.0,
    opener: Callable[..., Any] = urlopen,
) -> dict[str, Any]:
    """Queue the protected Ratings Explorer snapshot refresh endpoint."""
    endpoint = (endpoint_url or os.environ.get(SNAPSHOT_REFRESH_URL_SETTING) or DEFAULT_SNAPSHOT_REFRESH_URL).strip()
    key = function_key if function_key is not None else os.environ.get(SNAPSHOT_REFRESH_KEY_SETTING)
    key = (key or "").strip()
    if not key:
        raise SnapshotRefreshError(
            f"Missing {SNAPSHOT_REFRESH_KEY_SETTING}; the production commit completed but the snapshot refresh was not queued."
        )

    request = Request(
        endpoint,
        data=b"{}",
        method="POST",
        headers={
            "Content-Type": "application/json",
            "x-functions-key": key,
        },
    )
    try:
        with opener(request, timeout=timeout_seconds) as response:
            status_code = int(response.getcode())
            response_body = response.read()
    except HTTPError as exc:
        raise SnapshotRefreshError(f"Ratings Explorer rejected the snapshot refresh request with HTTP {exc.code}.") from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise SnapshotRefreshError(f"Ratings Explorer snapshot refresh request failed: {exc}.") from exc

    if not 200 <= status_code < 300:
        raise SnapshotRefreshError(f"Ratings Explorer returned HTTP {status_code} for the snapshot refresh request.")
    try:
        payload = json.loads(response_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotRefreshError("Ratings Explorer returned an invalid snapshot refresh response.") from exc
    if not isinstance(payload, dict) or payload.get("ok") is not True or payload.get("queued") is not True:
        raise SnapshotRefreshError("Ratings Explorer did not confirm that the snapshot refresh was queued.")
    return {
        "ok": True,
        "queued": True,
        "requested_at": payload.get("requested_at"),
    }
