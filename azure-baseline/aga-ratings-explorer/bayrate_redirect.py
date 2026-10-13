# Copyright 2026, American Go Association, All rights reserved

from __future__ import annotations

import os
from urllib.parse import urlsplit


DEFAULT_STANDALONE_BAYRATE_URL = "https://aga-bayrate.azurewebsites.net/api/bayrate"
STANDALONE_BAYRATE_URL_SETTING = "BAYRATE_STANDALONE_URL"


def standalone_bayrate_url(configured_url: str | None = None) -> str:
    """Return the configured standalone BayRate operator URL."""
    target = configured_url if configured_url is not None else os.environ.get(STANDALONE_BAYRATE_URL_SETTING)
    target = (target or DEFAULT_STANDALONE_BAYRATE_URL).strip()
    parsed = urlsplit(target)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{STANDALONE_BAYRATE_URL_SETTING} must be an absolute HTTP or HTTPS URL.")
    return target
