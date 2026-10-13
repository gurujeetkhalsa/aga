from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import importer


archive_path = Path(sys.argv[1]).resolve()
payload = importer.preview(
    base64.b64encode(archive_path.read_bytes()).decode("ascii"),
    archive_name=archive_path.name,
)
print(json.dumps({
    "archive": payload["archive"],
    "summary": payload["summary"],
    "tournaments": payload["tournaments"],
    "plan_hash": payload["plan_hash"],
    "first": payload["results"][0],
    "last": payload["results"][-1],
}, indent=2, default=str))
