# SPDX-FileCopyrightText: 2010 Philip Waldron
# SPDX-FileCopyrightText: 2026 American Go Association
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import unittest
from urllib.error import HTTPError

from bayrate.snapshot_refresh import SnapshotRefreshError, queue_ratings_explorer_snapshot_refresh


class FakeResponse:
    """Minimal context-managed HTTP response for snapshot refresh tests."""

    def __init__(self, payload, status_code=200):
        """Initialize the fake response."""
        self.payload = payload
        self.status_code = status_code

    def __enter__(self):
        """Enter the fake response context."""
        return self

    def __exit__(self, exc_type, exc, traceback):
        """Exit the fake response context."""
        return False

    def getcode(self):
        """Return the fake HTTP status code."""
        return self.status_code

    def read(self):
        """Return the encoded fake JSON response."""
        return json.dumps(self.payload).encode("utf-8")


class SnapshotRefreshTest(unittest.TestCase):
    """Verify BayRate's Ratings Explorer refresh request."""

    def test_queue_posts_function_key_and_returns_confirmation(self) -> None:
        """Verify the protected refresh request and normalized result."""
        captured = {}

        def opener(request, timeout):
            captured["request"] = request
            captured["timeout"] = timeout
            return FakeResponse({"ok": True, "queued": True, "requested_at": "2026-08-27T12:00:00Z"})

        result = queue_ratings_explorer_snapshot_refresh(
            endpoint_url="https://ratings.example.test/api/ratings-explorer/snapshot-refresh",
            function_key="refresh-secret",
            timeout_seconds=7.5,
            opener=opener,
        )

        request = captured["request"]
        headers = {name.lower(): value for name, value in request.header_items()}
        self.assertEqual(request.full_url, "https://ratings.example.test/api/ratings-explorer/snapshot-refresh")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.data, b"{}")
        self.assertEqual(headers["x-functions-key"], "refresh-secret")
        self.assertEqual(captured["timeout"], 7.5)
        self.assertEqual(
            result,
            {"ok": True, "queued": True, "requested_at": "2026-08-27T12:00:00Z"},
        )

    def test_queue_requires_function_key_before_opening_connection(self) -> None:
        """Verify missing configuration cannot make an unauthenticated request."""
        opened = False

        def opener(request, timeout):
            nonlocal opened
            opened = True
            return FakeResponse({"ok": True, "queued": True})

        with self.assertRaisesRegex(SnapshotRefreshError, "RATINGS_EXPLORER_SNAPSHOT_REFRESH_KEY"):
            queue_ratings_explorer_snapshot_refresh(function_key="", opener=opener)

        self.assertFalse(opened)

    def test_queue_rejects_unconfirmed_response(self) -> None:
        """Verify BayRate requires an explicit queued confirmation."""
        with self.assertRaisesRegex(SnapshotRefreshError, "did not confirm"):
            queue_ratings_explorer_snapshot_refresh(
                function_key="refresh-secret",
                opener=lambda request, timeout: FakeResponse({"ok": True, "queued": False}),
            )

    def test_queue_reports_http_failure_without_exposing_key(self) -> None:
        """Verify endpoint failures are safe to return to the operator."""
        def opener(request, timeout):
            raise HTTPError(request.full_url, 403, "Forbidden", hdrs=None, fp=None)

        with self.assertRaises(SnapshotRefreshError) as raised:
            queue_ratings_explorer_snapshot_refresh(function_key="refresh-secret", opener=opener)

        self.assertIn("HTTP 403", str(raised.exception))
        self.assertNotIn("refresh-secret", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
