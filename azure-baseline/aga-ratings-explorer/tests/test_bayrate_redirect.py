# Copyright 2026, American Go Association, All rights reserved

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from bayrate_redirect import (  # noqa: E402
    DEFAULT_STANDALONE_BAYRATE_URL,
    STANDALONE_BAYRATE_URL_SETTING,
    standalone_bayrate_url,
)


class BayRateRedirectTest(unittest.TestCase):
    """Verify the legacy page redirects to a safe standalone URL."""

    def test_default_target_is_the_standalone_bayrate_app(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(standalone_bayrate_url(), DEFAULT_STANDALONE_BAYRATE_URL)

    def test_environment_override_is_supported(self) -> None:
        with patch.dict(os.environ, {STANDALONE_BAYRATE_URL_SETTING: "https://example.test/api/bayrate"}, clear=True):
            self.assertEqual(standalone_bayrate_url(), "https://example.test/api/bayrate")

    def test_relative_redirect_target_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, STANDALONE_BAYRATE_URL_SETTING):
            standalone_bayrate_url("/api/bayrate")


if __name__ == "__main__":
    unittest.main()
