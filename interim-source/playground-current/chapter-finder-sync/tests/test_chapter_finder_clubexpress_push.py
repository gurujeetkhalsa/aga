# Copyright 2026, American Go Association, All rights reserved

from __future__ import annotations

import sys
import unittest
from pathlib import Path


SYNC_ROOT = Path(__file__).resolve().parents[1]
if str(SYNC_ROOT) not in sys.path:
    sys.path.insert(0, str(SYNC_ROOT))

import chapter_finder_clubexpress_push as push  # noqa: E402


class ChapterFinderClubExpressPushTests(unittest.TestCase):
    def test_builds_exact_public_control_writes_and_regenerates_geocoding(self):
        plan = push.build_push_plan(
            request_id=12,
            chapter_finder_id=77,
            normalized_payload={
                "chapter_name": "Test Go Club",
                "address_text": "1 Main Street\nSuite 2",
                "city": "Testville",
                "state_province": "TX",
                "postal_code": "70000",
                "description_text": "Meets Sundays.",
            },
            field_diffs=[
                {"field_name": "chapter_name"},
                {"field_name": "address_text"},
                {"field_name": "city"},
                {"field_name": "state_province"},
                {"field_name": "postal_code"},
                {"field_name": "postal_text"},
                {"field_name": "description_text"},
            ],
        )
        writes = {item["control_id"]: item for item in plan["control_writes"]}
        self.assertEqual(writes["ctl00_subgroup_name_text"]["value"], "Test Go Club")
        self.assertEqual(writes["ctl00_address_panel_address1_text"]["value"], "1 Main Street")
        self.assertEqual(writes["ctl00_address_panel_address2_text"]["value"], "Suite 2")
        self.assertEqual(writes["ctl00_parent_subgroup_dropdown"]["value"], "Texas")
        self.assertEqual(writes["ctl00_address_panel_state_dropdown"]["value"], "Texas")
        self.assertEqual(writes["ctl00_latitude"]["value"], "")
        self.assertEqual(writes["ctl00_longitude"]["value"], "")
        self.assertEqual(plan["derived_fields"], ["postal_text"])
        self.assertIn("page_id=382", plan["edit_url"])
        self.assertTrue(plan["safety"]["require_confirmation_immediately_before_save"])

    def test_builds_separate_multi_administrator_reconciliation(self):
        plan = push.build_push_plan(
            chapter_finder_id=77,
            normalized_payload={
                "administrators": [
                    {"display_name": "First Admin", "membership_agaid": 12},
                    {"display_name": "Unmatched Admin", "membership_agaid": None},
                ]
            },
            field_diffs=[{"field_name": "administrators"}],
        )
        admin_plan = plan["administrator_plan"]
        self.assertEqual(admin_plan["action"], "reconcile")
        self.assertEqual(len(admin_plan["desired_administrators"]), 2)
        self.assertIn("page_id=383", admin_plan["popup_url"])

    def test_rejects_more_than_two_address_lines(self):
        with self.assertRaises(push.PushPlanError):
            push.build_push_plan(
                chapter_finder_id=77,
                normalized_payload={"address_text": "One\nTwo\nThree"},
                field_diffs=[{"field_name": "address_text"}],
            )

    def test_rejects_changed_website_label_without_a_control(self):
        with self.assertRaises(push.PushPlanError):
            push.build_push_plan(
                chapter_finder_id=77,
                normalized_payload={"website_label": "New label"},
                field_diffs=[{"field_name": "website_label"}],
            )


if __name__ == "__main__":
    unittest.main()
