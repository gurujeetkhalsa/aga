# Copyright 2026, American Go Association, All rights reserved

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

import chapter_finder_update_support as portal  # noqa: E402


class FakeAdapter:
    """Return configured member rows for administrator resolution tests."""

    def __init__(self, members=None):
        self.members = members or {}

    def query_rows(self, query, params=()):
        if "FROM [membership].[members]" in query:
            row = self.members.get(int(tuple(params)[0]))
            return [row] if row else []
        raise AssertionError(f"Unexpected query: {query}")


class FakeSubmissionAdapter:
    """Emulate the SQL reads and capture submission writes."""

    def __init__(self):
        self.statements = []
        self.admin_rows = [
            {
                "Chapter_Finder_Administrator_Assignment_ID": 44,
                "Administrator_Display_Name": "Current Admin",
                "Membership_AGAID": 123,
                "Member_Match_Status": "matched_unique",
            }
        ]
        self.entry = {
            "Chapter_Finder_Update_Access_Link_ID": 8,
            "Expires_At": None,
            "Is_Active": True,
            "Chapter_Finder_Entry_ID": 5,
            "Club_ID": portal.CLUB_ID,
            "Chapter_Finder_ID": 77,
            "ClubExpress_Chapter_ID": 88,
            "Chapter_Name": "Test Go Club",
            "Marker_Name": "Test Go Club (88)",
            "Latitude": None,
            "Longitude": None,
            "Map_Icon": 1,
            "On_ClubExpress": True,
            "Address_Text": "1 Main Street",
            "Postal_Text": "Testville, TX 70000",
            "City": "Testville",
            "State_Province": "TX",
            "Postal_Code": "70000",
            "Contact_Name": "Contact Person",
            "Contact_Email": "contact@example.org",
            "Contact_Phone": None,
            "Website_Url": "https://example.org",
            "Website_Label": "Example",
            "Description_Text": "Current description",
            "Organization_Parent": "Texas",
            "Org_Chart_Url": None,
            "Content_Sha256": "a" * 64,
        }

    def query_rows(self, query, params=()):
        if "chapter_finder_update_access_links" in query:
            return [dict(self.entry)]
        if "chapter_finder_administrator_assignments" in query:
            return list(self.admin_rows)
        if "INSERT INTO [clubexpress].[chapter_finder_update_requests]" in query:
            self.request_insert = (query, tuple(params))
            return [{"Chapter_Finder_Update_Request_ID": 91, "Request_Key": "test-key", "Created_At": "now"}]
        raise AssertionError(f"Unexpected query: {query}")

    def execute_statements(self, statements):
        self.statements.extend(statements)


class ChapterFinderUpdateSupportTests(unittest.TestCase):
    def test_token_hash_is_stable_and_does_not_return_raw_token(self):
        raw = "private-token-value"
        digest = portal.token_hash(raw)
        self.assertEqual(len(digest), 64)
        self.assertNotIn(raw, digest)
        self.assertEqual(digest, portal.token_hash(raw))

    def test_canonical_administrators_sorts_and_normalizes(self):
        result = portal.canonical_administrators(
            [
                {"Administrator_Display_Name": "  Zoe  Zee ", "Membership_AGAID": 9},
                {"display_name": "Amy Able", "membership_agaid": 2},
            ]
        )
        self.assertEqual(
            result,
            [
                {"display_name": "Amy Able", "membership_agaid": 2},
                {"display_name": "Zoe Zee", "membership_agaid": 9},
            ],
        )

    def test_unknown_public_field_is_rejected(self):
        with self.assertRaises(portal.PortalError):
            portal.build_normalized_proposed({"chapter_name": "A"}, {"unexpected": "B"})

    def test_postal_line_is_derived_and_website_label_is_read_only(self):
        result = portal.build_normalized_proposed(
            {
                "chapter_name": "Test Go Club",
                "city": "Old City",
                "state_province": "TX",
                "postal_code": "70000",
                "postal_text": "Old City, TX 70000",
                "website_label": "Website",
            },
            {
                "city": "New City",
                "postal_text": "Tampered location",
                "website_label": "Tampered label",
            },
        )
        self.assertEqual(result["postal_text"], "New City, TX 70000")
        self.assertEqual(result["website_label"], "Website")

    def test_clubexpress_directory_limit_is_enforced(self):
        values = {field: None for field in portal.EDITABLE_FIELDS}
        values["chapter_name"] = "Test Go Club"
        values["description_text"] = "x" * 501
        with self.assertRaises(portal.PortalError):
            portal.validate_public_fields(values)

    def test_clubexpress_address_line_limit_is_enforced(self):
        values = {field: None for field in portal.EDITABLE_FIELDS}
        values["chapter_name"] = "Test Go Club"
        values["address_text"] = "One\nTwo\nThree"
        with self.assertRaises(portal.PortalError):
            portal.validate_public_fields(values)

    def test_resolve_administrators_retains_unmatched_and_adds_active_member(self):
        adapter = FakeAdapter(
            {
                123: {
                    "AGAID": 123,
                    "FirstName": "New",
                    "MiddleInitial": None,
                    "LastName": "Admin",
                    "Status": "Active",
                }
            }
        )
        current = [
            {
                "Chapter_Finder_Administrator_Assignment_ID": 7,
                "Administrator_Display_Name": "Existing Admin",
                "Membership_AGAID": None,
                "Member_Match_Status": "unmatched",
            }
        ]
        result = portal.resolve_proposed_administrators(adapter, current, [7], [123])
        self.assertEqual(
            result,
            [
                {"display_name": "Existing Admin", "membership_agaid": None},
                {"display_name": "New Admin", "membership_agaid": 123},
            ],
        )

    def test_inactive_new_administrator_is_rejected(self):
        adapter = FakeAdapter(
            {
                321: {
                    "AGAID": 321,
                    "FirstName": "Past",
                    "MiddleInitial": None,
                    "LastName": "Member",
                    "Status": "Expired",
                }
            }
        )
        with self.assertRaises(portal.PortalError):
            portal.resolve_proposed_administrators(adapter, [], [], [321])

    def test_create_access_link_stores_only_hash(self):
        class LinkAdapter:
            def __init__(self):
                self.insert_params = None

            def query_rows(self, query, params=()):
                if "chapter_finder_entries" in query:
                    return [{
                        "Chapter_Finder_Entry_ID": 5,
                        "Chapter_Finder_ID": 77,
                        "ClubExpress_Chapter_ID": 88,
                        "Chapter_Name": "Test Go Club",
                    }]
                self.insert_params = tuple(params)
                return [{"Chapter_Finder_Update_Access_Link_ID": 3, "Created_At": "now"}]

        adapter = LinkAdapter()
        with patch.object(portal.secrets, "token_urlsafe", return_value="raw-private-token"):
            result = portal.create_access_link(
                adapter,
                base_url="https://example.org/update",
                chapter_finder_id=77,
            )
        self.assertIn("raw-private-token", result["url"])
        self.assertNotIn("raw-private-token", adapter.insert_params)
        self.assertIn(portal.token_hash("raw-private-token"), adapter.insert_params)

    def test_submit_update_creates_public_diff_and_retains_administrator(self):
        adapter = FakeSubmissionAdapter()
        base_admin_hash = portal.sha256_json(portal.canonical_administrators(adapter.admin_rows))
        result = portal.submit_update(
            adapter,
            "valid-token",
            {
                "base_content_sha256": "a" * 64,
                "base_administrator_sha256": base_admin_hash,
                "fields": {"description_text": "Updated description"},
                "retained_assignment_ids": [44],
                "added_administrator_agaids": [],
                "submitted_by_name": "Chapter Submitter",
                "submitted_by_email": "submitter@example.org",
                "submitted_by_role": "Representative",
                "submitter_notes": "Test request",
            },
        )
        self.assertEqual(result["request_id"], 91)
        self.assertEqual(result["changed_field_count"], 1)
        self.assertTrue(any("chapter_finder_update_request_fields" in query for query, _ in adapter.statements))


if __name__ == "__main__":
    unittest.main()
