# Copyright 2026, American Go Association, All rights reserved

import unittest
from datetime import datetime, timedelta, timezone

from event_domain import (
    EventConflict,
    EventError,
    normalize_contact,
    normalize_occurrence_ids,
    normalize_occurrence_input,
    occurrence_registration_status,
    overall_registration_status,
    public_occurrence_payload,
    validate_selection_policy,
)


NOW = datetime(2026, 8, 5, 16, 0, tzinfo=timezone.utc)


class EventDomainTests(unittest.TestCase):
    def test_minimum_contact_is_normalized(self):
        self.assertEqual(
            normalize_contact({"first_name": "  Ada ", "last_name": " Lovelace ", "email": "ADA@EXAMPLE.COM"}),
            {
                "first_name": "Ada",
                "last_name": "Lovelace",
                "email": "ada@example.com",
                "city": None,
                "state_province": None,
                "country": None,
                "postal_code": None,
            },
        )

    def test_optional_contact_location_is_normalized(self):
        contact = normalize_contact(
            {
                "first_name": "Ada",
                "last_name": "Lovelace",
                "email": "ada@example.com",
                "city": "  London ",
                "state_province": "",
                "country": " United Kingdom ",
                "postal_code": " SW1A 1AA ",
            }
        )
        self.assertEqual(contact["city"], "London")
        self.assertIsNone(contact["state_province"])
        self.assertEqual(contact["country"], "United Kingdom")
        self.assertEqual(contact["postal_code"], "SW1A 1AA")

    def test_invalid_email_is_rejected(self):
        with self.assertRaises(EventError):
            normalize_contact({"first_name": "Ada", "last_name": "Lovelace", "email": "invalid"})

    def test_occurrence_ids_are_unique_and_sorted(self):
        self.assertEqual(normalize_occurrence_ids([7, "3", 7]), [3, 7])

    def test_single_selection_policy_rejects_multiple_dates(self):
        with self.assertRaises(EventError):
            validate_selection_policy([1, 2], "single", 1)

    def test_multiple_selection_policy_honors_configured_limit(self):
        validate_selection_policy([1, 2], "multiple", 2)
        with self.assertRaises(EventError):
            validate_selection_policy([1, 2, 3], "multiple", 2)

    def test_capacity_assigns_confirmation_then_waitlist(self):
        self.assertEqual(occurrence_registration_status(capacity=10, confirmed_count=9, waitlist_enabled=True), "confirmed")
        self.assertEqual(occurrence_registration_status(capacity=10, confirmed_count=10, waitlist_enabled=True), "waitlisted")

    def test_full_event_without_waitlist_conflicts(self):
        with self.assertRaises(EventConflict):
            occurrence_registration_status(capacity=10, confirmed_count=10, waitlist_enabled=False)

    def test_unlimited_capacity_is_confirmed(self):
        self.assertEqual(occurrence_registration_status(capacity=None, confirmed_count=1000, waitlist_enabled=False), "confirmed")

    def test_overall_status_supports_multi_occurrence_registration(self):
        self.assertEqual(overall_registration_status(["confirmed", "confirmed"]), "confirmed")
        self.assertEqual(overall_registration_status(["waitlisted", "waitlisted"]), "waitlisted")
        self.assertEqual(overall_registration_status(["confirmed", "waitlisted"]), "mixed")

    def test_public_payload_shows_counts_capacity_and_waitlist(self):
        payload = public_occurrence_payload(
            {
                "lifecycle": "published",
                "starts_at_utc": NOW + timedelta(days=2),
                "capacity": 12,
                "confirmed_count": 12,
                "waitlist_enabled": True,
                "registration_opens_at_utc": NOW - timedelta(days=1),
                "registration_closes_at_utc": NOW + timedelta(days=1),
            },
            NOW,
        )
        self.assertEqual(payload["availability"], "waitlist")
        self.assertEqual(payload["remaining_capacity"], 0)
        self.assertTrue(payload["selectable"])

    def test_occurrence_input_requires_aware_dates_and_valid_order(self):
        normalized = normalize_occurrence_input(
            {
                "occurrence_key": "sep-2026",
                "title": "September Self-Pair",
                "starts_at": "2026-09-12T14:00:00-04:00",
                "ends_at": "2026-09-12T18:00:00-04:00",
                "capacity": 32,
            }
        )
        self.assertEqual(normalized["starts_at_utc"], datetime(2026, 9, 12, 18, 0, tzinfo=timezone.utc))
        self.assertEqual(normalized["registration_closes_at_utc"], normalized["starts_at_utc"])

    def test_occurrence_input_accepts_event_local_dates(self):
        normalized = normalize_occurrence_input(
            {
                "time_zone": "America/New_York",
                "occurrence_key": "oct-2026",
                "title": "October Self-Pair",
                "starts_at_local": "2026-10-08T18:00",
                "ends_at_local": "2026-10-08T19:00",
                "capacity": 10,
            }
        )
        self.assertEqual(normalized["starts_at_utc"], datetime(2026, 10, 8, 22, 0, tzinfo=timezone.utc))
        self.assertEqual(normalized["ends_at_utc"], datetime(2026, 10, 8, 23, 0, tzinfo=timezone.utc))

    def test_one_hour_default_respects_eastern_summer_and_winter(self):
        for day, hour in [('2026-09-17', 22), ('2027-01-14', 23)]:
            item = normalize_occurrence_input(dict(occurrence_key='workshop', title='Workshop',
                starts_at_local=day+'T18:00', time_zone='America/New_York', capacity=10))
            self.assertEqual(item['starts_at_utc'].hour, hour)
            self.assertEqual(item['ends_at_utc']-item['starts_at_utc'], timedelta(hours=1))

    def test_registration_cutoff_boundary_and_legacy_windows(self):
        row = dict(lifecycle='published', capacity=10, confirmed_count=0,
                   registration_opens_at_utc=NOW+timedelta(days=7),
                   registration_closes_at_utc=NOW-timedelta(days=1))
        for seconds, available in [(86401, True), (86400, True), (86399, False), (-1, False)]:
            row['starts_at_utc'] = NOW+timedelta(seconds=seconds)
            self.assertEqual(public_occurrence_payload(row, NOW)['selectable'], available)


if __name__ == "__main__":
    unittest.main()
