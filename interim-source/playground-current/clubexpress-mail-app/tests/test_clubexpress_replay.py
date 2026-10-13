# Copyright 2026, American Go Association, All rights reserved

import json
import sys
import unittest
from datetime import date, datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Iterable


APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from clubexpress_replay import (
    DownstreamCall,
    execute_downstream_calls,
    event_from_row,
    list_events,
    load_event,
    parse_downstream_calls,
    process_pending_events,
    print_event_list,
    replay_event,
    stored_procedure_statement,
)


class FakeAdapter:
    """Represent fake adapter."""
    def __init__(self, rows_by_query: dict[str, list[dict[str, Any]]] | None = None) -> None:
        """Initialize the fake adapter instance."""
        self.rows_by_query = rows_by_query or {}
        self.queries: list[tuple[str, tuple[Any, ...]]] = []
        self.executed: list[list[tuple[str, tuple[Any, ...]]]] = []

    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        """Query rows."""
        params_tuple = tuple(params)
        self.queries.append((query, params_tuple))
        for key, rows in self.rows_by_query.items():
            if key in query:
                return rows
        return []

    def execute_statements(self, statements: Iterable[tuple[str, tuple[Any, ...]]]) -> None:
        """Execute statements."""
        self.executed.append(list(statements))


def sample_event_row(**overrides: Any) -> dict[str, Any]:
    """Execute the sample event row routine."""
    row = {
        "Parsed_Event_ID": 7,
        "Message_ID": "msg-123",
        "Event_Key": "msg-123:renewal:19987",
        "Message_Type": "member_renewal",
        "Event_Type": "renewal",
        "Received_At": datetime(2026, 6, 26, 15, 45, tzinfo=timezone.utc),
        "Event_Date": date(2026, 6, 26),
        "AGAID": 19987,
        "ChapterID": None,
        "Parsed_Item_Count": 1,
        "Sender": "ClubExpress <notifications@example.test>",
        "Subject": "American Go Association - Member Renewal",
        "Blob_Path": "member_renewal/2026/06/26/msg-123",
        "Parsed_Payload_Json": json.dumps({"message_id": "msg-123", "parsed": {"AGAID": 19987}}),
        "Downstream_Payload_Json": json.dumps(
            {
                "procedures": [
                    {
                        "name": "membership.sp_process_membership_renewal",
                        "params": {"MessageId": "msg-123", "AGAID": 19987},
                    },
                    {
                        "name": "rewards.sp_record_membership_event",
                        "params": {"MessageId": "msg-123", "AGAID": 19987, "EventType": "renewal"},
                    },
                ]
            }
        ),
        "Status": "staged",
        "Attempt_Count": 0,
        "Last_Processed_At": None,
        "Last_Error_Message": None,
        "Created_At": datetime(2026, 6, 26, 15, 46, tzinfo=timezone.utc),
        "Updated_At": datetime(2026, 6, 26, 15, 46, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return row


def sample_new_member_event_row(**overrides: Any) -> dict[str, Any]:
    """Execute the sample new member event row routine."""
    return sample_event_row(
        Event_Type="new_membership",
        Event_Key="msg-123:new_membership:19987",
        Downstream_Payload_Json=json.dumps(
            {
                "procedures": [
                    {
                        "name": "membership.sp_process_new_member_email",
                        "params": {"MessageId": "msg-123", "AGAID": 19987},
                    },
                    {
                        "name": "rewards.sp_record_membership_event",
                        "params": {"MessageId": "msg-123", "AGAID": 19987, "EventType": "new_membership"},
                    },
                ]
            }
        ),
        **overrides,
    )


class ClubExpressReplayTest(unittest.TestCase):
    """Represent club express replay test."""
    def test_event_from_row_parses_payload_and_downstream_calls(self):
        """Verify that event from row parses payload and downstream calls."""
        event = event_from_row(sample_event_row())

        self.assertEqual(event.parsed_event_id, 7)
        self.assertEqual(event.event_key, "msg-123:renewal:19987")
        self.assertEqual(event.parsed_payload["parsed"]["AGAID"], 19987)
        self.assertEqual(
            [call.name for call in event.downstream_calls],
            ["membership.sp_process_membership_renewal", "rewards.sp_record_membership_event"],
        )

    def test_parse_downstream_calls_rejects_unexpected_procedure(self):
        """Verify that parse downstream calls rejects unexpected procedure."""
        with self.assertRaisesRegex(ValueError, "Unsupported downstream procedure"):
            parse_downstream_calls({"procedures": [{"name": "dbo.sp_surprise", "params": {}}]})

    def test_stored_procedure_statement_uses_named_parameters(self):
        """Verify that stored procedure statement uses named parameters."""
        statement = stored_procedure_statement(
            DownstreamCall("rewards.sp_record_membership_event", {"MessageId": "msg-123", "AGAID": 19987})
        )

        self.assertEqual(
            statement,
            ("EXEC [rewards].[sp_record_membership_event] @MessageId = ?, @AGAID = ?", ("msg-123", 19987)),
        )

    def test_list_events_applies_filters(self):
        """Verify that list events applies filters."""
        adapter = FakeAdapter({"clubexpress_parsed_events": [sample_event_row()]})

        rows = list_events(adapter, top=10, status="error", event_type="renewal")

        self.assertEqual(len(rows), 1)
        self.assertEqual(adapter.queries[0][1], (10, "error", "error", "renewal", "renewal"))

    def test_load_event_requires_exactly_one_selector(self):
        """Verify that load event requires exactly one selector."""
        adapter = FakeAdapter()

        with self.assertRaisesRegex(ValueError, "exactly one"):
            load_event(adapter)
        with self.assertRaisesRegex(ValueError, "exactly one"):
            load_event(adapter, event_key="key", parsed_event_id=1)

    def test_replay_preview_does_not_execute(self):
        """Verify that replay preview does not execute."""
        adapter = FakeAdapter()
        event = event_from_row(sample_event_row())

        result = replay_event(adapter, event)

        self.assertFalse(result.executed)
        self.assertEqual(result.status_before, "staged")
        self.assertEqual(len(result.procedure_results), 2)
        self.assertEqual(adapter.executed, [])

    def test_replay_execute_updates_status_and_runs_calls(self):
        """Verify that replay execute updates status and runs calls."""
        adapter = FakeAdapter()
        event = event_from_row(sample_event_row())

        result = replay_event(adapter, event, execute=True, confirm_replay=True)

        self.assertTrue(result.executed)
        self.assertEqual(result.status_after, "processed")
        self.assertEqual(len(adapter.executed), 3)
        self.assertIn("@Status = ?", adapter.executed[0][0][0])
        self.assertIn("membership].[sp_process_membership_renewal", adapter.executed[1][0][0])
        self.assertIn("rewards].[sp_record_membership_event", adapter.executed[1][1][0])
        self.assertIn("@ResultPayloadJson = ?", adapter.executed[2][0][0])

    def test_replay_execute_refuses_processed_without_acknowledgement(self):
        """Verify that replay execute refuses processed without acknowledgement."""
        adapter = FakeAdapter()
        event = event_from_row(sample_event_row(Status="processed"))

        with self.assertRaisesRegex(ValueError, "processed event"):
            replay_event(adapter, event, execute=True, confirm_replay=True)

    def test_execute_downstream_calls_flushes_before_row_returning_proc(self):
        """Verify that execute downstream calls flushes before row returning proc."""
        adapter = FakeAdapter({"sp_record_chapter_renewal_confirmation": [{"Recorded": 1}]})
        calls = [
            DownstreamCall("membership.sp_process_membership_renewal", {"MessageId": "msg-123"}),
            DownstreamCall("rewards.sp_record_chapter_renewal_confirmation", {"MessageId": "msg-123"}),
            DownstreamCall("rewards.sp_record_membership_event", {"MessageId": "msg-123"}),
        ]

        results = execute_downstream_calls(adapter, calls)

        self.assertEqual(len(adapter.executed), 2)
        self.assertEqual(len(adapter.queries), 1)
        self.assertEqual(results[1]["name"], "rewards.sp_record_chapter_renewal_confirmation")
        self.assertEqual(results[1]["row_count"], 1)

    def test_process_pending_events_previews_new_members_without_writes(self):
        """Verify that process pending events previews new members without writes."""
        adapter = FakeAdapter({"clubexpress_parsed_events": [sample_new_member_event_row()]})

        result = process_pending_events(adapter, event_type="new_membership", top=5)

        self.assertFalse(result.executed)
        self.assertEqual(result.selected_count, 1)
        self.assertEqual(result.processed_count, 0)
        self.assertEqual(result.results[0]["status_before"], "staged")
        self.assertEqual(adapter.queries[0][1], (5, "new_membership"))
        self.assertEqual(adapter.executed, [])

    def test_process_pending_events_executes_new_members(self):
        """Verify that process pending events executes new members."""
        adapter = FakeAdapter({"clubexpress_parsed_events": [sample_new_member_event_row()]})

        result = process_pending_events(
            adapter,
            event_type="new_membership",
            top=5,
            execute=True,
            confirm_replay=True,
            processor_name="test_new_member_processor",
        )

        self.assertTrue(result.executed)
        self.assertEqual(result.processed_count, 1)
        self.assertEqual(result.error_count, 0)
        self.assertEqual(len(adapter.executed), 3)
        self.assertIn("@Status = ?", adapter.executed[0][0][0])
        self.assertIn("membership].[sp_process_new_member_email", adapter.executed[1][0][0])
        self.assertIn("@ResultPayloadJson = ?", adapter.executed[2][0][0])

    def test_print_event_list_omits_sender_and_subject(self):
        """Verify that print event list omits sender and subject."""
        output = StringIO()

        print_event_list([event_from_row(sample_event_row())], output)

        rendered = output.getvalue()
        self.assertIn("msg-123:renewal:19987", rendered)
        self.assertNotIn("notifications@example.test", rendered)
        self.assertNotIn("American Go Association - Member Renewal", rendered)


if __name__ == "__main__":
    unittest.main()
