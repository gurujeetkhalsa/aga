import json
import sys
import unittest
from datetime import date, datetime, timezone
from pathlib import Path


APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from clubexpress_staging import (
    DownstreamProcedure,
    build_chapter_renewal_notice_parsed_event,
    build_csv_attachment_parsed_event,
    build_journal_parsed_event,
    build_membership_parsed_event,
    result_payload_for_procedures,
    status_params,
)


class ClubExpressStagingTest(unittest.TestCase):
    def test_membership_event_record_params_are_json_safe_and_keyed(self):
        received_at = datetime(2026, 6, 26, 15, 45, tzinfo=timezone.utc)
        downstream = [
            DownstreamProcedure(
                "membership.sp_process_membership_renewal",
                {
                    "MessageId": "msg-123",
                    "ReceivedAt": received_at,
                    "ExpirationDate": date(2027, 6, 26),
                },
            ),
        ]

        event = build_membership_parsed_event(
            message_id="msg-123",
            message_type="member_renewal",
            event_type="renewal",
            received_at=received_at,
            event_date=date(2026, 6, 26),
            parsed={
                "AGAID": 19987,
                "MemberType": "Adult Full",
                "ExpirationDate": date(2027, 6, 26),
                "IsChapterMember": False,
            },
            downstream_procedures=downstream,
            sender="ClubExpress <notifications@example.test>",
            subject="American Go Association - Member Renewal",
            blob_path="member_renewal/2026/06/26/msg-123",
        )

        params = event.record_params()
        self.assertEqual(params["EventKey"], "msg-123:renewal:19987")
        self.assertEqual(params["AGAID"], 19987)
        self.assertIsNone(params["ChapterID"])
        self.assertEqual(params["ParsedItemCount"], 1)

        parsed_payload = json.loads(params["ParsedPayloadJson"])
        self.assertEqual(parsed_payload["parsed"]["ExpirationDate"], "2027-06-26")
        self.assertEqual(parsed_payload["blob_path"], "member_renewal/2026/06/26/msg-123")

        downstream_payload = json.loads(params["DownstreamPayloadJson"])
        self.assertEqual(downstream_payload["procedures"][0]["name"], "membership.sp_process_membership_renewal")
        self.assertEqual(downstream_payload["procedures"][0]["params"]["ReceivedAt"], received_at.isoformat())

    def test_chapter_renewal_notice_event_records_row_count(self):
        received_at = datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc)
        event = build_chapter_renewal_notice_parsed_event(
            message_id="notice-msg",
            message_type="chapter_renewal_notice",
            event_type="chapter_renewal_notice",
            received_at=received_at,
            notice_date=date(2026, 5, 5),
            parsed_rows=[
                {"source_row_number": 2, "chapter_id": 13529, "member_type": "Chapter"},
                {"source_row_number": 3, "chapter_id": 25495, "member_type": "Chapter"},
            ],
            downstream_procedures=[
                DownstreamProcedure("rewards.sp_process_chapter_renewal_notices", {"MessageId": "notice-msg"})
            ],
            subject="Membership Renewal Emails",
        )

        params = event.record_params()
        self.assertEqual(params["EventKey"], "notice-msg:chapter_renewal_notice")
        self.assertEqual(params["ParsedItemCount"], 2)
        parsed_payload = json.loads(params["ParsedPayloadJson"])
        self.assertEqual(parsed_payload["parsed"]["row_count"], 2)
        self.assertEqual(parsed_payload["parsed"]["rows"][1]["chapter_id"], 25495)

    def test_csv_attachment_event_records_archive_action(self):
        received_at = datetime(2026, 6, 26, 23, 0, tzinfo=timezone.utc)
        event = build_csv_attachment_parsed_event(
            message_id="csv-msg",
            message_type="nightly_memchap_csv",
            event_type="nightly_memchap_csv",
            received_at=received_at,
            event_date=date(2026, 6, 26),
            attachment_name="MemChap Report.csv",
            attachment_blob_name="MemChap_Report.csv",
            row_count=123,
            action_name="membership.import_memchap_csv",
            sender="ClubExpress <notifications@example.test>",
            subject="ClubExpress report",
            blob_path="nightly_memchap_csv/2026/06/26/csv-msg",
        )

        params = event.record_params()
        self.assertEqual(params["EventKey"], "csv-msg:nightly_memchap_csv")
        self.assertEqual(params["ParsedItemCount"], 123)
        parsed_payload = json.loads(params["ParsedPayloadJson"])
        self.assertEqual(parsed_payload["parsed"]["attachment_blob_name"], "MemChap_Report.csv")
        downstream_payload = json.loads(params["DownstreamPayloadJson"])
        self.assertEqual(downstream_payload["actions"][0]["name"], "membership.import_memchap_csv")
        self.assertEqual(
            downstream_payload["actions"][0]["params"]["blob_path"],
            "nightly_memchap_csv/2026/06/26/csv-msg",
        )

    def test_journal_event_records_news_and_review_match_counts(self):
        received_at = datetime(2026, 6, 26, 12, 30, tzinfo=timezone.utc)
        event = build_journal_parsed_event(
            message_id="journal-msg",
            message_type="american_go_e_journal",
            event_type="american_go_e_journal",
            received_at=received_at,
            journal_date=date(2026, 6, 26),
            parsed={
                "JournalDate": date(2026, 6, 26),
                "Articles": [{"title": "Tournament Results", "link": "https://example.test/news"}],
                "Matches": [{"agaid": 12345, "name": "News Player"}],
                "ReviewMatches": [{"agaid": 23456, "name": "Review Player"}],
            },
            downstream_procedures=[
                DownstreamProcedure(
                    "membership.sp_process_journal_news_email",
                    {"MessageId": "journal-msg", "ReviewMatchesJson": "[]"},
                )
            ],
            sender="ClubExpress <notifications@example.test>",
            subject="American Go E - Journal 6/26/2026",
            blob_path="american_go_e_journal/2026/06/26/journal-msg",
        )

        params = event.record_params()
        self.assertEqual(params["EventKey"], "journal-msg:american_go_e_journal")
        self.assertEqual(params["ParsedItemCount"], 3)
        parsed_payload = json.loads(params["ParsedPayloadJson"])
        self.assertEqual(parsed_payload["parsed"]["article_count"], 1)
        self.assertEqual(parsed_payload["parsed"]["match_count"], 1)
        self.assertEqual(parsed_payload["parsed"]["review_match_count"], 1)
        downstream_payload = json.loads(params["DownstreamPayloadJson"])
        self.assertEqual(downstream_payload["procedures"][0]["name"], "membership.sp_process_journal_news_email")

    def test_status_params_truncate_error_and_serialize_result_payload(self):
        params = status_params(
            "msg-123:renewal:19987",
            "error",
            error_message="x" * 5000,
            result_payload={"processed_at": datetime(2026, 6, 26, tzinfo=timezone.utc)},
        )

        self.assertEqual(params["EventKey"], "msg-123:renewal:19987")
        self.assertEqual(params["Status"], "error")
        self.assertEqual(len(params["ErrorMessage"]), 4000)
        result_payload = json.loads(params["ResultPayloadJson"])
        self.assertEqual(result_payload["processed_at"], "2026-06-26T00:00:00+00:00")

    def test_result_payload_for_procedures_summarizes_downstream_calls(self):
        payload = result_payload_for_procedures(
            [
                DownstreamProcedure("membership.sp_process_new_member_email", {"MessageId": "new-msg"}),
                DownstreamProcedure("rewards.sp_record_membership_event", {"MessageId": "new-msg"}),
            ],
            extra={"result_count": 2},
        )

        self.assertEqual(
            [procedure["name"] for procedure in payload["procedures"]],
            ["membership.sp_process_new_member_email", "rewards.sp_record_membership_event"],
        )
        self.assertEqual(payload["result_count"], 2)


if __name__ == "__main__":
    unittest.main()
