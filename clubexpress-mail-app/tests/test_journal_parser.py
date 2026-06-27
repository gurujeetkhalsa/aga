import base64
import importlib.util
import json
import os
from pathlib import Path
import sys
import unittest
from datetime import date, datetime, timezone


APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

spec = importlib.util.spec_from_file_location("clubexpress_function_app", APP_DIR / "function_app.py")
mailapp = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(mailapp)


class JournalParserTest(unittest.TestCase):
    def test_naol_review_parser_does_not_shift_videos_after_missing_iframe(self):
        html_body = """
        <html><body>
          <p><strong>Yilun Yang (7P) (8:30 to 9:30PM)</strong></p>
          <p>Dawei Zhang 2k Zhaorong Ma 2k -
            <a href="https://online-go.com/game/86799530">https://online-go.com/game/86799530</a></p>
          <p><strong>Review Video - </strong></p>

          <p><strong>Yoonyoung Kim (8P) (9 to 10 PM)</strong></p>
          <p>Ivan Lo 7d Jia Chen 4d -
            <a href="https://online-go.com/game/86922291">https://online-go.com/game/86922291</a></p>
          <p><strong>Review Video - </strong></p>
          <iframe src="https://www.youtube.com/embed/xip2xx7gFl4?si=test"></iframe>

          <p><strong>Alexander Qi (3P)</strong> <strong>(9 to 10PM)</strong></p>
          <p>Jonathan Buss 3d Riannie Duan 3d -
            <a href="https://online-go.com/game/86922119">https://online-go.com/game/86922119</a></p>
          <p><strong>Review Video - .</strong></p>
          <iframe src="https://www.youtube.com/embed/8Z-yE9I2dyo?si=test"></iframe>

          <p><strong>Soren Jaffe (6D) (9 to 10PM)</strong></p>
          <p>Maria Aozono-Araldi 6k Laura Wu 5k -
            <a href="https://online-go.com/game/86801165">https://online-go.com/game/86801165</a></p>
          <p><strong>Review Video - </strong></p>
          <iframe src="https://www.youtube.com/embed/6zUKJfdI6q0?si=test"></iframe>

          <p><strong>BenKyo Baduk (5D) (9 to 10PM)</strong></p>
          <p>Maria Aozono-Araldi 6k Benjamin Parrott 5k -
            <a href="https://online-go.com/game/86922161">https://online-go.com/game/86922161</a></p>
          <p><strong>Review Video - </strong></p>
          <iframe src="https://www.youtube.com/embed/2vniusy7OUE?si=test"></iframe>
        </body></html>
        """

        sections = mailapp._parse_naol_review_blog_html(html_body)["sections"]
        by_reviewer = {section["reviewer_name"]: section for section in sections}

        self.assertNotIn("Yilun Yang", by_reviewer)
        self.assertEqual(
            by_reviewer["Yoonyoung Kim"]["video_link"],
            "https://www.youtube.com/watch?v=xip2xx7gFl4",
        )
        self.assertEqual(
            by_reviewer["Alexander Qi"]["video_link"],
            "https://www.youtube.com/watch?v=8Z-yE9I2dyo",
        )
        self.assertEqual(
            by_reviewer["Soren Jaffe"]["video_link"],
            "https://www.youtube.com/watch?v=6zUKJfdI6q0",
        )
        self.assertEqual(
            by_reviewer["BenKyo Baduk"]["video_link"],
            "https://www.youtube.com/watch?v=2vniusy7OUE",
        )

    def test_heading_articles_use_linked_headlines_only_as_link_lookup(self):
        html_body = """
        <html><body>
          <h3>News</h3>
          <p><a href="https://example.test/shu">Shu Wins Seattle Go Center Spring Tournament</a></p>
          <p><a href="https://example.test/archive">From the Archives: None Redmond at the World Amateur Go Championships</a></p>
          <p><a href="https://example.test/vermont">Youth Takes Top Prize at Vermont Spring Go Tournament</a></p>

          <h2>Shu Wins Seattle Go Center Spring Tournament</h2>
          <p>Wenhuan Shu emerged as the undefeated winner.</p>

          <h2>From the Archives: None Redmond at the World Amateur Go Championships</h2>
          <p>None Redmond gathered interviews for the E-Journal.</p>
          <p><a href="https://example.test/ofer">5 MINUTES WITH: Ofer Zivony, Israel</a></p>

          <h2>Youth Takes Top Prize at Vermont Spring Go Tournament</h2>
          <p>Wren Perchlik went 3-0 to take second place, while Jonathan Green placed third.</p>

          <h3>Upcoming Events</h3>
          <p><a href="https://example.test/event">2026 New York Monthly Series - May - 5/2/2026</a></p>
        </body></html>
        """

        articles = mailapp._extract_journal_articles_from_html(html_body)
        titles = [article["title"] for article in articles]
        self.assertEqual(
            titles,
            [
                "Shu Wins Seattle Go Center Spring Tournament",
                "From the Archives: None Redmond at the World Amateur Go Championships",
                "Youth Takes Top Prize at Vermont Spring Go Tournament",
            ],
        )

        by_title = {article["title"]: article for article in articles}
        vermont_text = by_title["Youth Takes Top Prize at Vermont Spring Go Tournament"]["analysisText"]
        archive_text = by_title["From the Archives: None Redmond at the World Amateur Go Championships"]["analysisText"]

        self.assertEqual(by_title["Youth Takes Top Prize at Vermont Spring Go Tournament"]["link"], "https://example.test/vermont")
        self.assertIn("Jonathan Green", vermont_text)
        self.assertIn("Wren Perchlik", vermont_text)
        self.assertNotIn("Jonathan Green", archive_text)
        self.assertNotIn("5 MINUTES WITH: Ofer Zivony, Israel", titles)


class MembershipRewardEventTest(unittest.TestCase):
    def test_mailbox_candidates_are_fetched_and_sorted_oldest_first(self):
        messages_by_id = {
            "newer": {"id": "newer", "internalDate": "2000"},
            "older": {"id": "older", "internalDate": "1000"},
        }
        original_get_message = mailapp._get_gmail_message
        try:
            mailapp._get_gmail_message = lambda access_token, message_id: messages_by_id[message_id]
            messages = mailapp._fetch_gmail_messages_for_processing(
                "token",
                [{"id": "newer"}, {"id": "older"}],
            )
        finally:
            mailapp._get_gmail_message = original_get_message

        self.assertEqual([message["id"] for message in messages], ["older", "newer"])

    def test_renewal_parser_extracts_expiration_date_between_type_and_total(self):
        text = """
        A membership renewal has been processed for American Go Association.

        Sam Ackerman
        Member Number: 26265
        Phone: 404-555-0100
        Email: sam@example.test
        Login Name: samackerman
        Type: Tournament Pass
        Expiration Date: 7/12/2026
        Total: $10.00

        Club Url
        """

        parsed = mailapp._parse_renewal_email(text)

        self.assertEqual(parsed["AGAID"], 26265)
        self.assertEqual(parsed["MemberType"], "Tournament Pass")
        self.assertEqual(parsed["ExpirationDate"], date(2026, 7, 12))
        self.assertFalse(parsed["IsChapterMember"])

    def test_tournament_pass_default_expiration_is_thirty_day_window(self):
        self.assertEqual(
            mailapp._default_membership_expiration_date(date(2026, 6, 13), "Tournament Pass"),
            date(2026, 7, 12),
        )

    def test_membership_reward_event_params_preserve_source_context(self):
        received_at = datetime(2026, 5, 2, 15, 30, tzinfo=timezone.utc)

        params = mailapp._membership_reward_event_params(
            {"id": "msg-123"},
            received_at,
            mailapp.REWARDS_RENEWAL_EVENT_TYPE,
            date(2026, 5, 2),
            {
                "AGAID": 12345,
                "MemberType": "Adult Full",
                "IsChapterMember": False,
            },
            sender="ClubExpress <scheduler@example.test>",
            subject="American Go Association - Member Renewal",
            blob_path="member_renewal/2026/05/02/msg-123",
        )

        self.assertEqual(params["MessageId"], "msg-123")
        self.assertEqual(params["AGAID"], 12345)
        self.assertEqual(params["EventType"], "renewal")
        self.assertEqual(params["EventDate"], date(2026, 5, 2))
        self.assertEqual(params["MemberType"], "Adult Full")

        payload = json.loads(params["SourcePayloadJson"])
        self.assertEqual(payload["message_id"], "msg-123")
        self.assertEqual(payload["subject"], "American Go Association - Member Renewal")
        self.assertEqual(payload["parsed"]["MemberType"], "Adult Full")
        self.assertFalse(payload["parsed"]["IsChapterMember"])

    def test_stored_procedure_call_orders_params(self):
        sql, values = mailapp._stored_procedure_call(
            "rewards.sp_record_membership_event",
            {"MessageId": "msg-123", "AGAID": 12345},
        )

        self.assertEqual(
            sql,
            "EXEC rewards.sp_record_membership_event @MessageId = ?, @AGAID = ?",
        )
        self.assertEqual(values, ["msg-123", 12345])

    def test_new_member_staged_consumption_skips_direct_downstream_writes(self):
        text = """
        Thank you for purchasing a membership in American Go Association.

        Riley Chen
        Member Number: 34567
        Email: riley@example.test
        Login Name: rileychen
        Type: Adult Full
        New Member Expiration Date: 2027-06-26
        Total: $50.00

        Club Url
        """
        encoded_body = base64.urlsafe_b64encode(text.encode("utf-8")).decode("ascii").rstrip("=")
        message = {
            "id": "new-member-msg",
            "internalDate": "1782518400000",
            "payload": {
                "mimeType": "text/plain",
                "headers": [
                    {"name": "From", "value": "ClubExpress <notifications@example.test>"},
                    {"name": "Subject", "value": "American Go Association - New Member Signup - Payment"},
                ],
                "body": {"data": encoded_body},
            },
        }

        original_env = {
            "CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED": os.environ.get("CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED"),
            "CLUBEXPRESS_STAGED_NEW_MEMBER_CONSUMPTION_ENABLED": os.environ.get("CLUBEXPRESS_STAGED_NEW_MEMBER_CONSUMPTION_ENABLED"),
        }
        originals = {
            "_get_sql_connection_string": mailapp._get_sql_connection_string,
            "_archive_message_artifacts": mailapp._archive_message_artifacts,
            "_record_clubexpress_parsed_event": mailapp._record_clubexpress_parsed_event,
            "_execute_stored_procedures": mailapp._execute_stored_procedures,
            "_mark_clubexpress_parsed_event_processed": mailapp._mark_clubexpress_parsed_event_processed,
            "_mark_gmail_message_processed": mailapp._mark_gmail_message_processed,
        }
        staged_events = []
        direct_calls = []
        processed_marks = []
        gmail_marks = []
        try:
            os.environ["CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED"] = "true"
            os.environ["CLUBEXPRESS_STAGED_NEW_MEMBER_CONSUMPTION_ENABLED"] = "true"
            mailapp._get_sql_connection_string = lambda: "conn"
            mailapp._archive_message_artifacts = lambda message_type, message, attachments: "new_member_signup/2026/06/26/new-member-msg"
            mailapp._record_clubexpress_parsed_event = lambda conn_str, parsed_event: staged_events.append(parsed_event)
            mailapp._execute_stored_procedures = lambda conn_str, procedures: direct_calls.append((conn_str, procedures))
            mailapp._mark_clubexpress_parsed_event_processed = lambda conn_str, parsed_event, payload: processed_marks.append((parsed_event, payload))
            mailapp._mark_gmail_message_processed = lambda access_token, message: gmail_marks.append(message["id"])

            mailapp._process_mailbox_message("token", message)
        finally:
            for name, value in original_env.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            for name, value in originals.items():
                setattr(mailapp, name, value)

        self.assertEqual(len(staged_events), 1)
        self.assertEqual(staged_events[0].event_type, mailapp.REWARDS_NEW_MEMBERSHIP_EVENT_TYPE)
        self.assertEqual(staged_events[0].agaid, 34567)
        self.assertEqual(staged_events[0].event_key, "new-member-msg:new_membership:34567")
        self.assertEqual(direct_calls, [])
        self.assertEqual(processed_marks, [])
        self.assertEqual(gmail_marks, ["new-member-msg"])

    def test_renewal_staged_consumption_skips_direct_downstream_writes(self):
        text = """
        A membership renewal has been processed for American Go Association.

        Austin Go Club
        Member Number: 13529
        Phone:
        Email: aust@example.test
        Login Name: aust
        Type: Chapter
        Expiration Date: 6/13/2027
        Total: $35.00

        Club Url
        """
        encoded_body = base64.urlsafe_b64encode(text.encode("utf-8")).decode("ascii").rstrip("=")
        message = {
            "id": "renewal-msg",
            "internalDate": "1782518400000",
            "payload": {
                "mimeType": "text/plain",
                "headers": [
                    {"name": "From", "value": "ClubExpress <notifications@example.test>"},
                    {"name": "Subject", "value": "American Go Association - Member Renewal"},
                ],
                "body": {"data": encoded_body},
            },
        }

        original_env = {
            "CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED": os.environ.get("CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED"),
            "CLUBEXPRESS_STAGED_RENEWAL_CONSUMPTION_ENABLED": os.environ.get("CLUBEXPRESS_STAGED_RENEWAL_CONSUMPTION_ENABLED"),
        }
        originals = {
            "_get_sql_connection_string": mailapp._get_sql_connection_string,
            "_archive_message_artifacts": mailapp._archive_message_artifacts,
            "_record_clubexpress_parsed_event": mailapp._record_clubexpress_parsed_event,
            "_execute_stored_procedures": mailapp._execute_stored_procedures,
            "_execute_stored_procedure_rows": mailapp._execute_stored_procedure_rows,
            "_mark_clubexpress_parsed_event_processed": mailapp._mark_clubexpress_parsed_event_processed,
            "_mark_gmail_message_processed": mailapp._mark_gmail_message_processed,
        }
        staged_events = []
        direct_calls = []
        row_calls = []
        processed_marks = []
        gmail_marks = []
        try:
            os.environ["CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED"] = "true"
            os.environ["CLUBEXPRESS_STAGED_RENEWAL_CONSUMPTION_ENABLED"] = "true"
            mailapp._get_sql_connection_string = lambda: "conn"
            mailapp._archive_message_artifacts = lambda message_type, message, attachments: "member_renewal/2026/06/26/renewal-msg"
            mailapp._record_clubexpress_parsed_event = lambda conn_str, parsed_event: staged_events.append(parsed_event)
            mailapp._execute_stored_procedures = lambda conn_str, procedures: direct_calls.append((conn_str, procedures))
            mailapp._execute_stored_procedure_rows = lambda conn_str, name, params: row_calls.append((name, params)) or []
            mailapp._mark_clubexpress_parsed_event_processed = lambda conn_str, parsed_event, payload: processed_marks.append((parsed_event, payload))
            mailapp._mark_gmail_message_processed = lambda access_token, message: gmail_marks.append(message["id"])

            mailapp._process_mailbox_message("token", message)
        finally:
            for name, value in original_env.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            for name, value in originals.items():
                setattr(mailapp, name, value)

        self.assertEqual(len(staged_events), 1)
        self.assertEqual(staged_events[0].event_type, mailapp.REWARDS_RENEWAL_EVENT_TYPE)
        self.assertEqual(staged_events[0].agaid, 13529)
        self.assertEqual(staged_events[0].event_key, "renewal-msg:renewal:13529")
        downstream_payload = json.loads(staged_events[0].downstream_payload_json)
        self.assertEqual(
            [procedure["name"] for procedure in downstream_payload["procedures"]],
            [
                "membership.sp_process_membership_renewal",
                mailapp.REWARDS_MEMBERSHIP_EVENT_PROC,
                mailapp.REWARDS_CHAPTER_RENEWAL_CONFIRMATION_PROC,
            ],
        )
        self.assertEqual(direct_calls, [])
        self.assertEqual(row_calls, [])
        self.assertEqual(processed_marks, [])
        self.assertEqual(gmail_marks, ["renewal-msg"])

    def test_memchap_staged_consumption_skips_direct_import(self):
        message = {
            "id": "memchap-msg",
            "internalDate": "1782518400000",
            "payload": {
                "headers": [
                    {"name": "From", "value": "ClubExpress <notifications@example.test>"},
                    {"name": "Subject", "value": "ClubExpress MemChap Report"},
                ],
            },
        }
        attachments = [
            {
                "name": "Daily MemChap.csv",
                "mimeType": "text/csv",
                "contentBytes": b"not parsed in this test",
            }
        ]

        original_env = {
            "CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED": os.environ.get("CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED"),
            "CLUBEXPRESS_STAGED_MEMCHAP_CONSUMPTION_ENABLED": os.environ.get("CLUBEXPRESS_STAGED_MEMCHAP_CONSUMPTION_ENABLED"),
        }
        originals = {
            "_get_sql_connection_string": mailapp._get_sql_connection_string,
            "_extract_gmail_attachments": mailapp._extract_gmail_attachments,
            "_archive_message_artifacts": mailapp._archive_message_artifacts,
            "_parse_csv_rows": mailapp._parse_csv_rows,
            "_record_clubexpress_parsed_event": mailapp._record_clubexpress_parsed_event,
            "_execute_stored_procedure": mailapp._execute_stored_procedure,
            "_handle_memchap_email": mailapp._handle_memchap_email,
            "_mark_gmail_message_processed": mailapp._mark_gmail_message_processed,
        }
        staged_events = []
        email_log_statuses = []
        direct_imports = []
        gmail_marks = []
        try:
            os.environ["CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED"] = "true"
            os.environ["CLUBEXPRESS_STAGED_MEMCHAP_CONSUMPTION_ENABLED"] = "true"
            mailapp._get_sql_connection_string = lambda: "conn"
            mailapp._extract_gmail_attachments = lambda access_token, msg: attachments
            mailapp._archive_message_artifacts = lambda message_type, msg, attachment_list: "nightly_memchap_csv/2026/06/26/memchap-msg"
            mailapp._parse_csv_rows = lambda csv_bytes: [("row-1",), ("row-2",)]
            mailapp._record_clubexpress_parsed_event = lambda conn_str, parsed_event: staged_events.append(parsed_event)
            mailapp._execute_stored_procedure = lambda conn_str, proc_name, params: email_log_statuses.append(params.get("Status"))
            mailapp._handle_memchap_email = lambda conn_str, attachment_list: direct_imports.append(attachment_list) or 2
            mailapp._mark_gmail_message_processed = lambda access_token, msg: gmail_marks.append(msg["id"])

            mailapp._process_mailbox_message("token", message)
        finally:
            for name, value in original_env.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            for name, value in originals.items():
                setattr(mailapp, name, value)

        self.assertEqual(email_log_statuses, ["received", "staged"])
        self.assertEqual(len(staged_events), 1)
        self.assertEqual(staged_events[0].event_type, mailapp.NIGHTLY_MESSAGE_TYPE)
        self.assertEqual(staged_events[0].parsed_item_count, 2)
        downstream_payload = json.loads(staged_events[0].downstream_payload_json)
        self.assertEqual(downstream_payload["actions"][0]["name"], mailapp.MEMCHAP_IMPORT_ACTION)
        self.assertEqual(direct_imports, [])
        self.assertEqual(gmail_marks, ["memchap-msg"])

    def test_chapter_staged_consumption_skips_direct_import(self):
        message = {
            "id": "chapter-msg",
            "internalDate": "1782518400000",
            "payload": {
                "headers": [
                    {"name": "From", "value": "ClubExpress <notifications@example.test>"},
                    {"name": "Subject", "value": "ClubExpress ChapterX Report"},
                ],
            },
        }
        attachments = [
            {
                "name": "Immediate_Chapterx.csv",
                "mimeType": "text/csv",
                "contentBytes": b"not parsed in this test",
            }
        ]

        original_env = {
            "CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED": os.environ.get("CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED"),
            "CLUBEXPRESS_STAGED_CHAPTER_CONSUMPTION_ENABLED": os.environ.get("CLUBEXPRESS_STAGED_CHAPTER_CONSUMPTION_ENABLED"),
        }
        originals = {
            "_get_sql_connection_string": mailapp._get_sql_connection_string,
            "_extract_gmail_attachments": mailapp._extract_gmail_attachments,
            "_archive_message_artifacts": mailapp._archive_message_artifacts,
            "_parse_chapter_rows": mailapp._parse_chapter_rows,
            "_record_clubexpress_parsed_event": mailapp._record_clubexpress_parsed_event,
            "_execute_stored_procedure": mailapp._execute_stored_procedure,
            "_handle_chapter_email": mailapp._handle_chapter_email,
            "_mark_gmail_message_processed": mailapp._mark_gmail_message_processed,
        }
        staged_events = []
        email_log_statuses = []
        direct_imports = []
        gmail_marks = []
        try:
            os.environ["CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED"] = "true"
            os.environ["CLUBEXPRESS_STAGED_CHAPTER_CONSUMPTION_ENABLED"] = "true"
            mailapp._get_sql_connection_string = lambda: "conn"
            mailapp._extract_gmail_attachments = lambda access_token, msg: attachments
            mailapp._archive_message_artifacts = lambda message_type, msg, attachment_list: "chapter_csv/2026/06/26/chapter-msg"
            mailapp._parse_chapter_rows = lambda csv_bytes: [("row-1",), ("row-2",), ("row-3",)]
            mailapp._record_clubexpress_parsed_event = lambda conn_str, parsed_event: staged_events.append(parsed_event)
            mailapp._execute_stored_procedure = lambda conn_str, proc_name, params: email_log_statuses.append(params.get("Status"))
            mailapp._handle_chapter_email = lambda conn_str, attachment_list: direct_imports.append(attachment_list) or 3
            mailapp._mark_gmail_message_processed = lambda access_token, msg: gmail_marks.append(msg["id"])

            mailapp._process_mailbox_message("token", message)
        finally:
            for name, value in original_env.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            for name, value in originals.items():
                setattr(mailapp, name, value)

        self.assertEqual(email_log_statuses, ["received", "staged"])
        self.assertEqual(len(staged_events), 1)
        self.assertEqual(staged_events[0].event_type, mailapp.CHAPTER_MESSAGE_TYPE)
        self.assertEqual(staged_events[0].parsed_item_count, 3)
        downstream_payload = json.loads(staged_events[0].downstream_payload_json)
        self.assertEqual(downstream_payload["actions"][0]["name"], mailapp.CHAPTER_IMPORT_ACTION)
        self.assertEqual(direct_imports, [])
        self.assertEqual(gmail_marks, ["chapter-msg"])

    def test_member_categories_staged_consumption_skips_direct_import(self):
        message = {
            "id": "category-msg",
            "internalDate": "1782518400000",
            "payload": {
                "headers": [
                    {"name": "From", "value": "ClubExpress <notifications@example.test>"},
                    {"name": "Subject", "value": "ClubExpress Member Categories Report"},
                ],
            },
        }
        attachments = [
            {
                "name": "Immediate_MemberCategories.csv",
                "mimeType": "text/csv",
                "contentBytes": b"AGAID,Category\r\n12345,Adult\r\n23456,Youth\r\n",
            }
        ]

        original_env = {
            "CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED": os.environ.get("CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED"),
            "CLUBEXPRESS_STAGED_MEMBER_CATEGORIES_CONSUMPTION_ENABLED": os.environ.get("CLUBEXPRESS_STAGED_MEMBER_CATEGORIES_CONSUMPTION_ENABLED"),
        }
        originals = {
            "_get_sql_connection_string": mailapp._get_sql_connection_string,
            "_extract_gmail_attachments": mailapp._extract_gmail_attachments,
            "_archive_message_artifacts": mailapp._archive_message_artifacts,
            "_parse_member_category_rows": mailapp._parse_member_category_rows,
            "_record_clubexpress_parsed_event": mailapp._record_clubexpress_parsed_event,
            "_execute_stored_procedure": mailapp._execute_stored_procedure,
            "_handle_member_categories_email": mailapp._handle_member_categories_email,
            "_mark_gmail_message_processed": mailapp._mark_gmail_message_processed,
        }
        staged_events = []
        email_log_statuses = []
        direct_imports = []
        gmail_marks = []
        try:
            os.environ["CLUBEXPRESS_PARSED_EVENT_STAGING_ENABLED"] = "true"
            os.environ["CLUBEXPRESS_STAGED_MEMBER_CATEGORIES_CONSUMPTION_ENABLED"] = "true"
            mailapp._get_sql_connection_string = lambda: "conn"
            mailapp._extract_gmail_attachments = lambda access_token, msg: attachments
            mailapp._archive_message_artifacts = lambda message_type, msg, attachment_list: "nightly_member_categories_csv/2026/06/26/category-msg"
            mailapp._parse_member_category_rows = lambda csv_bytes: [(12345, "Adult"), (23456, "Youth")]
            mailapp._record_clubexpress_parsed_event = lambda conn_str, parsed_event: staged_events.append(parsed_event)
            mailapp._execute_stored_procedure = lambda conn_str, proc_name, params: email_log_statuses.append(params.get("Status"))
            mailapp._handle_member_categories_email = lambda conn_str, attachment_list: direct_imports.append(attachment_list) or 2
            mailapp._mark_gmail_message_processed = lambda access_token, msg: gmail_marks.append(msg["id"])

            mailapp._process_mailbox_message("token", message)
        finally:
            for name, value in original_env.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            for name, value in originals.items():
                setattr(mailapp, name, value)

        self.assertEqual(email_log_statuses, ["received", "staged"])
        self.assertEqual(len(staged_events), 1)
        self.assertEqual(staged_events[0].event_type, mailapp.NIGHTLY_CATEGORY_MESSAGE_TYPE)
        self.assertEqual(staged_events[0].parsed_item_count, 2)
        downstream_payload = json.loads(staged_events[0].downstream_payload_json)
        self.assertEqual(downstream_payload["actions"][0]["name"], mailapp.MEMBER_CATEGORIES_IMPORT_ACTION)
        self.assertEqual(direct_imports, [])
        self.assertEqual(gmail_marks, ["category-msg"])


class RewardsSnapshotTest(unittest.TestCase):
    def test_rewards_snapshot_params_do_not_replace_existing_snapshots(self):
        params = mailapp._rewards_snapshot_params(date(2026, 5, 2))

        self.assertEqual(
            params,
            {
                "SnapshotDate": date(2026, 5, 2),
                "RunType": "daily",
                "ReplaceExisting": 0,
            },
        )


class RewardsMembershipAwardsTest(unittest.TestCase):
    def test_rewards_membership_awards_params_write_daily_run(self):
        params = mailapp._rewards_membership_awards_params(date(2026, 5, 2))

        self.assertEqual(
            params,
            {
                "AsOfDate": date(2026, 5, 2),
                "RunType": "daily",
                "DryRun": 0,
            },
        )


class RewardsRatedGameAwardsTest(unittest.TestCase):
    def test_rewards_rated_game_awards_params_write_daily_run_on_ledger_start(self):
        params = mailapp._rewards_rated_game_awards_params(date(2026, 5, 2))

        self.assertEqual(
            params,
            {
                "GameDateFrom": date(2026, 5, 2),
                "GameDateTo": date(2026, 5, 2),
                "RunType": "daily",
                "DryRun": 0,
            },
        )

    def test_rewards_rated_game_awards_params_scan_from_ledger_start(self):
        params = mailapp._rewards_rated_game_awards_params(date(2026, 5, 21))

        self.assertEqual(
            params,
            {
                "GameDateFrom": date(2026, 5, 2),
                "GameDateTo": date(2026, 5, 21),
                "RunType": "daily",
                "DryRun": 0,
            },
        )

    def test_rewards_rated_game_awards_params_allow_configured_start_date(self):
        original = os.environ.get("REWARDS_RATED_GAME_AWARDS_DATE_FROM")
        os.environ["REWARDS_RATED_GAME_AWARDS_DATE_FROM"] = "2026-05-16"
        try:
            params = mailapp._rewards_rated_game_awards_params(date(2026, 5, 21))
        finally:
            if original is None:
                os.environ.pop("REWARDS_RATED_GAME_AWARDS_DATE_FROM", None)
            else:
                os.environ["REWARDS_RATED_GAME_AWARDS_DATE_FROM"] = original

        self.assertEqual(
            params,
            {
                "GameDateFrom": date(2026, 5, 16),
                "GameDateTo": date(2026, 5, 21),
                "RunType": "daily",
                "DryRun": 0,
            },
        )


class RewardsTournamentAwardsTest(unittest.TestCase):
    def test_rewards_tournament_awards_params_scan_through_daily_date(self):
        params = mailapp._rewards_tournament_awards_params(date(2026, 5, 3))

        self.assertEqual(
            params,
            {
                "TournamentDateFrom": None,
                "TournamentDateTo": date(2026, 5, 3),
                "RunType": "daily",
                "DryRun": 0,
            },
        )


class RewardsPointExpirationsTest(unittest.TestCase):
    def test_rewards_point_expirations_params_write_daily_run(self):
        params = mailapp._rewards_point_expirations_params(date(2028, 5, 3))

        self.assertEqual(
            params,
            {
                "AsOfDate": date(2028, 5, 3),
                "RunType": "daily",
                "DryRun": 0,
            },
        )


class ChapterRenewalNoticeTest(unittest.TestCase):
    def test_membership_renewal_emails_subject_is_classified(self):
        self.assertEqual(
            mailapp._classify_message("ClubExpress <scheduler@mail2.clubexpress.com>", "Membership Renewal Emails", []),
            mailapp.CHAPTER_RENEWAL_NOTICE_MESSAGE_TYPE,
        )

    def test_parse_chapter_renewal_notice_html_table_extracts_chapters_only(self):
        html_body = """
        <html><body>
          <table>
            <tr><th>Member</th><th>Name</th><th>Type</th><th>Expiration</th></tr>
            <tr><td>13529</td><td>Providence Go Club</td><td>Chapter</td><td>5/31/2026</td></tr>
            <tr><td>12345</td><td>Jane Player</td><td>Adult Full</td><td>5/31/2026</td></tr>
            <tr><td>25495 - Ghost City Go</td><td>Ghost City Go</td><td>Chapter</td><td>6/1/2026</td></tr>
          </table>
        </body></html>
        """

        rows = mailapp._extract_chapter_renewal_notice_rows_from_html(html_body)

        self.assertEqual([row["chapter_id"] for row in rows], [13529, 25495])
        self.assertEqual(rows[0]["member_type"], "Chapter")
        self.assertEqual(rows[0]["row_payload"]["name"], "Providence Go Club")
        self.assertEqual(rows[1]["member_raw"], "25495 - Ghost City Go")

    def test_chapter_renewal_notice_params_include_points_and_payload(self):
        received_at = datetime(2026, 5, 5, 1, 15, tzinfo=timezone.utc)
        parsed_rows = [
            {
                "source_row_number": 2,
                "chapter_id": 13529,
                "member_raw": "13529",
                "member_type": "Chapter",
                "row_payload": {"member": "13529", "type": "Chapter"},
            }
        ]

        params = mailapp._chapter_renewal_notice_params(
            {"id": "msg-renewals"},
            received_at,
            date(2026, 5, 5),
            parsed_rows,
            sender="ClubExpress <scheduler@example.test>",
            subject="Membership Renewal Emails",
            blob_path="chapter_renewal_notice/2026/05/05/msg-renewals",
        )

        self.assertEqual(params["MessageId"], "msg-renewals")
        self.assertEqual(params["PointsPerRenewal"], 35000)
        notices = json.loads(params["NoticesJson"])
        self.assertEqual(notices[0]["chapter_id"], 13529)
        self.assertEqual(notices[0]["source_payload"]["subject"], "Membership Renewal Emails")

    def test_chapter_renewal_confirmation_params_use_chapter_id(self):
        received_at = datetime(2026, 5, 5, 14, 30, tzinfo=timezone.utc)
        parsed = {
            "AGAID": 14182,
            "MemberType": "Chapter",
            "IsChapterMember": True,
            "EmailAddress": "chapter@example.test",
        }

        params = mailapp._chapter_renewal_confirmation_params(
            {"id": "msg-confirm"},
            received_at,
            parsed,
            sender="ClubExpress <scheduler@example.test>",
            subject="American Go Association - Member Renewal",
            blob_path="member_renewal/2026/05/05/msg-confirm",
        )

        self.assertEqual(params["MessageId"], "msg-confirm")
        self.assertEqual(params["ChapterID"], 14182)
        self.assertEqual(params["MemberType"], "Chapter")
        payload = json.loads(params["SourcePayloadJson"])
        self.assertEqual(payload["parsed"]["IsChapterMember"], True)
        self.assertEqual(payload["blob_path"], "member_renewal/2026/05/05/msg-confirm")

    def test_pending_chapter_renewals_email_body_lists_debited_chapters(self):
        body = mailapp._pending_chapter_renewals_email_body(
            [
                {
                    "ChapterID": 14182,
                    "Chapter_Code": "SHPO",
                    "Chapter_Name": "Shreveport-Bossier Go Club",
                    "Notice_Date": date(2026, 5, 4),
                    "Points_Required": 35000,
                    "Pending_Days": 2,
                    "TransactionID": 1269,
                }
            ],
            date(2026, 5, 6),
        )

        self.assertIn("Pending debited chapters: 1", body)
        self.assertIn("SHPO Shreveport-Bossier Go Club (14182)", body)
        self.assertIn("txn 1269", body)

    def test_pending_chapter_renewals_email_body_handles_empty_list(self):
        body = mailapp._pending_chapter_renewals_email_body([], date(2026, 5, 6))

        self.assertIn("Pending debited chapters: 0", body)
        self.assertIn("No chapters are currently debited", body)


class ChapterCsvImportTest(unittest.TestCase):
    def test_chapterx_filename_is_classified_as_chapter_csv(self):
        report_type = mailapp._detect_attachment_report_type(
            "Immediate_Chapterx.csv",
            b"ID,Name,Short Name,City,State,Status\r\n32477,Test Club,TST,Seattle,WA,Active\r\n",
        )

        self.assertEqual(report_type, mailapp.CHAPTER_MESSAGE_TYPE)

    def test_chapter_rows_accept_extra_clubexpress_columns_and_aliases(self):
        rows = mailapp._parse_chapter_rows(
            b"Report generated,ignored\r\n"
            b"ID,Name,Short Name,City,State,Primary Contact Member ID,Date Created,Status,Extra\r\n"
            b"32477,Test Go Club,TST,Seattle,WA,32478,4/17/2026,Active,ignored\r\n"
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 32477)
        self.assertEqual(rows[0][1], "TST")
        self.assertEqual(rows[0][2], "Test Go Club")
        self.assertEqual(rows[0][5], 32478)
        self.assertEqual(rows[0][7], "Active")

    def test_chapter_rows_reject_duplicate_chapter_ids(self):
        with self.assertRaisesRegex(mailapp.CsvValidationError, "Duplicate ChapterID"):
            mailapp._parse_chapter_rows(
                b"ChapterID,ChapterName,ChapterCode\r\n"
                b"1,One,ONE\r\n"
                b"1,Duplicate,DUP\r\n"
            )

    def test_chapter_rows_reject_missing_required_headers(self):
        with self.assertRaisesRegex(mailapp.CsvValidationError, "ChapterID, ChapterCode, ChapterName"):
            mailapp._parse_chapter_rows(
                b"ChapterID,ChapterName,City,State\r\n"
                b"1,One,Seattle,WA\r\n"
            )

    def test_chapter_rows_reject_blank_required_values(self):
        with self.assertRaisesRegex(mailapp.CsvValidationError, "Column ChapterCode is required"):
            mailapp._parse_chapter_rows(
                b"ChapterID,ChapterName,ChapterCode\r\n"
                b"1,One,\r\n"
            )


class MemChapCsvImportTest(unittest.TestCase):
    def test_memchap_rows_accept_active_header_as_mislabeled_agaid(self):
        header = ["Active", *mailapp.STAGING_COLUMNS[1:], "Member.DateCreated"]
        values = {column: "" for column in mailapp.STAGING_COLUMNS}
        values.update(
            {
                "AGAID": "12345",
                "MemberType": "Adult Full",
                "FirstName": "Test",
                "LastName": "Member",
                "Status": "Active",
                "EmailAddress": "test@example.test",
                "ChapterID": "32292",
            }
        )
        row = [values["AGAID"], *[values[column] for column in mailapp.STAGING_COLUMNS[1:]], "ignored"]
        csv_bytes = (",".join(header) + "\r\n" + ",".join(row) + "\r\n").encode("utf-8")

        rows = mailapp._parse_csv_rows(csv_bytes)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 12345)
        self.assertEqual(rows[0][1], "Adult Full")
        self.assertEqual(rows[0][mailapp.STAGING_COLUMNS.index("ChapterID")], 32292)

    def test_process_pending_memchap_events_imports_archived_attachment(self):
        prefix = "nightly_memchap_csv/2026/06/26/memchap-msg"
        row = {
            "Parsed_Event_ID": 21,
            "Message_ID": "memchap-msg",
            "Event_Key": "memchap-msg:nightly_memchap_csv",
            "Message_Type": mailapp.NIGHTLY_MESSAGE_TYPE,
            "Event_Type": mailapp.NIGHTLY_MESSAGE_TYPE,
            "Received_At": datetime(2026, 6, 26, 23, 0, tzinfo=timezone.utc),
            "Event_Date": date(2026, 6, 26),
            "AGAID": None,
            "ChapterID": None,
            "Parsed_Item_Count": 2,
            "Sender": "ClubExpress <notifications@example.test>",
            "Subject": "ClubExpress MemChap Report",
            "Blob_Path": prefix,
            "Parsed_Payload_Json": json.dumps(
                {
                    "message_id": "memchap-msg",
                    "blob_path": prefix,
                    "parsed": {
                        "attachment_name": "Daily MemChap.csv",
                        "attachment_blob_name": "Daily_MemChap.csv",
                        "row_count": 2,
                    },
                }
            ),
            "Downstream_Payload_Json": json.dumps(
                {
                    "actions": [
                        {
                            "name": mailapp.MEMCHAP_IMPORT_ACTION,
                            "params": {"blob_path": prefix, "attachment_blob_name": "Daily_MemChap.csv"},
                        }
                    ]
                }
            ),
            "Status": "staged",
            "Attempt_Count": 0,
            "Last_Processed_At": None,
            "Last_Error_Message": None,
            "Created_At": datetime(2026, 6, 26, 23, 1, tzinfo=timezone.utc),
            "Updated_At": datetime(2026, 6, 26, 23, 1, tzinfo=timezone.utc),
        }
        adapter = _FakeStagedAdapter([row])
        originals = {
            "_download_archived_attachment_bytes": mailapp._download_archived_attachment_bytes,
            "_import_memchap_bytes": mailapp._import_memchap_bytes,
            "_execute_stored_procedure": mailapp._execute_stored_procedure,
        }
        downloads = []
        imports = []
        email_logs = []
        try:
            mailapp._download_archived_attachment_bytes = (
                lambda blob_path, attachment_blob_name: downloads.append((blob_path, attachment_blob_name)) or b"csv"
            )
            mailapp._import_memchap_bytes = lambda conn_str, csv_bytes: imports.append((conn_str, csv_bytes)) or 2
            mailapp._execute_stored_procedure = lambda conn_str, proc_name, params: email_logs.append((proc_name, params))

            result = mailapp._process_pending_memchap_events(
                "conn",
                top=5,
                execute=True,
                confirm_replay=True,
                processor_name="test_memchap_processor",
                adapter=adapter,
            )
        finally:
            for name, value in originals.items():
                setattr(mailapp, name, value)

        self.assertTrue(result.executed)
        self.assertEqual(result.selected_count, 1)
        self.assertEqual(result.processed_count, 1)
        self.assertEqual(result.error_count, 0)
        self.assertEqual(downloads, [(prefix, "Daily_MemChap.csv")])
        self.assertEqual(imports, [("conn", b"csv")])
        self.assertEqual([batch[0][1][1] for batch in adapter.executed], ["processing", "processed"])
        self.assertEqual(email_logs[0][0], "membership.sp_log_clubexpress_email")
        self.assertEqual(email_logs[0][1]["MessageId"], "memchap-msg")
        self.assertEqual(email_logs[0][1]["Status"], "processed")
        self.assertIsNone(email_logs[0][1]["ErrorMessage"])
        self.assertEqual(result.results[0]["status_after"], "processed")

    def test_process_pending_chapter_events_imports_archived_attachment(self):
        prefix = "chapter_csv/2026/06/26/chapter-msg"
        row = {
            "Parsed_Event_ID": 22,
            "Message_ID": "chapter-msg",
            "Event_Key": "chapter-msg:chapter_csv",
            "Message_Type": mailapp.CHAPTER_MESSAGE_TYPE,
            "Event_Type": mailapp.CHAPTER_MESSAGE_TYPE,
            "Received_At": datetime(2026, 6, 26, 23, 0, tzinfo=timezone.utc),
            "Event_Date": date(2026, 6, 26),
            "AGAID": None,
            "ChapterID": None,
            "Parsed_Item_Count": 3,
            "Sender": "ClubExpress <notifications@example.test>",
            "Subject": "ClubExpress ChapterX Report",
            "Blob_Path": prefix,
            "Parsed_Payload_Json": json.dumps(
                {
                    "message_id": "chapter-msg",
                    "blob_path": prefix,
                    "parsed": {
                        "attachment_name": "Immediate_Chapterx.csv",
                        "attachment_blob_name": "Immediate_Chapterx.csv",
                        "row_count": 3,
                    },
                }
            ),
            "Downstream_Payload_Json": json.dumps(
                {
                    "actions": [
                        {
                            "name": mailapp.CHAPTER_IMPORT_ACTION,
                            "params": {"blob_path": prefix, "attachment_blob_name": "Immediate_Chapterx.csv"},
                        }
                    ]
                }
            ),
            "Status": "staged",
            "Attempt_Count": 0,
            "Last_Processed_At": None,
            "Last_Error_Message": None,
            "Created_At": datetime(2026, 6, 26, 23, 1, tzinfo=timezone.utc),
            "Updated_At": datetime(2026, 6, 26, 23, 1, tzinfo=timezone.utc),
        }
        adapter = _FakeStagedAdapter([row])
        originals = {
            "_download_archived_attachment_bytes": mailapp._download_archived_attachment_bytes,
            "_import_chapter_bytes": mailapp._import_chapter_bytes,
            "_execute_stored_procedure": mailapp._execute_stored_procedure,
        }
        downloads = []
        imports = []
        email_logs = []
        try:
            mailapp._download_archived_attachment_bytes = (
                lambda blob_path, attachment_blob_name: downloads.append((blob_path, attachment_blob_name)) or b"csv"
            )
            mailapp._import_chapter_bytes = lambda conn_str, csv_bytes: imports.append((conn_str, csv_bytes)) or 3
            mailapp._execute_stored_procedure = lambda conn_str, proc_name, params: email_logs.append((proc_name, params))

            result = mailapp._process_pending_chapter_events(
                "conn",
                top=5,
                execute=True,
                confirm_replay=True,
                processor_name="test_chapter_processor",
                adapter=adapter,
            )
        finally:
            for name, value in originals.items():
                setattr(mailapp, name, value)

        self.assertTrue(result.executed)
        self.assertEqual(result.selected_count, 1)
        self.assertEqual(result.processed_count, 1)
        self.assertEqual(result.error_count, 0)
        self.assertEqual(downloads, [(prefix, "Immediate_Chapterx.csv")])
        self.assertEqual(imports, [("conn", b"csv")])
        self.assertEqual([batch[0][1][1] for batch in adapter.executed], ["processing", "processed"])
        self.assertEqual(email_logs[0][0], "membership.sp_log_clubexpress_email")
        self.assertEqual(email_logs[0][1]["MessageId"], "chapter-msg")
        self.assertEqual(email_logs[0][1]["Status"], "processed")
        self.assertIsNone(email_logs[0][1]["ErrorMessage"])
        self.assertEqual(result.results[0]["status_after"], "processed")

    def test_process_pending_member_category_events_imports_archived_attachment(self):
        prefix = "nightly_member_categories_csv/2026/06/26/category-msg"
        row = {
            "Parsed_Event_ID": 23,
            "Message_ID": "category-msg",
            "Event_Key": "category-msg:nightly_member_categories_csv",
            "Message_Type": mailapp.NIGHTLY_CATEGORY_MESSAGE_TYPE,
            "Event_Type": mailapp.NIGHTLY_CATEGORY_MESSAGE_TYPE,
            "Received_At": datetime(2026, 6, 26, 23, 0, tzinfo=timezone.utc),
            "Event_Date": date(2026, 6, 26),
            "AGAID": None,
            "ChapterID": None,
            "Parsed_Item_Count": 2,
            "Sender": "ClubExpress <notifications@example.test>",
            "Subject": "ClubExpress Member Categories Report",
            "Blob_Path": prefix,
            "Parsed_Payload_Json": json.dumps(
                {
                    "message_id": "category-msg",
                    "blob_path": prefix,
                    "parsed": {
                        "attachment_name": "Immediate_MemberCategories.csv",
                        "attachment_blob_name": "Immediate_MemberCategories.csv",
                        "row_count": 2,
                    },
                }
            ),
            "Downstream_Payload_Json": json.dumps(
                {
                    "actions": [
                        {
                            "name": mailapp.MEMBER_CATEGORIES_IMPORT_ACTION,
                            "params": {"blob_path": prefix, "attachment_blob_name": "Immediate_MemberCategories.csv"},
                        }
                    ]
                }
            ),
            "Status": "staged",
            "Attempt_Count": 0,
            "Last_Processed_At": None,
            "Last_Error_Message": None,
            "Created_At": datetime(2026, 6, 26, 23, 1, tzinfo=timezone.utc),
            "Updated_At": datetime(2026, 6, 26, 23, 1, tzinfo=timezone.utc),
        }
        adapter = _FakeStagedAdapter([row])
        originals = {
            "_download_archived_attachment_bytes": mailapp._download_archived_attachment_bytes,
            "_import_member_categories_bytes": mailapp._import_member_categories_bytes,
            "_execute_stored_procedure": mailapp._execute_stored_procedure,
        }
        downloads = []
        imports = []
        email_logs = []
        try:
            mailapp._download_archived_attachment_bytes = (
                lambda blob_path, attachment_blob_name: downloads.append((blob_path, attachment_blob_name)) or b"csv"
            )
            mailapp._import_member_categories_bytes = lambda conn_str, csv_bytes: imports.append((conn_str, csv_bytes)) or 2
            mailapp._execute_stored_procedure = lambda conn_str, proc_name, params: email_logs.append((proc_name, params))

            result = mailapp._process_pending_member_category_events(
                "conn",
                top=5,
                execute=True,
                confirm_replay=True,
                processor_name="test_member_category_processor",
                adapter=adapter,
            )
        finally:
            for name, value in originals.items():
                setattr(mailapp, name, value)

        self.assertTrue(result.executed)
        self.assertEqual(result.selected_count, 1)
        self.assertEqual(result.processed_count, 1)
        self.assertEqual(result.error_count, 0)
        self.assertEqual(downloads, [(prefix, "Immediate_MemberCategories.csv")])
        self.assertEqual(imports, [("conn", b"csv")])
        self.assertEqual([batch[0][1][1] for batch in adapter.executed], ["processing", "processed"])
        self.assertEqual(email_logs[0][0], "membership.sp_log_clubexpress_email")
        self.assertEqual(email_logs[0][1]["MessageId"], "category-msg")
        self.assertEqual(email_logs[0][1]["Status"], "processed")
        self.assertIsNone(email_logs[0][1]["ErrorMessage"])
        self.assertEqual(result.results[0]["status_after"], "processed")


class _FakeStagedAdapter:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []
        self.executed = []

    def query_rows(self, query, params=()):
        self.queries.append((query, tuple(params)))
        return self.rows

    def execute_statements(self, statements):
        self.executed.append(list(statements))


if __name__ == "__main__":
    unittest.main()
