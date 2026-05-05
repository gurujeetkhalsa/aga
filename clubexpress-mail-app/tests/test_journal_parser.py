import importlib.util
import json
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
    def test_rewards_rated_game_awards_params_write_daily_run(self):
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


if __name__ == "__main__":
    unittest.main()
