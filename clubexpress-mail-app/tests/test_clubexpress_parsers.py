import sys
import unittest
from datetime import date
from pathlib import Path


APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from clubexpress_parsers import (
    EmailProcessingError,
    extract_chapter_renewal_notice_rows_from_html,
    extract_chapter_renewal_notice_rows_from_text,
    parse_chapter_renewal_notice,
    parse_new_member_email,
    parse_renewal_email,
)


class ClubExpressParserTest(unittest.TestCase):
    def test_new_member_parser_extracts_member_context(self):
        text = """
        Thank you for purchasing a membership in American Go Association.

        Riley Chen
        Member Number: 34567
        Email: riley@example.test
        Login Name: rileychen
        Type: Youth
        New Member Expiration Date: 2027-06-13
        Total: $20.00

        Club Url
        """

        parsed = parse_new_member_email(text)

        self.assertEqual(parsed["AGAID"], 34567)
        self.assertEqual(parsed["FirstName"], "Riley")
        self.assertEqual(parsed["LastName"], "Chen")
        self.assertEqual(parsed["EmailAddress"], "riley@example.test")
        self.assertEqual(parsed["MemberType"], "Youth")
        self.assertEqual(parsed["ExpirationDate"], date(2027, 6, 13))

    def test_new_member_parser_rejects_chapter_member_signup(self):
        text = """
        Thank you for purchasing a membership in American Go Association.

        Austin Go Club
        Member Number: 13529
        Email: aust@example.test
        Login Name: aust
        Type: Chapter
        New Member Expiration Date: 2027-06-13
        Total: $35.00

        Club Url
        """

        with self.assertRaisesRegex(EmailProcessingError, "Chapter member signup"):
            parse_new_member_email(text)

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

        parsed = parse_renewal_email(text)

        self.assertEqual(parsed["AGAID"], 26265)
        self.assertEqual(parsed["PhoneNumber"], "404-555-0100")
        self.assertEqual(parsed["EmailAddress"], "sam@example.test")
        self.assertEqual(parsed["LoginName"], "samackerman")
        self.assertEqual(parsed["MemberType"], "Tournament Pass")
        self.assertEqual(parsed["ExpirationDate"], date(2026, 7, 12))
        self.assertFalse(parsed["IsChapterMember"])

    def test_renewal_parser_marks_chapter_member(self):
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

        parsed = parse_renewal_email(text)

        self.assertEqual(parsed["AGAID"], 13529)
        self.assertEqual(parsed["MemberType"], "Chapter")
        self.assertTrue(parsed["IsChapterMember"])

    def test_chapter_renewal_notice_html_parser_extracts_chapters_only(self):
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

        rows = extract_chapter_renewal_notice_rows_from_html(html_body)

        self.assertEqual([row["chapter_id"] for row in rows], [13529, 25495])
        self.assertEqual(rows[0]["member_type"], "Chapter")
        self.assertEqual(rows[0]["row_payload"]["name"], "Providence Go Club")
        self.assertEqual(rows[1]["member_raw"], "25495 - Ghost City Go")

    def test_chapter_renewal_notice_text_parser_accepts_pipe_table(self):
        text_body = """
        Member | Name | Type | Expiration
        13529 | Providence Go Club | Chapter | 5/31/2026
        12345 | Jane Player | Adult Full | 5/31/2026
        25495 - Ghost City Go | Ghost City Go | Chapter | 6/1/2026
        """

        rows = extract_chapter_renewal_notice_rows_from_text(text_body)

        self.assertEqual([row["chapter_id"] for row in rows], [13529, 25495])
        self.assertEqual(rows[1]["row_payload"]["expiration"], "6/1/2026")

    def test_chapter_renewal_notice_parser_falls_back_to_text(self):
        text_body = """
        Member\tName\tType\tExpiration
        13529\tProvidence Go Club\tChapter\t5/31/2026
        """

        rows = parse_chapter_renewal_notice("", text_body)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["chapter_id"], 13529)

    def test_chapter_renewal_notice_parser_rejects_duplicate_chapter(self):
        html_body = """
        <table>
          <tr><th>Member</th><th>Type</th></tr>
          <tr><td>13529</td><td>Chapter</td></tr>
          <tr><td>13529 - Duplicate</td><td>Chapter</td></tr>
        </table>
        """

        with self.assertRaisesRegex(EmailProcessingError, "Duplicate ChapterID 13529"):
            parse_chapter_renewal_notice(html_body, "")


if __name__ == "__main__":
    unittest.main()
