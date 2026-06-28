import sys
import unittest
from pathlib import Path


APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from clubexpress_csv_parsers import (
    CHAPTER_MESSAGE_TYPE,
    STAGING_COLUMNS,
    CsvValidationError,
    detect_attachment_report_type,
    parse_chapter_rows,
    parse_member_category_rows,
    parse_memchap_rows,
)


class ClubExpressCsvParserTest(unittest.TestCase):
    """Represent club express csv parser test."""
    def test_chapterx_filename_is_detected_as_chapter_csv(self):
        """Verify that chapterx filename is detected as chapter csv."""
        report_type = detect_attachment_report_type(
            "Immediate_Chapterx.csv",
            b"ID,Name,Short Name,City,State,Status\r\n32477,Test Club,TST,Seattle,WA,Active\r\n",
        )

        self.assertEqual(report_type, CHAPTER_MESSAGE_TYPE)

    def test_chapter_rows_accept_extra_clubexpress_columns_and_aliases(self):
        """Verify that chapter rows accept extra clubexpress columns and aliases."""
        rows = parse_chapter_rows(
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
        """Verify that chapter rows reject duplicate chapter ids."""
        with self.assertRaisesRegex(CsvValidationError, "Duplicate ChapterID"):
            parse_chapter_rows(
                b"ChapterID,ChapterName,ChapterCode\r\n"
                b"1,One,ONE\r\n"
                b"1,Duplicate,DUP\r\n"
            )

    def test_member_category_rows_reject_duplicate_pairs(self):
        """Verify that member category rows reject duplicate pairs."""
        with self.assertRaisesRegex(CsvValidationError, "Duplicate AGAID/category pair"):
            parse_member_category_rows(
                b"AGAID,Category\r\n"
                b"12345,Youth\r\n"
                b"12345,Youth\r\n"
            )

    def test_memchap_rows_accept_active_header_as_mislabeled_agaid(self):
        """Verify that memchap rows accept active header as mislabeled agaid."""
        header = ["Active", *STAGING_COLUMNS[1:], "Member.DateCreated"]
        values = {column: "" for column in STAGING_COLUMNS}
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
        row = [values["AGAID"], *[values[column] for column in STAGING_COLUMNS[1:]], "ignored"]
        csv_bytes = (",".join(header) + "\r\n" + ",".join(row) + "\r\n").encode("utf-8")

        rows = parse_memchap_rows(csv_bytes)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 12345)
        self.assertEqual(rows[0][1], "Adult Full")
        self.assertEqual(rows[0][STAGING_COLUMNS.index("ChapterID")], 32292)


if __name__ == "__main__":
    unittest.main()
