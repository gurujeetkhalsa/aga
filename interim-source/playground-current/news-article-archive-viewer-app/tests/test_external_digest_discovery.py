from __future__ import annotations

import sys
import unittest
from datetime import date, datetime
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from external_go_news_sources import source_digest_date
from news_archive_external_sync import (
    ExternalNewsSyncConfig,
    build_digest_articles,
    eastern_timestamp_text,
    item_metadata_parts,
    load_external_source_items_for_digest_date,
    upsert_external_source_items,
)


class RecordingAdapter:
    def __init__(self, responses: list[list[dict]]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, tuple]] = []

    def query_rows(self, sql: str, params: tuple = ()) -> list[dict]:
        self.calls.append((sql, params))
        return self.responses.pop(0)


def source_item(title: str, *, digest_date: str = "2026-07-11") -> dict:
    return {
        "source_id": "kansaikiin",
        "source_name": "Kansai Ki-in",
        "source_home_url": "https://kansaikiin.jp/",
        "source_language": "ja",
        "source_article_id": f"https://example.test/{title}",
        "url": f"https://example.test/{title}",
        "title": title,
        "title_en": f"English {title}",
        "category": "News",
        "category_en": "News",
        "summary": "",
        "summary_en": "Summary",
        "published_at": "2026-07-10T10:58:16+00:00",
        "digest_date": digest_date,
    }


class ExternalDigestDiscoveryTests(unittest.TestCase):
    def test_publication_timestamp_is_converted_to_eastern_date(self) -> None:
        self.assertEqual(source_digest_date("kansaikiin", "2026-07-10T10:58:16+00:00"), "2026-07-10")
        self.assertEqual(source_digest_date("kansaikiin", "2026-07-10T00:30:00+09:00"), "2026-07-09")

    def test_source_timestamp_display_is_eastern(self) -> None:
        self.assertEqual(
            eastern_timestamp_text("2026-07-10T10:58:16+00:00"),
            "July 10, 2026 6:58 AM EDT",
        )
        self.assertIn(
            "Source time (Eastern): July 10, 2026 6:58 AM EDT",
            item_metadata_parts(source_item("one")),
        )

    def test_upsert_preserves_original_digest_date_on_updates(self) -> None:
        adapter = RecordingAdapter(
            [[{"Merge_Action": "INSERT", "External_News_Source_Item_ID": 42}]]
        )
        stats = upsert_external_source_items(adapter, [source_item("one")], "run-1")
        self.assertEqual(stats["inserted"], 1)
        self.assertEqual(stats["inserted_item_ids"], [42])
        merge_sql = adapter.calls[0][0]
        self.assertNotIn("[Digest_Date] = source.[Digest_Date]", merge_sql)

    def test_load_uses_first_discovery_date(self) -> None:
        adapter = RecordingAdapter(
            [[
                {
                    "Source_ID": "kansaikiin",
                    "Source_Article_ID": "article-1",
                    "Source_Name": "Kansai Ki-in",
                    "Source_Home_Url": "https://kansaikiin.jp/",
                    "Source_Language": "ja",
                    "Canonical_Url": "https://example.test/one",
                    "Title": "one",
                    "Author": "",
                    "Category": "News",
                    "Published_Text": "",
                    "Published_At": "2026-07-10T10:58:16+00:00",
                    "Digest_Date": date(2026, 7, 11),
                    "Summary": "",
                    "Summary_Type": "none",
                    "Title_En": "English one",
                    "Category_En": "News",
                    "Summary_En": "Summary",
                    "Enrichment_Status": "translated",
                    "Enrichment_Model": "test",
                    "Image_Url": "",
                    "Content_Sha256": "abc",
                    "First_Seen_At": datetime(2026, 7, 11, 10, 35, 25),
                }
            ]]
        )
        items = load_external_source_items_for_digest_date(
            adapter,
            date(2026, 7, 11),
            ExternalNewsSyncConfig(source_ids=("kansaikiin",)),
        )
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["digest_date"], "2026-07-11")
        self.assertEqual(items[0]["discovered_at"], "2026-07-11T10:35:25+00:00")
        self.assertEqual(adapter.calls[0][1], (date(2026, 7, 11),))

    def test_builds_one_digest_for_eastern_discovery_date(self) -> None:
        items = [
            source_item("one", digest_date="2026-07-11"),
            source_item("two", digest_date="2026-07-11"),
            source_item("older", digest_date="2026-07-10"),
        ]
        articles = build_digest_articles(
            items,
            ExternalNewsSyncConfig(source_ids=("kansaikiin",)),
            digest_date=date(2026, 7, 11),
        )
        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0]["headline"], "Go News Digest: July 11, 2026")
        self.assertEqual(articles[0]["rendered_metadata"]["sourceItemCount"], 2)
        self.assertIn("first discovered on July 11, 2026 Eastern Time", articles[0]["article_html"])
        self.assertNotIn("Asian", articles[0]["article_html"])
        self.assertNotIn("English older", articles[0]["article_html"])

    def test_no_discoveries_means_no_digest(self) -> None:
        self.assertEqual(
            build_digest_articles(
                [],
                ExternalNewsSyncConfig(),
                digest_date=date(2026, 7, 11),
            ),
            [],
        )


if __name__ == "__main__":
    unittest.main()
