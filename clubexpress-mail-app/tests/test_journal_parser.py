import importlib.util
from pathlib import Path
import sys
import unittest


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


if __name__ == "__main__":
    unittest.main()
