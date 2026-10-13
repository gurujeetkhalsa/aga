from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

# Keep this focused viewer test independent of the scheduled-sync modules and
# their local timezone-data dependency.
for module_name, config_name, sync_name in (
    ("news_archive_external_sync", "ExternalNewsSyncConfig", "sync_external_go_news_archive"),
    ("news_archive_player_context_sync", "NewsArchivePlayerContextSyncConfig", "sync_news_archive_player_context"),
    ("news_archive_public_sync", "PublicNewsSyncConfig", "sync_public_news_archive"),
):
    module = types.ModuleType(module_name)
    setattr(module, config_name, type(config_name, (), {"__init__": lambda self, **kwargs: None}))
    setattr(module, sync_name, lambda *args, **kwargs: {})
    sys.modules[module_name] = module

import function_app


class ViewerSharingTests(unittest.TestCase):
    def test_home_carousel_does_not_start_dragging_from_links(self) -> None:
        request = SimpleNamespace(params={})
        page = function_app._render_home_cards_page(
            [
                {
                    "News_Article_Snapshot_ID": 42,
                    "Headline": "Congress recap",
                    "Summary": "Highlights from the Go Congress.",
                }
            ],
            request,
        )

        self.assertIn('class="read-more"', page)
        pointerdown_script = page.split("viewport.addEventListener('pointerdown'", 1)[1]
        self.assertLess(
            pointerdown_script.index("suppressClick = false;"),
            pointerdown_script.index("if (isInteractiveTarget(event.target)) return;"),
        )
        self.assertIn("if (isInteractiveTarget(event.target)) return;", page)
        self.assertIn(
            "target.closest('a, button, input, select, textarea, [role=\"button\"], "
            "[contenteditable=\"true\"]')",
            page,
        )

    def test_open_article_renders_accessible_share_actions_and_fallbacks(self) -> None:
        page = function_app._render_article_page(
            {
                "Headline": "Congress recap",
                "Author": "AGA",
                "Active_Date": "2026-08-10",
                "Summary": "Highlights from the Go Congress.",
            },
            "<p>Article body</p>",
            [],
            article_url="https://example.test/api/news-archive?snapshotId=42",
            copy_url="https://example.test/api/news-archive/article/42",
            email_html_url="/api/news-archive/article/42/email-html?share=1",
        )

        self.assertIn('id="printArticle"', page)
        self.assertIn('id="printArticle" type="button" role="menuitem">Print</button>', page)
        self.assertNotIn('M6 9V3h12v6', page)
        self.assertNotIn('id="emailArticle"', page)
        self.assertIn('id="shareArticle"', page)
        self.assertIn('id="emailFullArticle"', page)
        self.assertNotIn('id="nativeShareArticle"', page)
        self.assertIn('id="copyArticleLink"', page)
        self.assertNotIn('id="shareInstagram"', page)
        self.assertIn('href="mailto:?subject=Congress+recap"', page)
        self.assertIn('The complete formatted article is on your clipboard.', page)
        self.assertIn("await copyHtml(payload.html || '', payload.text || '')", page)
        self.assertIn("await copyArticleLinkText()", page)
        self.assertIn(
            'const copyData = {"url": "https://example.test/api/news-archive/article/42"};',
            page,
        )
        self.assertIn("navigator.clipboard.writeText(copyData.url)", page)
        self.assertIn("facebook.com/sharer/sharer.php", page)
        self.assertIn("linkedin.com/sharing/share-offsite", page)
        self.assertIn("bsky.app/intent/compose", page)
        self.assertIn("twitter.com/intent/tweet", page)
        self.assertNotIn("await navigator.share(shareData)", page)
        self.assertIn("setMenuOpen(shareMenu.hidden)", page)
        self.assertIn("window.print()", page)

    def test_inline_share_data_cannot_end_the_script_element(self) -> None:
        page = function_app._render_article_page(
            {"Headline": "News </script><script>alert(1)</script>"},
            "<p>Article body</p>",
            [],
            article_url="https://example.test/api/news-archive?snapshotId=42",
            copy_url="https://example.test/api/news-archive/article/42",
            email_html_url="/api/news-archive/article/42/email-html?share=1",
        )

        script_data = page.split("const shareData = ", 1)[1].split(";", 1)[0]
        self.assertNotIn("</script>", script_data.lower())
        self.assertIn("\\u003c/script\\u003e", script_data)

    def test_selected_article_populates_social_card_metadata(self) -> None:
        request = SimpleNamespace(url="https://example.test/api/news-archive", params={})
        metadata = {
            "title": "Congress recap",
            "description": "Highlights from the Go Congress.",
            "url": "https://example.test/api/news-archive?snapshotId=42",
            "image": "https://example.test/api/news-archive/asset/7",
        }
        with patch.object(function_app, "_viewer_social_metadata", return_value=metadata):
            page = function_app._render_viewer_page(request)

        self.assertIn("<title>Congress recap | AGA E-Journal</title>", page)
        self.assertIn('property="og:title" content="Congress recap"', page)
        self.assertIn('property="og:image" content="https://example.test/api/news-archive/asset/7"', page)
        self.assertIn('name="twitter:card" content="summary_large_image"', page)
        self.assertIn('rel="canonical" href="https://example.test/api/news-archive?snapshotId=42"', page)

    def test_iframe_and_response_delegate_web_share(self) -> None:
        self.assertIn("iframe.allow = 'web-share'", function_app.INDEX_HTML)
        response = function_app._html_response("<p>ok</p>")
        self.assertEqual(response.headers["Permissions-Policy"], "web-share=(self)")

    def test_share_email_preserves_larger_floated_images(self) -> None:
        article_html = (
            '<p><img src="https://example.test/api/news-archive/asset/7526" '
            'style="float: right;">Body</p>'
        )
        assets = [{"News_Article_Asset_ID": 7526, "Width": 691}]

        shared = function_app._prepare_email_article_html(
            article_html,
            assets,
            image_max_width=600,
        )
        legacy = function_app._prepare_email_article_html(article_html, assets)

        self.assertIn('width="600"', shared)
        self.assertIn('max-width: 600px', shared)
        self.assertIn('width="220"', legacy)
        self.assertIn('max-width: 220px', legacy)


if __name__ == "__main__":
    unittest.main()
