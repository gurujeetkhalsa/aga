# Copyright 2026, American Go Association, All rights reserved

"""Prototype external Go/weiqi/baduk news source aggregator.

This first pass intentionally stores source metadata and short source-provided
excerpts, not full third-party article bodies.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import sys
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo


EGF_HOME_URL = "https://www.eurogofed.org/"
NIHONKIIN_NEWS_URL = "https://www.nihonkiin.or.jp/news/"
KBADUK_PRESS_URL = "https://www.kbaduk.or.kr/bbs/read/basic/press_release/"
HANGUKKIWON_REPORT_URL = "https://www.baduk.or.kr/news/report_in.asp"
AIGP_NEWS_URL = "https://aigp.org.in/pages/news.html"
CWA_HOME_URL = "https://www.weiqi.org.cn/"
CWA_API_BASE_URL = "https://wqapi.cwql.org.cn"
PGETC_NEWS_URL = "https://pandanet-igs.com/communities/euroteamchamps/news"
FEDIBERGO_HOME_URL = "https://www.fedibergo.org/"
FEED_SOURCE_CONFIGS = {
    "kansaikiin": {
        "id": "kansaikiin",
        "name": "Kansai Ki-in",
        "home_url": "https://kansaikiin.jp/",
        "feed_url": "https://kansaikiin.jp/wp/feed/",
        "language": "ja",
    },
    "britgo": {
        "id": "britgo",
        "name": "British Go Association",
        "home_url": "https://www.britgo.org/",
        "feed_url": "https://www.britgo.org/rss.xml",
        "language": "en",
    },
    "igf": {
        "id": "igf",
        "name": "International Go Federation",
        "home_url": "https://intergofed.org/",
        "feed_url": "https://intergofed.org/feed/",
        "language": "en",
    },
    "singapore": {
        "id": "singapore",
        "name": "Singapore Weiqi Association",
        "home_url": "https://weiqi.org.sg/",
        "feed_url": "https://weiqi.org.sg/feed/",
        "language": "en",
    },
    "taiwanchi": {
        "id": "taiwanchi",
        "name": "Taiwan Chi Yuan",
        "home_url": "https://taiwangorg.blogspot.com/",
        "feed_url": "https://taiwangorg.blogspot.com/feeds/posts/default",
        "language": "zh-Hant",
    },
    "australiango": {
        "id": "australiango",
        "name": "Australian Go Association",
        "home_url": "https://australiango.asn.au/",
        "feed_url": "https://australiango.asn.au/feed/",
        "language": "en",
    },
    "thaigo": {
        "id": "thaigo",
        "name": "Thailand Go Association",
        "home_url": "https://www.thaigo.org/",
        "feed_url": (
            "https://www.thaigo.org/category/"
            "%e0%b8%82%e0%b9%88%e0%b8%b2%e0%b8%a7%e0%b8%81%e0%b8%b5%e0%b8%ac"
            "%e0%b8%b2%e0%b8%ab%e0%b8%a1%e0%b8%b2%e0%b8%81%e0%b8%a5%e0%b9%89"
            "%e0%b8%ad%e0%b8%a1/feed/"
        ),
        "language": "th",
        "exclude_title_regex": r"^[a-z0-9]{12,}$",
    },
    "nagf": {
        "id": "nagf",
        "name": "North American Go Federation",
        "home_url": "https://news.nagofed.org/",
        "feed_url": "https://news.nagofed.org/feed/",
        "language": "en",
    },
    "dgob": {
        "id": "dgob",
        "name": "Deutscher Go-Bund",
        "home_url": "https://www.dgob.de/",
        "feed_url": "https://www.dgob.de/feed/",
        "language": "de",
    },
}
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
DEFAULT_OPENAI_MODEL = "gpt-5.5"
AZURE_TRANSLATOR_URL = "https://api.cognitive.microsofttranslator.com/translate"
AZURE_TRANSLATOR_MODEL_LABEL = "azure-translator:text-translation-v3"
EASTERN_TIME_ZONE = ZoneInfo("America/New_York")
NIHONKIIN_CATEGORY_TRANSLATIONS = {
    "その他": "Other",
    "ニュースリリース": "News release",
    "ピックアップ": "Featured",
    "メディア掲載": "Media coverage",
    "会員組織": "Membership",
    "出版": "Publishing",
    "出版・販売": "Publishing and sales",
    "囲碁学習・普及活動": "Go education and outreach",
    "採用情報": "Recruiting",
}


@dataclass(frozen=True)
class AggregatedNewsItem:
    """Normalized external news item."""

    source_id: str
    source_name: str
    source_home_url: str
    source_language: str
    source_article_id: str
    title: str
    url: str
    author: str
    category: str
    published_text: str
    published_at: str | None
    digest_date: str | None
    summary: str
    summary_type: str
    title_en: str
    category_en: str
    summary_en: str
    enrichment_status: str
    enrichment_model: str | None
    image_url: str | None
    fetched_at: str
    content_sha256: str


class EGFNewsBlockCollector(HTMLParser):
    """Collect EGF news item blocks from the home page."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.blocks: list[str] = []
        self._in_news_heading = False
        self._in_h2 = False
        self._h2_text: list[str] = []
        self._collecting_block = False
        self._block_depth = 0
        self._block_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = {name.lower(): value or "" for name, value in attrs}
        if tag.lower() == "h2":
            self._in_h2 = True
            self._h2_text = []
        if self._in_news_heading and tag.lower() == "div":
            style = attrs_dict.get("style", "").replace(" ", "").lower()
            if "background-color:#f4f2e5" in style and not self._collecting_block:
                self._collecting_block = True
                self._block_depth = 0
                self._block_parts = []
        if self._collecting_block:
            if tag.lower() == "div":
                self._block_depth += 1
            self._block_parts.append(self.get_starttag_text() or "")

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self._collecting_block:
            self._block_parts.append(self.get_starttag_text() or "")

    def handle_endtag(self, tag: str) -> None:
        if self._collecting_block:
            self._block_parts.append(f"</{tag}>")
            if tag.lower() == "div":
                self._block_depth -= 1
                if self._block_depth <= 0:
                    self.blocks.append("".join(self._block_parts))
                    self._collecting_block = False
                    self._block_parts = []
        if tag.lower() == "h2":
            heading = normalize_text(" ".join(self._h2_text))
            if heading == "News":
                self._in_news_heading = True
            elif heading == "Calendar":
                self._in_news_heading = False
            self._in_h2 = False
            self._h2_text = []

    def handle_data(self, data: str) -> None:
        if self._in_h2:
            self._h2_text.append(data)
        if self._collecting_block:
            self._block_parts.append(data)

    def handle_entityref(self, name: str) -> None:
        value = f"&{name};"
        if self._in_h2:
            self._h2_text.append(html.unescape(value))
        if self._collecting_block:
            self._block_parts.append(value)

    def handle_charref(self, name: str) -> None:
        value = f"&#{name};"
        if self._in_h2:
            self._h2_text.append(html.unescape(value))
        if self._collecting_block:
            self._block_parts.append(value)


class TextExtractor(HTMLParser):
    """Extract normalized text from a small HTML fragment."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)

    def text(self) -> str:
        return normalize_text(" ".join(self.parts))


def main(argv: list[str] | None = None) -> int:
    """Run the aggregator prototype."""
    configure_stdout()
    args = parse_args(argv)
    fetched_at = utc_now_iso()
    if args.source == "egf":
        html_text = fetch_text(EGF_HOME_URL)
        source = {
            "id": "egf",
            "name": "European Go Federation",
            "home_url": EGF_HOME_URL,
            "language": "en",
        }
        items = parse_egf_news_listing(html_text, fetched_at=fetched_at, limit=args.limit)
    elif args.source == "nihonkiin":
        html_text = fetch_text(NIHONKIIN_NEWS_URL)
        source = {
            "id": "nihonkiin",
            "name": "Nihon Ki-in",
            "home_url": "https://www.nihonkiin.or.jp/",
            "language": "ja",
        }
        items = parse_nihonkiin_news_listing(html_text, fetched_at=fetched_at, limit=args.limit)
    elif args.source == "kbaduk":
        html_text = fetch_text(KBADUK_PRESS_URL)
        source = {
            "id": "kbaduk",
            "name": "Korea Baduk Federation",
            "home_url": "https://www.kbaduk.or.kr/",
            "language": "ko",
        }
        items = parse_kbaduk_press_listing(html_text, fetched_at=fetched_at, limit=args.limit)
    elif args.source == "hangukkiwon":
        html_text = fetch_text(HANGUKKIWON_REPORT_URL)
        source = {
            "id": "hangukkiwon",
            "name": "Hanguk Kiwon",
            "home_url": "https://www.baduk.or.kr/",
            "language": "ko",
        }
        items = parse_hangukkiwon_report_listing(html_text, fetched_at=fetched_at, limit=args.limit)
    elif args.source == "aigp":
        html_text = fetch_text(AIGP_NEWS_URL)
        source = {
            "id": "aigp",
            "name": "Association of Indian Go Players",
            "home_url": "https://aigp.org.in/",
            "language": "en",
        }
        items = parse_aigp_news_listing(html_text, fetched_at=fetched_at, limit=args.limit)
    elif args.source == "pgetc":
        html_text = fetch_text(PGETC_NEWS_URL)
        source = {
            "id": "pgetc",
            "name": "Pandanet European Team Go Championship",
            "home_url": "https://pandanet-igs.com/communities/euroteamchamps",
            "language": "en",
        }
        items = parse_pgetc_news_listing(html_text, fetched_at=fetched_at, limit=args.limit)
    elif args.source == "fedibergo":
        html_text = fetch_text(FEDIBERGO_HOME_URL)
        source = {
            "id": "fedibergo",
            "name": "Federacion Iberoamericana de Go",
            "home_url": FEDIBERGO_HOME_URL,
            "language": "es",
        }
        items = parse_fedibergo_news_listing(html_text, fetched_at=fetched_at, limit=args.limit)
    elif args.source == "cwa":
        records = fetch_cwa_news_records(limit=args.limit)
        source = {
            "id": "cwa",
            "name": "Chinese Weiqi Association",
            "home_url": CWA_HOME_URL,
            "language": "zh-Hans",
        }
        items = parse_cwa_news_records(records, fetched_at=fetched_at, limit=args.limit)
    elif args.source in FEED_SOURCE_CONFIGS:
        config = FEED_SOURCE_CONFIGS[args.source]
        feed_text = fetch_text(str(config["feed_url"]))
        source = feed_source_metadata(config)
        items = parse_generic_feed(config, feed_text, fetched_at=fetched_at, limit=args.limit)
    else:
        raise SystemExit(f"Unsupported source: {args.source}")
    payload = {
        "source": source,
        "fetched_item_count": len(items),
        "items": [asdict(item) for item in items],
    }
    if args.enrich_openai:
        enrich_payload_with_openai(
            payload,
            api_key_env=args.openai_api_key_env,
            model=args.openai_model,
            detail_chars=args.enrichment_detail_chars,
        )
    if args.enrich_azure_translator:
        enrich_payload_with_azure_translator(
            payload,
            api_key_env=args.azure_translator_key_env,
            region_env=args.azure_translator_region_env,
            translator_url=args.azure_translator_url,
            detail_chars=args.enrichment_detail_chars,
        )
    rendered = json.dumps(payload, ensure_ascii=False, indent=2 if args.pretty else None)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


def configure_stdout() -> None:
    """Prefer UTF-8 stdout for multilingual source output on Windows."""
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if reconfigure is not None:
        reconfigure(encoding="utf-8", errors="replace")


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Parse command-line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        default="egf",
        choices=[
            "egf",
            "nihonkiin",
            "kbaduk",
            "hangukkiwon",
            "aigp",
            "pgetc",
            "fedibergo",
            "cwa",
            *FEED_SOURCE_CONFIGS.keys(),
        ],
        help="External news source to fetch.",
    )
    parser.add_argument("--limit", type=int, default=10, help="Maximum items to return.")
    parser.add_argument("--output", type=Path, help="Optional JSON output path.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    parser.add_argument(
        "--enrich-openai",
        action="store_true",
        help="Use OpenAI to translate non-English fields and generate English summaries.",
    )
    parser.add_argument(
        "--enrich-azure-translator",
        action="store_true",
        help="Use Azure Translator to translate non-English fields and a short detail excerpt.",
    )
    parser.add_argument(
        "--openai-model",
        default=os.environ.get("OPENAI_MODEL", DEFAULT_OPENAI_MODEL),
        help=f"OpenAI model for enrichment. Defaults to OPENAI_MODEL or {DEFAULT_OPENAI_MODEL}.",
    )
    parser.add_argument(
        "--openai-api-key-env",
        default="OPENAI_API_KEY",
        help="Environment variable containing the OpenAI API key.",
    )
    parser.add_argument(
        "--azure-translator-key-env",
        default="AZURE_TRANSLATOR_KEY",
        help="Environment variable containing the Azure Translator key.",
    )
    parser.add_argument(
        "--azure-translator-region-env",
        default="AZURE_TRANSLATOR_REGION",
        help="Environment variable containing the Azure Translator resource region.",
    )
    parser.add_argument(
        "--azure-translator-url",
        default=os.environ.get("AZURE_TRANSLATOR_URL", AZURE_TRANSLATOR_URL),
        help=f"Azure Translator translate URL. Defaults to AZURE_TRANSLATOR_URL or {AZURE_TRANSLATOR_URL}.",
    )
    parser.add_argument(
        "--enrichment-detail-chars",
        type=int,
        default=3500,
        help="Maximum detail-page characters to send per item for enrichment.",
    )
    args = parser.parse_args(argv)
    if args.enrich_openai and args.enrich_azure_translator:
        parser.error("Choose either --enrich-openai or --enrich-azure-translator, not both.")
    return args


def fetch_text(url: str) -> str:
    """Fetch a URL as text."""
    request = Request(
        url,
        headers={
            "User-Agent": "AGA-NewsSourcePrototype/0.1 (+https://www.usgo.org/)",
            "Accept": (
                "text/html,application/xhtml+xml,application/xml,text/xml,"
                "application/rss+xml,application/atom+xml"
            ),
        },
    )
    with urlopen(request, timeout=30) as response:
        body = response.read()
        charset = response.headers.get_content_charset() or html_meta_charset(body) or "utf-8"
        return body.decode(charset, errors="replace")


def post_json(url: str, payload: Any, headers: dict[str, str]) -> Any:
    """Post JSON and return a JSON response."""
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            **headers,
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=90) as response:
            body = response.read().decode(response.headers.get_content_charset() or "utf-8", errors="replace")
            return json.loads(body)
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from {url}: {body}") from exc


def post_form_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Post form data and return a JSON response."""
    request = Request(
        url,
        data=urlencode(payload, doseq=True).encode("utf-8"),
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept": "application/json",
            "User-Agent": "AGA-NewsSourcePrototype/0.1 (+https://www.usgo.org/)",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=30) as response:
            body = response.read().decode(response.headers.get_content_charset() or "utf-8", errors="replace")
            return json.loads(body)
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from {url}: {body}") from exc


def fetch_json(url: str) -> dict[str, Any]:
    """Fetch a URL and return a JSON response."""
    request = Request(
        url,
        headers={
            "User-Agent": "AGA-NewsSourcePrototype/0.1 (+https://www.usgo.org/)",
            "Accept": "application/json",
        },
    )
    with urlopen(request, timeout=30) as response:
        body = response.read().decode(response.headers.get_content_charset() or "utf-8", errors="replace")
        return json.loads(body)


def html_meta_charset(body: bytes) -> str | None:
    """Extract an HTML meta charset declaration from the document prefix."""
    prefix = body[:4096].decode("ascii", errors="ignore")
    match = re.search(r"charset\s*=\s*['\"]?([A-Za-z0-9_-]+)", prefix, flags=re.IGNORECASE)
    return match.group(1) if match else None


def enrich_payload_with_openai(
    payload: dict[str, Any],
    api_key_env: str,
    model: str,
    detail_chars: int,
) -> None:
    """Add English translations and summaries to non-English source items."""
    source_language = str(payload.get("source", {}).get("language") or "")
    items = payload.get("items")
    if source_language == "en" or not isinstance(items, list) or not items:
        return

    api_key = os.environ.get(api_key_env)
    if not api_key:
        raise SystemExit(
            f"{api_key_env} is not set. Run without --enrich-openai, or set {api_key_env} "
            "to enable translation and English summaries."
        )

    records = [enrichment_record_for_item(item, detail_chars=detail_chars) for item in items]
    response = post_json(
        OPENAI_RESPONSES_URL,
        openai_enrichment_payload(records, model),
        headers={"Authorization": f"Bearer {api_key}"},
    )
    enrichment = parse_openai_json_response(response)
    indexed = {
        str(item.get("source_article_id") or ""): item
        for item in enrichment.get("items", [])
        if isinstance(item, dict)
    }

    for item in items:
        source_article_id = str(item.get("source_article_id") or "")
        enriched = indexed.get(source_article_id)
        if not enriched:
            item["enrichment_status"] = "missing_model_output"
            item["enrichment_model"] = model
            continue
        item["title_en"] = normalize_text(str(enriched.get("title_en") or ""))
        item["category_en"] = normalize_text(str(enriched.get("category_en") or item.get("category_en") or ""))
        item["summary_en"] = normalize_text(str(enriched.get("summary_en") or ""))
        item["enrichment_status"] = "openai_enriched"
        item["enrichment_model"] = model


def enrich_payload_with_azure_translator(
    payload: dict[str, Any],
    api_key_env: str,
    region_env: str,
    translator_url: str,
    detail_chars: int,
) -> None:
    """Add machine-translated English helper fields using Azure Translator."""
    source_language = str(payload.get("source", {}).get("language") or "")
    items = payload.get("items")
    if source_language == "en" or not isinstance(items, list) or not items:
        return

    api_key = os.environ.get(api_key_env)
    if not api_key:
        raise SystemExit(
            f"{api_key_env} is not set. Set {api_key_env} to enable Azure Translator enrichment. "
            f"Also set {region_env} when using a regional or multi-service resource."
        )
    region = os.environ.get(region_env, "")

    records = [enrichment_record_for_item(item, detail_chars=detail_chars) for item in items]
    texts: list[str] = []
    slots: list[tuple[dict[str, Any], str]] = []
    for item, record in zip(items, records):
        title = normalize_text(str(record.get("title") or ""))
        category = normalize_text(str(record.get("category") or ""))
        summary_source = normalize_text(str(record.get("detail_text") or item.get("summary") or ""))
        if title:
            slots.append((item, "title_en"))
            texts.append(title)
        if category and not item.get("category_en"):
            slots.append((item, "category_en"))
            texts.append(category)
        if summary_source:
            slots.append((item, "summary_en"))
            texts.append(summary_source)

    translations = azure_translate_texts(
        texts,
        api_key=api_key,
        region=region,
        translator_url=translator_url,
        source_language=source_language,
        target_language="en",
    )
    for (item, field), translated in zip(slots, translations):
        translated = normalize_text(translated)
        if field == "summary_en":
            translated = translated_excerpt(translated)
        item[field] = translated

    for item in items:
        item["enrichment_status"] = "azure_translator_enriched" if item.get("summary_en") else "azure_translator_partial"
        item["enrichment_model"] = AZURE_TRANSLATOR_MODEL_LABEL


def azure_translate_texts(
    texts: list[str],
    api_key: str,
    region: str,
    translator_url: str,
    source_language: str,
    target_language: str,
) -> list[str]:
    """Translate a list of plain-text strings with Azure Translator."""
    if not texts:
        return []
    translated: list[str] = []
    query = {
        "api-version": "3.0",
        "from": source_language,
        "to": target_language,
    }
    headers = {
        "Ocp-Apim-Subscription-Key": api_key,
        "Content-Type": "application/json; charset=UTF-8",
    }
    if region:
        headers["Ocp-Apim-Subscription-Region"] = region
    request_url = f"{translator_url}?{urlencode(query)}"
    for batch in chunked(texts, 25):
        response = post_json(request_url, [{"Text": text} for text in batch], headers=headers)
        if not isinstance(response, list):
            raise RuntimeError("Azure Translator response was not a JSON array.")
        for item in response:
            translations = item.get("translations") if isinstance(item, dict) else None
            if not translations or not isinstance(translations, list):
                translated.append("")
                continue
            first = translations[0]
            translated.append(str(first.get("text") or "") if isinstance(first, dict) else "")
    return translated


def translated_excerpt(value: str, max_chars: int = 500) -> str:
    """Return a compact excerpt from translated article-body text."""
    text = normalize_text(value)
    if len(text) <= max_chars:
        return text
    prefix = text[:max_chars].rstrip()
    sentence_breaks = list(re.finditer(r"(?<=[.!?])\s+", prefix))
    if sentence_breaks and sentence_breaks[-1].start() >= max_chars // 3:
        prefix = prefix[: sentence_breaks[-1].start() + 1].rstrip()
    return prefix.rstrip(". ") + "..."


def chunked(values: list[str], size: int) -> list[list[str]]:
    """Split values into batches for a bulk API call."""
    return [values[index : index + size] for index in range(0, len(values), size)]


def enrichment_record_for_item(item: dict[str, Any], detail_chars: int) -> dict[str, Any]:
    """Build one model input record from a normalized item."""
    detail_text = ""
    if item.get("source_id") == "nihonkiin" and item.get("url"):
        detail_text = nihonkiin_article_detail_text(str(item["url"]))[: max(0, detail_chars)]
    elif item.get("source_id") == "kbaduk" and item.get("url"):
        detail_text = kbaduk_article_detail_text(str(item["url"]))[: max(0, detail_chars)]
    elif item.get("source_id") == "cwa" and item.get("source_article_id"):
        detail_text = cwa_article_detail_text(str(item["source_article_id"]))[: max(0, detail_chars)]
    return {
        "source_article_id": item.get("source_article_id"),
        "source_name": item.get("source_name"),
        "source_language": item.get("source_language"),
        "title": item.get("title"),
        "category": item.get("category"),
        "published_text": item.get("published_text"),
        "source_summary": item.get("summary"),
        "url": item.get("url"),
        "detail_text": detail_text,
    }


def openai_enrichment_payload(records: list[dict[str, Any]], model: str) -> dict[str, Any]:
    """Return a Responses API payload for news source enrichment."""
    return {
        "model": model,
        "store": False,
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "You translate and summarize official Go/weiqi/baduk news for an "
                            "American Go Association editor. Preserve names, ranks, tournament "
                            "names, dates, and organization names accurately. Do not add facts "
                            "that are not supported by the source fields. Keep summaries neutral."
                        ),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(
                            {
                                "task": (
                                    "For each item, translate the title and category into English "
                                    "and write a concise one-sentence English summary. Use detail_text "
                                    "when present, otherwise use source_summary when present. If neither "
                                    "is present, summarize only what the title/category imply."
                                ),
                                "items": records,
                            },
                            ensure_ascii=False,
                        ),
                    }
                ],
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "go_news_source_enrichment",
                "strict": True,
                "schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["items"],
                    "properties": {
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["source_article_id", "title_en", "category_en", "summary_en"],
                                "properties": {
                                    "source_article_id": {"type": "string"},
                                    "title_en": {"type": "string"},
                                    "category_en": {"type": "string"},
                                    "summary_en": {"type": "string"},
                                },
                            },
                        }
                    },
                },
            }
        },
    }


def parse_openai_json_response(response: dict[str, Any]) -> dict[str, Any]:
    """Extract the JSON text output from a Responses API result."""
    text = response.get("output_text")
    if isinstance(text, str) and text.strip():
        return json.loads(text)
    for output_item in response.get("output") or []:
        if not isinstance(output_item, dict):
            continue
        for content_item in output_item.get("content") or []:
            if not isinstance(content_item, dict):
                continue
            text = content_item.get("text")
            if isinstance(text, str) and text.strip():
                return json.loads(text)
    raise RuntimeError("OpenAI response did not include JSON text output.")


def nihonkiin_article_detail_text(url: str) -> str:
    """Fetch and extract the readable Nihon Ki-in article body text."""
    article_html = fetch_text(url)
    match = re.search(
        r'<div\b[^>]*class=["\'][^"\']*\bentry_body\b[^"\']*["\'][^>]*>(?P<body>.*?)</div>',
        article_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ""
    return html_fragment_text(strip_active_html(match.group("body")))


def strip_active_html(value: str) -> str:
    """Remove script/style content before text extraction."""
    stripped = re.sub(r"(?is)<script\b[^>]*>.*?</script>", " ", value)
    stripped = re.sub(r"(?is)<style\b[^>]*>.*?</style>", " ", stripped)
    return stripped



def parse_egf_news_listing(html_text: str, fetched_at: str, limit: int) -> list[AggregatedNewsItem]:
    """Parse EGF home-page news cards into normalized records."""
    collector = EGFNewsBlockCollector()
    collector.feed(html_text)
    items: list[AggregatedNewsItem] = []
    for block in collector.blocks:
        item = parse_egf_news_block(block, fetched_at)
        if item is not None:
            items.append(item)
        if len(items) >= limit:
            break
    return items


def parse_egf_news_block(block: str, fetched_at: str) -> AggregatedNewsItem | None:
    """Parse one EGF news card."""
    title_match = re.search(
        r'<a\b[^>]*href=["\'](?P<href>/index\.html\?id=(?P<id>\d+))["\'][^>]*>(?P<title>.*?)</a>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not title_match:
        return None

    article_id = title_match.group("id")
    article_url = urljoin(EGF_HOME_URL, title_match.group("href"))
    title = html_fragment_text(title_match.group("title"))

    metadata_match = re.search(
        r'<div\b[^>]*color:\s*#666666[^>]*>(?P<meta>.*?)</div>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    author, category, published_text, published_at = parse_egf_metadata(
        html_fragment_text(metadata_match.group("meta") if metadata_match else "")
    )

    summary_match = re.search(
        r'<div\b[^>]*text-align:\s*justify[^>]*>(?P<summary>.*?)<a\b[^>]*>\s*Read more\.\.\.\s*</a>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    summary = html_fragment_text(summary_match.group("summary") if summary_match else "")

    image_match = re.search(r'<img\b[^>]*\bsrc=["\'](?P<src>[^"\']+)["\']', block, flags=re.IGNORECASE)
    image_url = urljoin(EGF_HOME_URL, html.unescape(image_match.group("src"))) if image_match else None
    content_sha = sha256_text("\n".join([article_url, title, author, category, published_text, summary]))

    return AggregatedNewsItem(
        source_id="egf",
        source_name="European Go Federation",
        source_home_url=EGF_HOME_URL,
        source_language="en",
        source_article_id=article_id,
        title=title,
        url=article_url,
        author=author,
        category=category,
        published_text=published_text,
        published_at=published_at,
        digest_date=source_digest_date("egf", published_at),
        summary=summary,
        summary_type="source_excerpt",
        title_en=title,
        category_en=category,
        summary_en=summary,
        enrichment_status="source_already_english",
        enrichment_model=None,
        image_url=image_url,
        fetched_at=fetched_at,
        content_sha256=content_sha,
    )


def parse_nihonkiin_news_listing(
    html_text: str,
    fetched_at: str,
    limit: int,
) -> list[AggregatedNewsItem]:
    """Parse Nihon Ki-in news listing records."""
    items: list[AggregatedNewsItem] = []
    for block in re.findall(r"<li\b[^>]*>(.*?)</li>", html_text, flags=re.IGNORECASE | re.DOTALL):
        item = parse_nihonkiin_news_block(block, fetched_at)
        if item is not None:
            items.append(item)
        if len(items) >= limit:
            break
    return items


def parse_nihonkiin_news_block(block: str, fetched_at: str) -> AggregatedNewsItem | None:
    """Parse one Nihon Ki-in news list item."""
    date_match = re.search(
        r'<span\b[^>]*class=["\'][^"\']*\bdate\b[^"\']*["\'][^>]*>(?P<date>.*?)</span>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    category_match = re.search(
        r'<span\b[^>]*class=["\'][^"\']*\bcate\b[^"\']*["\'][^>]*>(?P<category>.*?)</span>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    link_match = re.search(
        r'<a\b[^>]*href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<title>.*?)</a>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not date_match or not category_match or not link_match:
        return None

    article_url = urljoin(NIHONKIIN_NEWS_URL, html.unescape(link_match.group("href")))
    title = html_fragment_text(link_match.group("title"))
    published_text = html_fragment_text(date_match.group("date"))
    category = html_fragment_text(category_match.group("category"))
    published_at = parse_nihonkiin_date(published_text)
    article_id = nihonkiin_article_id(article_url)
    content_sha = sha256_text("\n".join([article_url, title, category, published_text]))

    return AggregatedNewsItem(
        source_id="nihonkiin",
        source_name="Nihon Ki-in",
        source_home_url="https://www.nihonkiin.or.jp/",
        source_language="ja",
        source_article_id=article_id,
        title=title,
        url=article_url,
        author="",
        category=category,
        published_text=published_text,
        published_at=published_at,
        digest_date=source_digest_date("nihonkiin", published_at),
        summary="",
        summary_type="none",
        title_en="",
        category_en=NIHONKIIN_CATEGORY_TRANSLATIONS.get(category, ""),
        summary_en="",
        enrichment_status="not_requested",
        enrichment_model=None,
        image_url=None,
        fetched_at=fetched_at,
        content_sha256=content_sha,
    )


def parse_kbaduk_press_listing(
    html_text: str,
    fetched_at: str,
    limit: int,
) -> list[AggregatedNewsItem]:
    """Parse Korea Baduk Federation press-release table rows."""
    items: list[AggregatedNewsItem] = []
    for row in re.findall(r"<tr\b[^>]*>(.*?)</tr>", html_text, flags=re.IGNORECASE | re.DOTALL):
        item = parse_kbaduk_press_row(row, fetched_at)
        if item is not None:
            items.append(item)
        if len(items) >= limit:
            break
    return items


def parse_kbaduk_press_row(row: str, fetched_at: str) -> AggregatedNewsItem | None:
    """Parse one Korea Baduk Federation press-release table row."""
    link_match = re.search(
        r'<td\b[^>]*class=["\'][^"\']*\bsubject\b[^"\']*["\'][^>]*>\s*'
        r'<a\b[^>]*href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<title>.*?)</a>',
        row,
        flags=re.IGNORECASE | re.DOTALL,
    )
    date_match = re.search(r"<td\b[^>]*>\s*(?P<date>\d{4}\.\d{2}\.\d{2})\s*</td>", row, flags=re.IGNORECASE)
    if not link_match or not date_match:
        return None

    article_url = urljoin(KBADUK_PRESS_URL, html.unescape(link_match.group("href")))
    title = html_fragment_text(link_match.group("title"))
    published_text = date_match.group("date")
    published_at = parse_dotted_date(published_text)
    article_id = urlparse(article_url).path.strip("/") or sha256_text(article_url)[:16]
    content_sha = sha256_text("\n".join([article_url, title, published_text]))

    return AggregatedNewsItem(
        source_id="kbaduk",
        source_name="Korea Baduk Federation",
        source_home_url="https://www.kbaduk.or.kr/",
        source_language="ko",
        source_article_id=article_id,
        title=title,
        url=article_url,
        author="",
        category="Press release",
        published_text=published_text,
        published_at=published_at,
        digest_date=source_digest_date("kbaduk", published_at),
        summary="",
        summary_type="none",
        title_en="",
        category_en="Press release",
        summary_en="",
        enrichment_status="not_requested",
        enrichment_model=None,
        image_url=None,
        fetched_at=fetched_at,
        content_sha256=content_sha,
    )


def parse_hangukkiwon_report_listing(
    html_text: str,
    fetched_at: str,
    limit: int,
) -> list[AggregatedNewsItem]:
    """Parse Hanguk Kiwon report list items."""
    items: list[AggregatedNewsItem] = []
    for block in re.findall(r"<li\b[^>]*>(.*?)</li>", html_text, flags=re.IGNORECASE | re.DOTALL):
        item = parse_hangukkiwon_report_item(block, fetched_at)
        if item is not None:
            items.append(item)
        if len(items) >= limit:
            break
    return items


def parse_hangukkiwon_report_item(block: str, fetched_at: str) -> AggregatedNewsItem | None:
    """Parse one Hanguk Kiwon report list item."""
    link_match = re.search(
        r'<a\b[^>]*href=["\'](?P<href>[^"\']*report_view\.asp\?news_no=(?P<id>\d+)[^"\']*)["\']',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    title_match = re.search(r"<dt\b[^>]*>(?P<title>.*?)</dt>", block, flags=re.IGNORECASE | re.DOTALL)
    summary_match = re.search(r"<dd\b[^>]*>(?P<summary>.*?)</dd>", block, flags=re.IGNORECASE | re.DOTALL)
    date_match = re.search(
        r'<span\b[^>]*class=["\'][^"\']*\bdate\b[^"\']*["\'][^>]*>(?P<date>\d{4}-\d{2}-\d{2})</span>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not link_match or not title_match or not date_match:
        return None

    article_id = link_match.group("id")
    article_url = urljoin(HANGUKKIWON_REPORT_URL, html.unescape(link_match.group("href")))
    title = html_fragment_text(title_match.group("title"))
    summary = html_fragment_text(summary_match.group("summary") if summary_match else "")
    published_text = normalize_text(date_match.group("date"))
    published_at = parse_iso_date(published_text)
    image_match = re.search(r'<img\b[^>]*\bsrc=["\'](?P<src>[^"\']+)["\']', block, flags=re.IGNORECASE)
    image_url = urljoin(article_url, html.unescape(image_match.group("src"))) if image_match else None
    content_sha = sha256_text("\n".join([article_url, title, published_text, summary]))

    return AggregatedNewsItem(
        source_id="hangukkiwon",
        source_name="Hanguk Kiwon",
        source_home_url="https://www.baduk.or.kr/",
        source_language="ko",
        source_article_id=article_id,
        title=title,
        url=article_url,
        author="",
        category="News report",
        published_text=published_text,
        published_at=published_at,
        digest_date=source_digest_date("hangukkiwon", published_at),
        summary=summary,
        summary_type="source_excerpt" if summary else "none",
        title_en="",
        category_en="News report",
        summary_en="",
        enrichment_status="not_requested",
        enrichment_model=None,
        image_url=image_url,
        fetched_at=fetched_at,
        content_sha256=content_sha,
    )


def parse_aigp_news_listing(
    html_text: str,
    fetched_at: str,
    limit: int,
) -> list[AggregatedNewsItem]:
    """Parse Association of Indian Go Players news cards."""
    items: list[AggregatedNewsItem] = []
    for block in re.findall(r"<article\b[^>]*>(.*?)</article>", html_text, flags=re.IGNORECASE | re.DOTALL):
        item = parse_aigp_news_card(block, fetched_at)
        if item is not None:
            items.append(item)
        if len(items) >= limit:
            break
    return items


def parse_aigp_news_card(block: str, fetched_at: str) -> AggregatedNewsItem | None:
    """Parse one AIGP news card."""
    title_match = re.search(
        r'<h4\b[^>]*class=["\'][^"\']*\btitle\b[^"\']*["\'][^>]*>(?P<title>.*?)</h4>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    date_match = re.search(r"<em>\s*Date:\s*(?P<date>.*?)</em>", block, flags=re.IGNORECASE | re.DOTALL)
    link_match = re.search(
        r'<span\b[^>]*class=["\'][^"\']*\bis-pulled-bottom-right\b[^"\']*["\'][^>]*>.*?'
        r'<a\b[^>]*href=["\'](?P<href>[^"\']+)["\']',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not title_match or not date_match or not link_match:
        return None

    title = html_fragment_text(title_match.group("title"))
    published_text = html_fragment_text(date_match.group("date"))
    published_at = parse_aigp_date(published_text)
    article_url = urljoin("https://aigp.org.in/", html.unescape(link_match.group("href")))
    article_id = urlparse(article_url).path.strip("/") or sha256_text(article_url)[:16]

    summary_match = re.search(
        r'</em>\s*(?P<summary>.*?)<span\b[^>]*class=["\'][^"\']*\bis-pulled-bottom-right',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    summary_html = summary_match.group("summary") if summary_match else ""
    summary_html = re.sub(r"(?is)^\s*(?:<br\s*/?>\s*)+", "", summary_html)
    summary_html = re.sub(r"(?is)(?:<br\s*/?>\s*)+$", "", summary_html)
    summary = html_fragment_text(strip_active_html(summary_html))

    image_match = re.search(r'<img\b[^>]*\bsrc=["\'](?P<src>[^"\']+)["\']', block, flags=re.IGNORECASE)
    image_url = urljoin("https://aigp.org.in/", html.unescape(image_match.group("src"))) if image_match else None
    content_sha = sha256_text("\n".join([article_url, title, published_text, summary]))

    return AggregatedNewsItem(
        source_id="aigp",
        source_name="Association of Indian Go Players",
        source_home_url="https://aigp.org.in/",
        source_language="en",
        source_article_id=article_id,
        title=title,
        url=article_url,
        author="",
        category="News",
        published_text=published_text,
        published_at=published_at,
        digest_date=source_digest_date("aigp", published_at),
        summary=summary,
        summary_type="source_excerpt" if summary else "none",
        title_en=title,
        category_en="News",
        summary_en=summary,
        enrichment_status="source_already_english",
        enrichment_model=None,
        image_url=image_url,
        fetched_at=fetched_at,
        content_sha256=content_sha,
    )


def parse_pgetc_news_listing(
    html_text: str,
    fetched_at: str,
    limit: int,
) -> list[AggregatedNewsItem]:
    """Parse Pandanet European Team Go Championship news boxes."""
    items: list[AggregatedNewsItem] = []
    for block in re.findall(
        r'<div\b[^>]*class=["\'][^"\']*\bnode-box\b[^"\']*["\'][^>]*>(.*?)</div>\s*</div>',
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        item = parse_pgetc_news_box(block, fetched_at)
        if item is not None:
            items.append(item)
        if len(items) >= limit:
            break
    return items


def parse_pgetc_news_box(block: str, fetched_at: str) -> AggregatedNewsItem | None:
    """Parse one PGETC news box."""
    date_match = re.search(
        r'<div\b[^>]*class=["\'][^"\']*\bdate\b[^"\']*["\'][^>]*>(?P<date>.*?)</div>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    title_match = re.search(
        r'<h3\b[^>]*class=["\'][^"\']*\bnode-title\b[^"\']*["\'][^>]*>(?P<title>.*?)</h3>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    link_match = re.search(
        r'<div\b[^>]*class=["\'][^"\']*\bnode-footer\b[^"\']*["\'][^>]*>.*?'
        r'<a\b[^>]*href=["\'](?P<href>[^"\']+)["\']',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not date_match or not title_match or not link_match:
        return None

    article_url = urljoin(PGETC_NEWS_URL, html.unescape(link_match.group("href")))
    article_id = urlparse(article_url).path.strip("/") or sha256_text(article_url)[:16]
    title = html_fragment_text(title_match.group("title"))
    published_text = html_fragment_text(date_match.group("date"))
    published_at = parse_pgetc_datetime(published_text)
    content_match = re.search(
        r'<div\b[^>]*class=["\'][^"\']*\bnode-content\b[^"\']*["\'][^>]*>(?P<content>.*?)</div>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    summary = feed_summary_text(content_match.group("content") if content_match else "")
    content_sha = sha256_text("\n".join([article_url, title, published_text, summary]))

    return AggregatedNewsItem(
        source_id="pgetc",
        source_name="Pandanet European Team Go Championship",
        source_home_url="https://pandanet-igs.com/communities/euroteamchamps",
        source_language="en",
        source_article_id=article_id,
        title=title,
        url=article_url,
        author="",
        category="European team championship",
        published_text=published_text,
        published_at=published_at,
        digest_date=source_digest_date("pgetc", published_at),
        summary=summary,
        summary_type="source_excerpt" if summary else "none",
        title_en=title,
        category_en="European team championship",
        summary_en=summary,
        enrichment_status="source_already_english",
        enrichment_model=None,
        image_url=None,
        fetched_at=fetched_at,
        content_sha256=content_sha,
    )


def parse_fedibergo_news_listing(
    html_text: str,
    fetched_at: str,
    limit: int,
) -> list[AggregatedNewsItem]:
    """Parse Fedibergo latest-news article cards."""
    items: list[AggregatedNewsItem] = []
    for block in re.findall(
        r'<div\b[^>]*class=["\'][^"\']*\bsppb-addon-article\b[^"\']*["\'][^>]*>(.*?)</div>\s*</div>\s*</div>',
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        item = parse_fedibergo_news_card(block, fetched_at)
        if item is not None:
            items.append(item)
        if len(items) >= limit:
            break
    return items


def parse_fedibergo_news_card(block: str, fetched_at: str) -> AggregatedNewsItem | None:
    """Parse one Fedibergo latest-news card."""
    link_match = re.search(
        r'<h3\b[^>]*>.*?<a\b[^>]*href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<title>.*?)</a>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    day_match = re.search(
        r'<span\b[^>]*class=["\'][^"\']*\bsppb-meta-date\b[^"\']*["\'][^>]*>(?P<day>\d{1,2})</span>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    month_match = re.search(
        r'<span\b[^>]*class=["\'][^"\']*\bsppb-meta-month\b[^"\']*["\'][^>]*>(?P<month>.*?)</span>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not link_match or not day_match or not month_match:
        return None

    article_url = urljoin(FEDIBERGO_HOME_URL, html.unescape(link_match.group("href")))
    title = html_fragment_text(link_match.group("title"))
    article_id = urlparse(article_url).path.strip("/") or sha256_text(article_url)[:16]
    image_match = re.search(r'<img\b[^>]*\bsrc=["\'](?P<src>[^"\']+)["\']', block, flags=re.IGNORECASE)
    image_url = urljoin(FEDIBERGO_HOME_URL, html.unescape(image_match.group("src"))) if image_match else None
    published_text = f"{html_fragment_text(day_match.group('day'))} {html_fragment_text(month_match.group('month'))}"
    published_at = parse_fedibergo_date(published_text, image_url, fetched_at)
    content_sha = sha256_text("\n".join([article_url, title, published_text, image_url or ""]))

    return AggregatedNewsItem(
        source_id="fedibergo",
        source_name="Federacion Iberoamericana de Go",
        source_home_url=FEDIBERGO_HOME_URL,
        source_language="es",
        source_article_id=article_id,
        title=title,
        url=article_url,
        author="",
        category="News",
        published_text=published_text,
        published_at=published_at,
        digest_date=source_digest_date("fedibergo", published_at),
        summary="",
        summary_type="none",
        title_en="",
        category_en="News",
        summary_en="",
        enrichment_status="not_requested",
        enrichment_model=None,
        image_url=image_url,
        fetched_at=fetched_at,
        content_sha256=content_sha,
    )


def fetch_cwa_news_records(limit: int) -> list[dict[str, Any]]:
    """Fetch Chinese Weiqi Association web news records."""
    page_size = max(1, min(limit, 50))
    response = post_form_json(
        f"{CWA_API_BASE_URL}/news/publish/list",
        {
            "page": 1,
            "pageSize": page_size,
            "newsChannel": "web",
            "newsType": 1,
        },
    )
    data = cwa_response_data(response)
    records = data.get("records") if isinstance(data, dict) else None
    if not isinstance(records, list):
        raise RuntimeError("Chinese Weiqi Association response did not include data.records.")
    return [record for record in records if isinstance(record, dict)]


def parse_cwa_news_records(
    records: list[dict[str, Any]],
    fetched_at: str,
    limit: int,
) -> list[AggregatedNewsItem]:
    """Parse Chinese Weiqi Association API records into normalized items."""
    items: list[AggregatedNewsItem] = []
    for record in records:
        item = parse_cwa_news_record(record, fetched_at)
        if item is not None:
            items.append(item)
        if len(items) >= limit:
            break
    return items


def parse_cwa_news_record(record: dict[str, Any], fetched_at: str) -> AggregatedNewsItem | None:
    """Parse one Chinese Weiqi Association API record."""
    article_id = normalize_text(str(record.get("newsPublishNo") or record.get("newsNo") or ""))
    title = normalize_text(str(record.get("newsTitle") or ""))
    if not article_id or not title:
        return None
    article_url = urljoin(CWA_HOME_URL, f"news/details/{article_id}")
    author = normalize_text(str(record.get("newsAuthor") or ""))
    category = normalize_text(str(record.get("newsClassify1Name") or "News"))
    published_text = normalize_text(str(record.get("newsDate") or ""))
    published_at = parse_iso_date(published_text)
    summary = html_fragment_text(str(record.get("newsAbstract") or ""))
    image_url = normalize_text(str(record.get("newsCover") or "")) or None
    content_sha = sha256_text("\n".join([article_url, title, author, category, published_text, summary]))

    return AggregatedNewsItem(
        source_id="cwa",
        source_name="Chinese Weiqi Association",
        source_home_url=CWA_HOME_URL,
        source_language="zh-Hans",
        source_article_id=article_id,
        title=title,
        url=article_url,
        author=author,
        category=category,
        published_text=published_text,
        published_at=published_at,
        digest_date=source_digest_date("cwa", published_at),
        summary=summary,
        summary_type="source_excerpt" if summary else "none",
        title_en="",
        category_en="",
        summary_en="",
        enrichment_status="not_requested",
        enrichment_model=None,
        image_url=image_url,
        fetched_at=fetched_at,
        content_sha256=content_sha,
    )


def feed_source_metadata(config: dict[str, str]) -> dict[str, str]:
    """Return public source metadata from a feed source config."""
    return {
        "id": str(config["id"]),
        "name": str(config["name"]),
        "home_url": str(config["home_url"]),
        "language": str(config["language"]),
    }


def parse_generic_feed(
    config: dict[str, str],
    feed_text: str,
    fetched_at: str,
    limit: int,
) -> list[AggregatedNewsItem]:
    """Parse RSS or Atom feed items into normalized source records."""
    root = ET.fromstring(feed_text.lstrip())
    if xml_local_name(root.tag) == "feed":
        entries = xml_direct_children(root, "entry")
        parser = parse_atom_feed_entry
    else:
        channel = xml_first_child(root, "channel")
        if channel is None:
            channel = root
        entries = xml_direct_children(channel, "item")
        parser = parse_rss_feed_item

    items: list[AggregatedNewsItem] = []
    for entry in entries:
        item = parser(config, entry, fetched_at)
        if item is not None:
            items.append(item)
        if len(items) >= limit:
            break
    return items


def parse_rss_feed_item(
    config: dict[str, str],
    item: ET.Element,
    fetched_at: str,
) -> AggregatedNewsItem | None:
    """Parse one RSS item into a normalized source record."""
    source_id = str(config["id"])
    source_name = str(config["name"])
    source_home_url = str(config["home_url"])
    source_language = str(config["language"])

    title = xml_child_text(item, "title")
    link = xml_child_text(item, "link")
    guid = xml_child_text(item, "guid")
    if not title or not (link or guid):
        return None

    article_url = urljoin(source_home_url, link or guid)
    article_id = feed_article_id(article_url, guid)
    author = xml_child_text(item, "creator") or xml_child_text(item, "author")
    category = " | ".join(xml_child_texts(item, "category"))
    published_text = (
        xml_child_text(item, "pubDate")
        or xml_child_text(item, "date")
        or xml_child_text(item, "published")
        or xml_child_text(item, "updated")
    )
    published_at = parse_feed_datetime(published_text)
    content_html = xml_child_raw_text(item, "encoded") or xml_child_raw_text(item, "content")
    summary_html = xml_child_raw_text(item, "description") or xml_child_raw_text(item, "summary") or content_html
    summary = feed_summary_text(summary_html)
    if feed_config_excludes_item(config, title, summary):
        return None
    image_url = feed_image_url(item, content_html or summary_html, article_url)
    content_sha = sha256_text("\n".join([article_url, title, author, category, published_text, summary]))
    return feed_item_record(
        source_id=source_id,
        source_name=source_name,
        source_home_url=source_home_url,
        source_language=source_language,
        article_id=article_id,
        title=title,
        article_url=article_url,
        author=author,
        category=category,
        published_text=published_text,
        published_at=published_at,
        summary=summary,
        image_url=image_url,
        fetched_at=fetched_at,
        content_sha=content_sha,
    )


def parse_atom_feed_entry(
    config: dict[str, str],
    entry: ET.Element,
    fetched_at: str,
) -> AggregatedNewsItem | None:
    """Parse one Atom entry into a normalized source record."""
    source_id = str(config["id"])
    source_name = str(config["name"])
    source_home_url = str(config["home_url"])
    source_language = str(config["language"])

    title = xml_child_text(entry, "title")
    link = atom_entry_link(entry)
    entry_id = xml_child_text(entry, "id")
    if not title or not (link or entry_id):
        return None

    article_url = urljoin(source_home_url, link or entry_id)
    article_id = feed_article_id(article_url, entry_id)
    author = atom_entry_author(entry)
    category = " | ".join(atom_entry_categories(entry))
    published_text = xml_child_text(entry, "published") or xml_child_text(entry, "updated")
    published_at = parse_feed_datetime(published_text)
    content_html = xml_child_raw_text(entry, "content")
    summary_html = xml_child_raw_text(entry, "summary") or content_html or xml_child_raw_text(entry, "description")
    summary = feed_summary_text(summary_html)
    if feed_config_excludes_item(config, title, summary):
        return None
    image_url = feed_image_url(entry, content_html or summary_html, article_url)
    content_sha = sha256_text("\n".join([article_url, title, author, category, published_text, summary]))
    return feed_item_record(
        source_id=source_id,
        source_name=source_name,
        source_home_url=source_home_url,
        source_language=source_language,
        article_id=article_id,
        title=title,
        article_url=article_url,
        author=author,
        category=category,
        published_text=published_text,
        published_at=published_at,
        summary=summary,
        image_url=image_url,
        fetched_at=fetched_at,
        content_sha=content_sha,
    )


def feed_item_record(
    source_id: str,
    source_name: str,
    source_home_url: str,
    source_language: str,
    article_id: str,
    title: str,
    article_url: str,
    author: str,
    category: str,
    published_text: str,
    published_at: str | None,
    summary: str,
    image_url: str | None,
    fetched_at: str,
    content_sha: str,
) -> AggregatedNewsItem:
    """Create a normalized feed-backed news item."""
    source_is_english = source_language == "en"
    return AggregatedNewsItem(
        source_id=source_id,
        source_name=source_name,
        source_home_url=source_home_url,
        source_language=source_language,
        source_article_id=article_id,
        title=title,
        url=article_url,
        author=author,
        category=category,
        published_text=published_text,
        published_at=published_at,
        digest_date=source_digest_date(source_id, published_at),
        summary=summary,
        summary_type="source_excerpt" if summary else "none",
        title_en=title if source_is_english else "",
        category_en=category if source_is_english else "",
        summary_en=summary if source_is_english else "",
        enrichment_status="source_already_english" if source_is_english else "not_requested",
        enrichment_model=None,
        image_url=image_url,
        fetched_at=fetched_at,
        content_sha256=content_sha,
    )


def feed_config_excludes_item(config: dict[str, str], title: str, summary: str) -> bool:
    """Return whether a source config asks to skip a likely non-news item."""
    title_pattern = normalize_text(str(config.get("exclude_title_regex", "")))
    if title_pattern and re.search(title_pattern, title, flags=re.IGNORECASE):
        return True
    summary_pattern = normalize_text(str(config.get("exclude_summary_regex", "")))
    return bool(summary_pattern and re.search(summary_pattern, summary, flags=re.IGNORECASE))


def feed_article_id(article_url: str, fallback: str) -> str:
    """Return a stable id for a feed item."""
    parsed = urlparse(article_url)
    if parsed.path.strip("/"):
        return parsed.path.strip("/")
    fallback = normalize_text(fallback)
    return fallback or sha256_text(article_url)[:16]


def atom_entry_link(entry: ET.Element) -> str:
    """Return the preferred Atom link href."""
    fallback = ""
    for child in xml_direct_children(entry, "link"):
        href = normalize_text(child.attrib.get("href", ""))
        if not href:
            continue
        rel = normalize_text(child.attrib.get("rel", "alternate"))
        if rel == "alternate":
            return href
        fallback = fallback or href
    return fallback or xml_child_text(entry, "link")


def atom_entry_author(entry: ET.Element) -> str:
    """Return an Atom entry author name."""
    author = xml_first_child(entry, "author")
    if author is None:
        return ""
    return xml_child_text(author, "name") or normalize_text(" ".join(author.itertext()))


def atom_entry_categories(entry: ET.Element) -> list[str]:
    """Return Atom category labels."""
    categories: list[str] = []
    for child in xml_direct_children(entry, "category"):
        value = normalize_text(child.attrib.get("label", "") or child.attrib.get("term", ""))
        if value:
            categories.append(value)
    return categories


def feed_summary_text(value: str) -> str:
    """Return a compact text excerpt from a feed summary/content value."""
    return translated_excerpt(html_fragment_text(strip_active_html(value)), max_chars=500) if value else ""


def feed_image_url(entry: ET.Element, summary_html: str, article_url: str) -> str | None:
    """Return a likely lead image URL from feed metadata or summary HTML."""
    for child in entry.iter():
        tag_name = xml_local_name(child.tag)
        if tag_name in {"thumbnail", "content"}:
            candidate = normalize_text(child.attrib.get("url", ""))
            media_type = normalize_text(child.attrib.get("type", ""))
            image_url = urljoin(article_url, candidate) if candidate else ""
            if image_url and not is_ignorable_image_url(image_url) and (
                tag_name == "thumbnail" or media_type.startswith("image/")
            ):
                return image_url
        if tag_name == "enclosure":
            candidate = normalize_text(child.attrib.get("url", ""))
            media_type = normalize_text(child.attrib.get("type", ""))
            image_url = urljoin(article_url, candidate) if candidate else ""
            if image_url and not is_ignorable_image_url(image_url) and media_type.startswith("image/"):
                return image_url
    image_match = re.search(r"<img\b(?P<attrs>[^>]*)>", summary_html, flags=re.IGNORECASE | re.DOTALL)
    if image_match:
        return best_image_url_from_img_attrs(image_match.group("attrs"), article_url)
    return None


def best_image_url_from_img_attrs(attrs: str, article_url: str) -> str | None:
    """Return the best URL from one HTML image tag's attributes."""
    candidates: list[str] = []
    srcset_match = re.search(r'\bsrcset=["\'](?P<srcset>[^"\']+)["\']', attrs, flags=re.IGNORECASE | re.DOTALL)
    if srcset_match:
        for srcset_item in srcset_match.group("srcset").split(","):
            candidate = normalize_text(srcset_item).split(" ", 1)[0]
            if candidate:
                candidates.append(candidate)
    src_match = re.search(r'\bsrc=["\'](?P<src>[^"\']+)["\']', attrs, flags=re.IGNORECASE | re.DOTALL)
    if src_match:
        candidates.append(src_match.group("src"))

    article_host = urlparse(article_url).netloc.lower()
    resolved = [urljoin(article_url, html.unescape(candidate)) for candidate in candidates]
    for candidate in resolved:
        if not is_ignorable_image_url(candidate) and urlparse(candidate).netloc.lower() == article_host:
            return candidate
    for candidate in resolved:
        if not is_ignorable_image_url(candidate):
            return candidate
    return None


def is_ignorable_image_url(value: str) -> bool:
    """Return whether an extracted image URL is not a useful lead image."""
    parsed = urlparse(value)
    path = parsed.path.lower()
    if parsed.scheme == "data":
        return True
    if parsed.netloc.lower() == "s.w.org" and "/emoji/" in path:
        return True
    return False


def kbaduk_article_detail_text(url: str) -> str:
    """Fetch and extract readable Korea Baduk Federation article body text."""
    article_html = fetch_text(url)
    match = re.search(
        r'<div\b[^>]*class=["\'][^"\']*\bboardView\b[^"\']*["\'][^>]*'
        r'id=["\']viewLayerout["\'][^>]*>(?P<body>.*?)<!--\s*//\s*contentView\s*-->',
        article_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ""
    return html_fragment_text(strip_active_html(match.group("body")))


def cwa_article_detail_text(article_id: str) -> str:
    """Fetch and extract readable Chinese Weiqi Association article body text."""
    response = fetch_json(f"{CWA_API_BASE_URL}/news/publish/{article_id}")
    data = cwa_response_data(response)
    if not isinstance(data, dict):
        return ""
    detail_html = str(data.get("newsContent") or data.get("newsAbstract") or "")
    return html_fragment_text(strip_active_html(detail_html))


def cwa_response_data(response: dict[str, Any]) -> Any:
    """Return data from a Chinese Weiqi Association API response or raise."""
    if response.get("code") != 0:
        raise RuntimeError(f"Chinese Weiqi Association API error: {response.get('msg') or response.get('code')}")
    return response.get("data")


def parse_egf_metadata(value: str) -> tuple[str, str, str, str | None]:
    """Parse EGF metadata text into author/category/date fields."""
    parts = [part.strip() for part in value.split("|") if part.strip()]
    author = ""
    category = ""
    published_text = ""
    if parts and parts[0].startswith("By "):
        author = parts.pop(0)[3:].strip()
    date_index = next((index for index, part in enumerate(parts) if is_egf_datetime_text(part)), None)
    if date_index is not None:
        published_text = parts[date_index]
        category = " | ".join(parts[:date_index]).strip()
    elif parts:
        published_text = parts[-1]
        category = " | ".join(parts[:-1]).strip()
    published_at = parse_egf_datetime(published_text)
    return author, category, published_text, published_at


def is_egf_datetime_text(value: str) -> bool:
    """Return whether text looks like EGF's dd.mm.yyyy h:mm date."""
    return bool(re.fullmatch(r"\d{2}\.\d{2}\.\d{4}\s+\d{1,2}:\d{2}", value.strip()))


def parse_egf_datetime(value: str) -> str | None:
    """Return ISO local datetime from EGF's dd.mm.yyyy h:mm text."""
    try:
        return datetime.strptime(value.strip(), "%d.%m.%Y %H:%M").isoformat(timespec="minutes")
    except ValueError:
        return None


def parse_nihonkiin_date(value: str) -> str | None:
    """Return ISO date from Nihon Ki-in's yyyy年mm月dd日 text."""
    match = re.fullmatch(r"\s*(\d{4})年(\d{1,2})月(\d{1,2})日\s*", value)
    if not match:
        return None
    year, month, day = (int(piece) for piece in match.groups())
    return datetime(year, month, day).date().isoformat()


def parse_dotted_date(value: str) -> str | None:
    """Return ISO date from yyyy.mm.dd text."""
    with suppress(ValueError):
        return datetime.strptime(value.strip(), "%Y.%m.%d").date().isoformat()
    return None


def parse_iso_date(value: str) -> str | None:
    """Return ISO date from yyyy-mm-dd text."""
    with suppress(ValueError):
        return date.fromisoformat(value.strip()[:10]).isoformat()
    return None


def parse_aigp_date(value: str) -> str | None:
    """Return ISO date from AIGP's dd Mon, yyyy text."""
    with suppress(ValueError):
        return datetime.strptime(normalize_text(value), "%d %b, %Y").date().isoformat()
    return None


def parse_pgetc_datetime(value: str) -> str | None:
    """Return ISO local datetime from PGETC's dd Mon yy HH:MM:SS text."""
    with suppress(ValueError):
        return datetime.strptime(normalize_text(value), "%d %b %y %H:%M:%S").isoformat(timespec="seconds")
    return None


def parse_fedibergo_date(value: str, image_url: str | None, fetched_at: str) -> str | None:
    """Return ISO date from Fedibergo day/month cards, inferring year from image path."""
    month_numbers = {
        "ene": 1,
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "abr": 4,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "aug": 8,
        "sep": 9,
        "sept": 9,
        "oct": 10,
        "nov": 11,
        "dic": 12,
        "dec": 12,
    }
    pieces = normalize_text(value).lower().split()
    if len(pieces) < 2:
        return None
    with suppress(ValueError):
        day = int(pieces[0])
        month = month_numbers.get(pieces[1][:4]) or month_numbers.get(pieces[1][:3])
        if month is None:
            return None
        year = fedibergo_year_from_image_url(image_url) or iso_date_prefix(fetched_at)
        if isinstance(year, str):
            year = int(year[:4])
        return date(int(year), month, day).isoformat()
    return None


def fedibergo_year_from_image_url(image_url: str | None) -> int | None:
    """Infer a Fedibergo news year from image paths such as /images/2026/06/05/."""
    if not image_url:
        return None
    match = re.search(r"/images/(?P<year>20\d{2})/", image_url)
    return int(match.group("year")) if match else None


def parse_feed_datetime(value: str) -> str | None:
    """Return ISO datetime or date from common RSS/Atom date text."""
    text = normalize_text(value)
    if not text:
        return None
    with suppress(ValueError, TypeError, IndexError):
        return parsedate_to_datetime(text).isoformat(timespec="seconds")
    iso_text = text.replace("Z", "+00:00")
    with suppress(ValueError):
        return datetime.fromisoformat(iso_text).isoformat(timespec="seconds")
    with suppress(ValueError):
        return date.fromisoformat(text[:10]).isoformat()
    return None


def source_digest_date(source_id: str, published_at: str | None) -> str | None:
    """Return an Eastern publication date for compatibility metadata only."""
    del source_id
    text = str(published_at or "").strip()
    if not text:
        return None
    if "T" in text:
        with suppress(ValueError):
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                return parsed.astimezone(EASTERN_TIME_ZONE).date().isoformat()
    return iso_date_prefix(text)


def iso_date_prefix(value: str | None) -> str | None:
    """Return the leading yyyy-mm-dd date from an ISO-ish value."""
    if not value:
        return None
    match = re.match(r"\d{4}-\d{2}-\d{2}", value)
    return match.group(0) if match else None


def nihonkiin_article_id(url: str) -> str:
    """Return a stable Nihon Ki-in source article id from a URL."""
    parsed = urlparse(url)
    return parsed.path.strip("/") or sha256_text(url)[:16]


def xml_local_name(tag: str) -> str:
    """Return an XML tag local name without its namespace."""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def xml_direct_children(element: ET.Element, local_name: str) -> list[ET.Element]:
    """Return direct child elements matching a local name."""
    return [child for child in list(element) if xml_local_name(child.tag) == local_name]


def xml_first_child(element: ET.Element, local_name: str) -> ET.Element | None:
    """Return the first direct child with a matching local name."""
    for child in list(element):
        if xml_local_name(child.tag) == local_name:
            return child
    return None


def xml_child_raw_text(element: ET.Element, *local_names: str) -> str:
    """Return raw concatenated text for the first matching direct child."""
    for child in list(element):
        if xml_local_name(child.tag) in local_names:
            return "".join(child.itertext()).strip()
    return ""


def xml_child_text(element: ET.Element, *local_names: str) -> str:
    """Return normalized text for the first matching direct child."""
    return normalize_text(xml_child_raw_text(element, *local_names))


def xml_child_texts(element: ET.Element, local_name: str) -> list[str]:
    """Return normalized text values for matching direct children."""
    values: list[str] = []
    for child in list(element):
        if xml_local_name(child.tag) == local_name:
            value = normalize_text("".join(child.itertext()))
            if value:
                values.append(value)
    return values


def html_fragment_text(value: str) -> str:
    """Extract text from a small HTML fragment."""
    parser = TextExtractor()
    parser.feed(value)
    return parser.text()


def normalize_text(value: str) -> str:
    """Normalize whitespace and HTML entities in extracted text."""
    text = html.unescape(value).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def utc_now_iso() -> str:
    """Return current UTC timestamp."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_text(value: str) -> str:
    """Hash normalized source fields."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
