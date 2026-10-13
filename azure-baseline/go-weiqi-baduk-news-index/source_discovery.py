from dataclasses import asdict, dataclass
from legacy_sources import *
from bs4 import BeautifulSoup
from national_sources import HTML_SOURCES, national_archive
from additional_sources import additional_recent

EGJ_SOURCE = {'id': 'egj', 'name': 'European Go Journal', 'language': 'en',
              'home_url': 'https://eurogojournal.com/blog/'}


def parse_egj_blog(raw):
    """Public blog catalogue only; magazine storefronts are not article bodies."""
    soup = BeautifulSoup(raw, 'html.parser')
    items = {}
    for row in soup.select('main .posts .post'):
        link = row.select_one('a.title[href]')
        stamp = row.find('span', recursive=False)
        if not link or not stamp:
            raise ValueError('European Go Journal article title/date missing')
        url = urljoin(EGJ_SOURCE['home_url'], link['href'])
        parsed = urlparse(url)
        if parsed.hostname != 'eurogojournal.com' or not re.fullmatch(r'/blog/[^/]+/', parsed.path):
            continue
        published = datetime.strptime(stamp.get_text(' ', strip=True), '%b %d, %Y').date().isoformat()
        items[url] = {'source_id': 'egj', 'source_name': EGJ_SOURCE['name'], 'source_language': 'en',
                      'source_article_id': parsed.path.strip('/'), 'url': url,
                      'title': link.get_text(' ', strip=True), 'published_at': published}
    if not items:
        raise ValueError('European Go Journal returned no dated public blog articles')
    # Today the complete catalogue is one page. Fail visibly if pagination appears.
    if soup.select('main a[rel="next"], main .pagination, main .pager'):
        raise ValueError('European Go Journal pagination requires an archive adapter update')
    return sorted(items.values(), key=lambda i: i['published_at'], reverse=True)

@dataclass
class ExternalNewsSyncConfig:
    item_limit_per_source: int = 100

def fetch_external_source_payload(
    source_id: str,
    config: ExternalNewsSyncConfig,
    *,
    translate: bool = True,
) -> dict[str, Any]:
    """Fetch and parse one configured external source."""
    fetched_at = utc_now_iso()
    if source_id in ('austria', 'italy', 'malaysia'):
        return additional_recent(source_id, config.item_limit_per_source)
    if source_id in ('argentinago', 'mexicogo'):
        return {'source': dict(HTML_SOURCES[source_id]),
                'items': national_archive(source_id)['items'][:config.item_limit_per_source]}
    if source_id == 'egj':
        return {'source': dict(EGJ_SOURCE), 'items': parse_egj_blog(
            fetch_text(EGJ_SOURCE['home_url']))[:config.item_limit_per_source]}
    if source_id == "egf":
        source = {
            "id": "egf",
            "name": "European Go Federation",
            "home_url": EGF_HOME_URL,
            "language": "en",
        }
        items = parse_egf_news_listing(fetch_text(EGF_HOME_URL), fetched_at=fetched_at, limit=config.item_limit_per_source)
    elif source_id == "nihonkiin":
        source = {
            "id": "nihonkiin",
            "name": "Nihon Ki-in",
            "home_url": "https://www.nihonkiin.or.jp/",
            "language": "ja",
        }
        items = parse_nihonkiin_news_listing(
            fetch_text(NIHONKIIN_NEWS_URL),
            fetched_at=fetched_at,
            limit=config.item_limit_per_source,
        )
    elif source_id == "kbaduk":
        source = {
            "id": "kbaduk",
            "name": "Korea Baduk Federation",
            "home_url": "https://www.kbaduk.or.kr/",
            "language": "ko",
        }
        items = parse_kbaduk_press_listing(fetch_text(KBADUK_PRESS_URL), fetched_at=fetched_at, limit=config.item_limit_per_source)
    elif source_id == "hangukkiwon":
        source = {
            "id": "hangukkiwon",
            "name": "Hanguk Kiwon",
            "home_url": "https://www.baduk.or.kr/",
            "language": "ko",
        }
        items = parse_hangukkiwon_report_listing(
            fetch_text(HANGUKKIWON_REPORT_URL),
            fetched_at=fetched_at,
            limit=config.item_limit_per_source,
        )
    elif source_id == "aigp":
        source = {
            "id": "aigp",
            "name": "Association of Indian Go Players",
            "home_url": "https://aigp.org.in/",
            "language": "en",
        }
        items = parse_aigp_news_listing(fetch_text(AIGP_NEWS_URL), fetched_at=fetched_at, limit=config.item_limit_per_source)
    elif source_id == "pgetc":
        source = {
            "id": "pgetc",
            "name": "Pandanet European Team Go Championship",
            "home_url": "https://pandanet-igs.com/communities/euroteamchamps",
            "language": "en",
        }
        items = parse_pgetc_news_listing(fetch_text(PGETC_NEWS_URL), fetched_at=fetched_at, limit=config.item_limit_per_source)
    elif source_id == "fedibergo":
        source = {
            "id": "fedibergo",
            "name": "Federacion Iberoamericana de Go",
            "home_url": FEDIBERGO_HOME_URL,
            "language": "es",
        }
        items = parse_fedibergo_news_listing(
            fetch_text(FEDIBERGO_HOME_URL),
            fetched_at=fetched_at,
            limit=config.item_limit_per_source,
        )
    elif source_id == "cwa":
        source = {
            "id": "cwa",
            "name": "Chinese Weiqi Association",
            "home_url": CWA_HOME_URL,
            "language": "zh-Hans",
        }
        records = fetch_cwa_news_records(limit=config.item_limit_per_source)
        items = parse_cwa_news_records(records, fetched_at=fetched_at, limit=config.item_limit_per_source)
    elif source_id in FEED_SOURCE_CONFIGS:
        feed_config = FEED_SOURCE_CONFIGS[source_id]
        source = feed_source_metadata(feed_config)
        items = parse_generic_feed(
            feed_config,
            fetch_text(str(feed_config["feed_url"])),
            fetched_at=fetched_at,
            limit=config.item_limit_per_source,
        )
    else:
        raise ValueError(f"Unsupported external news source: {source_id}")

    payload = {
        "source": source,
        "fetched_item_count": len(items),
        "items": [asdict(item) for item in items],
    }
    return payload
