"""Dated public catalogues for Austria, Italy, and Malaysia."""
from datetime import datetime
import re
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup
from legacy_sources import fetch_text, feed_article_id

ADDITIONAL_IDS = ('austria', 'italy', 'romania', 'malaysia')
SOURCES = {
    'austria': {'id': 'austria', 'name': 'Austrian Go Association', 'language': 'de',
                'home_url': 'https://goverband.at/wp2/'},
    'italy': {'id': 'italy', 'name': 'Italian Go Federation', 'language': 'it',
              'home_url': 'https://www.figg.org/'},
    'romania': {'id': 'romania', 'name': 'Romanian Go Federation', 'language': 'ro',
                'home_url': 'https://frgo.ro/', 'feed_url': 'https://frgo.ro/feed/'},
    'malaysia': {'id': 'malaysia', 'name': 'Malaysia Weiqi Association (archive)', 'language': 'en',
                 'home_url': 'https://www.weiqi.org.my/en/news', 'historical_archive': True},
}
ITALY_ARCHIVE = 'https://www.figg.org/old_site/la-figg/notizie'


def record(sid, url, title, published):
    config = SOURCES[sid]
    if published:
        datetime.fromisoformat(published.replace('Z', '+00:00'))
    key = feed_article_id(url, '')
    if sid == 'italy' and urlparse(url).path == '/articolo.php':
        key += '?id=' + parse_qs(urlparse(url).query)['id'][0]
    return {'source_id': sid, 'source_name': config['name'], 'source_language': config['language'],
            'source_article_id': key, 'url': url,
            'title': title, 'published_at': published}


def parse_austria(raw):
    soup = BeautifulSoup(raw, 'html.parser')
    items = {}
    for link in soup.select('.news-content .entry a[href]'):
        url = urljoin(SOURCES['austria']['home_url'], link['href'])
        match = re.fullmatch(r'/wp2/(\d{4})/(\d{2})/(\d{2})/[^/]+/', urlparse(url).path)
        if match and urlparse(url).hostname == 'goverband.at':
            items[url] = record('austria', url, link.get_text(' ', strip=True), '-'.join(match.groups()))
    if not items:
        raise ValueError('Austrian catalogue returned no dated article links')
    return list(items.values()), soup


def parse_malaysia(raw):
    soup = BeautifulSoup(raw, 'html.parser')
    items = []
    for row in soup.select('main article.archive-row'):
        link, stamp = row.select_one('h2 a[href]'), row.select_one('time[datetime]')
        if not link or not stamp:
            raise ValueError('Malaysian archive article title/date missing')
        url = urljoin(SOURCES['malaysia']['home_url'], link['href'])
        if not url.startswith('https://www.weiqi.org.my/en/news/'):
            raise ValueError('Unexpected Malaysian news link')
        items.append(record('malaysia', url, link.get_text(' ', strip=True), stamp['datetime']))
    if not items:
        raise ValueError('Malaysian news archive returned no dated articles')
    if soup.select('a[rel="next"], .pagination, .pager'):
        raise ValueError('Malaysian archive pagination needs an adapter update')
    return items


def italy_publication(raw):
    soup = BeautifulSoup(raw, 'html.parser')
    stamp = soup.select_one('time[itemprop="datePublished"][datetime]')
    if not stamp:
        raise ValueError('Italian legacy article publication date unavailable')
    return stamp['datetime']


def parse_italy_archive(raw, fetch=fetch_text):
    soup = BeautifulSoup(raw, 'html.parser')
    items = []
    for row in soup.select('tbody tr'):
        link = row.select_one('a[href*="/old_site/la-figg/notizie/"]')
        if not link:
            continue
        url = urljoin(SOURCES['italy']['home_url'], link['href'])
        # Catalogue columns contain creation dates. Use the article's explicit
        # publication date instead; the two differ for republished notices.
        items.append(record('italy', url, link.get_text(' ', strip=True), italy_publication(fetch(url))))
    if not items:
        raise ValueError('Italian legacy news catalogue returned no articles')
    return items, soup


def parse_italy_current(raw):
    soup = BeautifulSoup(raw, 'html.parser')
    items = []
    for row in soup.select('main .news-item'):
        link = row.select_one('a.n-name[href]')
        if not link:
            raise ValueError('Italian news title/link missing')
        url = urljoin(SOURCES['italy']['home_url'], link['href'])
        stamp = row.select_one('.n-date')
        text = stamp.get_text(' ', strip=True) if stamp else ''
        published = None
        if text not in ('', '—', '-'):
            published = datetime.strptime(text, '%d/%m/%Y').date().isoformat()
        items.append(record('italy', url, link.get_text(' ', strip=True), published))
    if not items:
        raise ValueError('Italian current news catalogue returned no articles')
    return items


def additional_archive(sid, cursor=None):
    cursor = cursor or {}
    page = cursor.get('page', 1)
    if sid == 'malaysia':
        items = parse_malaysia(fetch_text(SOURCES[sid]['home_url']))
        return {'items': items, 'cursor': {}, 'exhausted': True}
    if sid == 'austria':
        url = SOURCES[sid]['home_url'] + (f'page/{page}/' if page > 1 else '')
        items, soup = parse_austria(fetch_text(url))
        later = [int(m[1]) for a in soup.select('a[href]')
                 if (m := re.search(r'/wp2/page/(\d+)/', a['href'])) and int(m[1]) > page]
        return {'items': items, 'cursor': {'page': min(later) if later else page}, 'exhausted': not later}
    if sid == 'italy':
        start = cursor.get('start', 0)
        items, soup = parse_italy_archive(fetch_text(ITALY_ARCHIVE + f'?start={start}'))
        later = [int(value) for a in soup.select('a[href]')
                 for value in parse_qs(urlparse(a['href']).query).get('start', [])
                 if value.isdigit() and int(value) > start]
        return {'items': items, 'cursor': {'start': min(later) if later else start}, 'exhausted': not later}
    raise ValueError('Unsupported additional HTML source')


def additional_recent(sid, limit=100):
    items = additional_archive(sid)['items']
    if sid == 'italy':
        items += parse_italy_current(fetch_text(SOURCES[sid]['home_url'] + 'news.php'))
        # Featured stories can be absent from the category's first page.
        home = BeautifulSoup(fetch_text(SOURCES[sid]['home_url'] + 'old_site/'), 'html.parser')
        for row in home.select('.items-leading > div, .items-row .item'):
            link, stamp = row.select_one('h2 a[href]'), row.select_one('time[itemprop="datePublished"][datetime]')
            if link and stamp:
                items.append(record(sid, urljoin(SOURCES[sid]['home_url'], link['href']),
                                    link.get_text(' ', strip=True), stamp['datetime']))
        items = list({i['source_article_id']: i for i in items}.values())
    # Seed only a small head of Malaysia's historical catalogue; hourly archive
    # batches handle the remainder instead of treating 137 old articles as news.
    count = min(limit, 8) if sid == 'malaysia' else limit
    return {'source': dict(SOURCES[sid]),
            'items': sorted(items, key=lambda row: row.get('published_at') or '9999', reverse=True)[:count]}
