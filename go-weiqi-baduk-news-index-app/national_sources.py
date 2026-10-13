"""Public national-association catalogues with publisher dates and durable pagination."""
from datetime import datetime
import re
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup
from legacy_sources import fetch_text, feed_article_id

NEW_SOURCE_IDS = ('goweb', 'gofed', 'irishgo', 'poland', 'ufgo', 'argentinago', 'mexicogo', 'chilego')
HTML_SOURCES = {
    'argentinago': {'id': 'argentinago', 'name': 'Argentine Go Association', 'language': 'es',
                    'home_url': 'https://www.go.org.ar/index.php/noticias'},
    'mexicogo': {'id': 'mexicogo', 'name': 'Mexican Go Association', 'language': 'es',
                 'home_url': 'https://go.org.mx/blog'},
    'poland': {'id': 'poland', 'name': 'Go w Polsce (Poland)', 'language': 'pl',
               'home_url': 'https://go.art.pl/'},
}


def item(sid, url, title, published):
    config = HTML_SOURCES[sid]
    datetime.fromisoformat(published[:10])  # Reject missing or invented dates.
    return {'source_id': sid, 'source_name': config['name'], 'source_language': config['language'],
            'source_article_id': feed_article_id(url, ''), 'url': url, 'title': title,
            'published_at': published}


def later_pages(soup, parameter, current):
    return sorted({int(v) for a in soup.select('a[href]')
                   for v in parse_qs(urlparse(a['href']).query).get(parameter, [])
                   if v.isdigit() and int(v) > current})


def parse_argentina(raw):
    soup = BeautifulSoup(raw, 'html.parser')
    items = []
    for row in soup.select('article.item'):
        link, stamp = row.select_one('h2 a[href]'), row.select_one('time[datetime]')
        if not link or not stamp:
            raise ValueError('Argentine news article title/date unavailable')
        items.append(item('argentinago', urljoin(HTML_SOURCES['argentinago']['home_url'], link['href']),
                          link.get_text(' ', strip=True), stamp['datetime']))
    if not items:
        raise ValueError('Argentine news catalogue returned no articles')
    return items, soup


SPANISH_MONTHS = dict(zip(('enero febrero marzo abril mayo junio julio agosto septiembre octubre noviembre diciembre').split(), range(1, 13)))


def mexico_date(raw):
    soup = BeautifulSoup(raw, 'html.parser')
    for stamp in soup.select('main section span'):
        match = re.search(r'\b(\d{1,2}) de (\w+) de (\d{4})\b', stamp.get_text(' ', strip=True).lower())
        if match:
            return datetime(int(match[3]), SPANISH_MONTHS[match[2]], int(match[1])).date().isoformat()
    raise ValueError('Mexican article publication date unavailable')


def mexico_catalogue(raw, fetch=fetch_text):
    soup = BeautifulSoup(raw, 'html.parser')
    items = {}
    for card in soup.select('main section a[href^="/blog/"]'):
        title = card.select_one('h3')
        if not title:
            continue
        url = urljoin(HTML_SOURCES['mexicogo']['home_url'], card['href'])
        items[url] = item('mexicogo', url, title.get_text(' ', strip=True), mexico_date(fetch(url)))
    if not items:
        raise ValueError('Mexican news catalogue returned no articles')
    # Current public catalogue has no pagination controls.
    if soup.select('main .pagination, main [aria-label*="pagination"], main a[rel="next"], main button'):
        raise ValueError('Mexican catalogue pagination changed; reader needs updating')
    return sorted(items.values(), key=lambda i: i['published_at'], reverse=True)


def parse_poland_month(raw):
    soup = BeautifulSoup(raw, 'html.parser')
    items = []
    for row in soup.select('.node'):
        link, stamp = row.select_one('h2 a[href]'), row.select_one('.submitted')
        if not link or not stamp:
            raise ValueError('Polish archive title/date unavailable')
        date = re.search(r'\b\d{4}-\d{2}-\d{2}\b', stamp.get_text())
        if not date:
            raise ValueError('Polish archive publication date unavailable')
        items.append(item('poland', urljoin('https://go.art.pl/', link['href']), link.get_text(' ', strip=True), date[0]))
    if not items:
        raise ValueError('Published Polish archive month returned no articles')
    return items, soup


def national_archive(sid, cursor=None):
    cursor = dict(cursor or {})
    page = cursor.get('page', 1)
    if sid == 'argentinago':
        offset = (page - 1) * 10
        items, soup = parse_argentina(fetch_text(HTML_SOURCES[sid]['home_url'] + '?start=' + str(offset)))
        return {'items': items, 'cursor': {'page': page + 1}, 'exhausted': not later_pages(soup, 'start', offset)}
    if sid == 'mexicogo':
        return {'items': mexico_catalogue(fetch_text(HTML_SOURCES[sid]['home_url'])), 'cursor': {}, 'exhausted': True}
    # Traverse the publisher's paginated month catalogue, including mini-news.
    # Homepage and RSS have pinned old stories and cannot define archive boundaries.
    months = list(cursor.get('months', []))
    catalogue_page = cursor.get('catalogue_page', 0)
    catalogue_more = cursor.get('catalogue_more', True)
    if not months:
        raw = fetch_text('https://go.art.pl/archive?page=' + str(catalogue_page))
        soup = BeautifulSoup(raw, 'html.parser')
        months = sorted({a['href'] for a in soup.select('.view-archive a[href]')
                         if re.fullmatch(r'/archive/\d{6}', a['href'])}, reverse=True)
        if not months:
            raise ValueError('Polish archive month catalogue unavailable')
        catalogue_more = bool(later_pages(soup, 'page', catalogue_page))
        catalogue_page += 1
    month_page = cursor.get('month_page', 0)
    items, soup = parse_poland_month(fetch_text('https://go.art.pl' + months[0] + '?page=' + str(month_page)))
    if later_pages(soup, 'page', month_page):
        month_page += 1
    else:
        months.pop(0)
        month_page = 0
    return {'items': items, 'cursor': {'months': months, 'catalogue_page': catalogue_page,
            'catalogue_more': catalogue_more, 'month_page': month_page},
            'exhausted': not months and not catalogue_more}
