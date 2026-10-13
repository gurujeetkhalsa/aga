"""Publisher archive pages. These return metadata, never substitute excerpts for text."""
from dataclasses import asdict
from datetime import datetime
import re
from urllib.error import HTTPError
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup
import legacy_sources as legacy
from source_discovery import EGJ_SOURCE, parse_egj_blog
from national_sources import national_archive
from additional_sources import additional_archive

EJ_URL = 'https://usgo.org/content.aspx?page_id=3&club_id=454497'
EGF = {'id': 'egf', 'name': 'European Go Federation', 'language': 'en',
       'home_url': 'https://www.eurogofed.org/', 'feed_url': 'https://www.eurogofed.org/feed/'}


def article(sid, name, lang, url, title, published, article_id=None):
    return {'source_id': sid, 'source_name': name, 'source_language': lang,
            'source_article_id': article_id or urlparse(url).path.strip('/'),
            'url': url, 'title': title, 'published_at': published}


def parse_ej_archive(raw):
    soup = BeautifulSoup(raw, 'lxml')
    items = {}
    for heading in soup.select('.card-title'):
        link = heading.select_one('a[href*="page_id=5"]')
        stamp = heading.parent.select_one('.card-date')
        if not link or not stamp:
            continue
        match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', stamp.get_text())
        if not match:
            raise ValueError('E-Journal archive publication date is missing')
        aid = parse_qs(urlparse(link['href']).query)['item_id'][0]
        url = f'https://usgo.org/content.aspx?page_id=5&club_id=454497&item_id={aid}&action=view'
        items[aid] = article('ej', 'AGA E-Journal', 'en', url, link.get_text(' ', strip=True),
                             datetime.strptime(match[1], '%m/%d/%Y').date().isoformat(), aid)
    if not items:
        raise ValueError('E-Journal archive returned no dated article cards')
    return list(items.values())


def parse_dated_cards(raw, sid):
    soup = BeautifulSoup(raw, 'html.parser')
    if sid == 'britgo':
        rows, selector, home, name, lang = (soup.select('tbody tr'), '.views-field-title a',
            'https://www.britgo.org/', 'British Go Association', 'en')
    else:
        rows, selector, home, name, lang = (soup.select('article[itemprop="blogPost"]'), 'h2 a',
            'https://www.fedibergo.org/', 'Federacion Iberoamericana de Go', 'es')
    items = []
    for row in rows:
        link, stamp = row.select_one(selector), row.select_one('time[datetime]')
        if link and stamp:
            items.append(article(sid, name, lang, urljoin(home, link['href']),
                                 link.get_text(' ', strip=True), stamp['datetime']))
    if not items:
        raise ValueError(f'{sid} archive returned no dated articles')
    return items


def archive_page(sid, cursor=None, upper_day=None):
    """Return one complete page and durable next cursor. Only explicit ends are terminal."""
    cursor = cursor or {}
    if sid in ('austria', 'italy', 'malaysia'):
        return additional_archive(sid, cursor)
    if sid in ('poland', 'argentinago', 'mexicogo'):
        return national_archive(sid, cursor)
    page = cursor.get('page', 1)
    fetched = legacy.utc_now_iso()
    next_cursor = {'page': page + 1}
    exhausted = False
    if sid == 'egj':
        items = parse_egj_blog(legacy.fetch_text(EGJ_SOURCE['home_url']))
        exhausted = True  # All public blog posts are listed in this one catalogue.
    elif sid == 'ej':
        items = parse_ej_archive(legacy.fetch_text(EJ_URL))
        exhausted = True  # The public page contains the entire dated card catalogue.
    elif sid == 'aigp':
        items = [asdict(i) for i in legacy.parse_aigp_news_listing(
            legacy.fetch_text(legacy.AIGP_NEWS_URL), fetched, 100000)]
        if not items:
            raise ValueError('AIGP news catalogue is empty')
        exhausted = True
    elif sid == 'nihonkiin':
        months = cursor.get('months')
        if months is None:
            soup = BeautifulSoup(legacy.fetch_text(legacy.NIHONKIIN_NEWS_URL), 'html.parser')
            months = sorted({urljoin(legacy.NIHONKIIN_NEWS_URL, a['href']) for a in soup.select('a[href]')
                             if re.search(r'/news/\d{4}/\d{2}/$', a['href'])
                             and re.search(r'/news/(\d{4}/\d{2})/', a['href'])[1].replace('/', '-') <= upper_day[:7]},
                            key=lambda u: re.search(r'/news/(\d{4}/\d{2})/', u)[1], reverse=True)
        if not months:
            raise ValueError('Nihon Ki-in archive month links unavailable')
        url = months[0]
        items = [asdict(i) for i in legacy.parse_nihonkiin_news_listing(legacy.fetch_text(url), fetched, 100000)]
        if not items:
            raise ValueError('Nihon Ki-in archive month could not be read: ' + url)
        next_cursor, exhausted = {'months': months[1:]}, len(months) == 1
    elif sid == 'cwa':
        data = legacy.cwa_response_data(legacy.post_form_json(legacy.CWA_API_BASE_URL + '/news/publish/list',
            {'page': page, 'pageSize': 50, 'newsChannel': 'web', 'newsType': 1}))
        if not isinstance(data, dict) or not isinstance(data.get('records'), list):
            raise ValueError('Chinese Weiqi Association archive response missing records')
        records = data['records']
        items = [asdict(i) for i in legacy.parse_cwa_news_records(records, fetched, 1000)]
        exhausted = not records or (data.get('pages') is not None and page >= int(data['pages']))
        if records and not items:
            raise ValueError('Chinese Weiqi Association archive dates could not be parsed')
    elif sid in ('britgo', 'fedibergo'):
        url = ('https://www.britgo.org/allnews?page=' + str(page - 1) if sid == 'britgo'
               else 'https://www.fedibergo.org/noticias?start=' + str((page - 1) * 12))
        raw = legacy.fetch_text(url)
        items = parse_dated_cards(raw, sid)
        soup = BeautifulSoup(raw, 'html.parser')
        param, current = ('page', page - 1) if sid == 'britgo' else ('start', (page - 1) * 12)
        pages = [int(v) for a in soup.select('a[href]')
                 for v in parse_qs(urlparse(a['href']).query).get(param, []) if v.isdigit()]
        exhausted = not any(p > current for p in pages)
    elif sid in ('kbaduk', 'hangukkiwon', 'pgetc'):
        if sid == 'kbaduk':
            url, parser = legacy.KBADUK_PRESS_URL + '?p=' + str(page), legacy.parse_kbaduk_press_listing
            raw = legacy.fetch_text(url)
            later = [int(x) for x in re.findall(r'[?&]p=(\d+)', raw)]
        elif sid == 'pgetc':
            url, parser = legacy.PGETC_NEWS_URL + '?page=' + str(page), legacy.parse_pgetc_news_listing
            raw = legacy.fetch_text(url)
            later = [int(x) for x in re.findall(r'[?&]page=(\d+)', raw)]
        else:
            parser = legacy.parse_hangukkiwon_report_listing
            request = Request(legacy.HANGUKKIWON_REPORT_URL, data=urlencode({'pageNo': page}).encode(),
                headers={'User-Agent': 'AGA-NewsSourcePrototype/0.1 (+https://www.usgo.org/)'})
            with urlopen(request, timeout=30) as response:
                raw = response.read().decode('utf-8')
            later = [int(x) for x in re.findall(r'pageMove_paging\((\d+)\)', raw)]
        items = [asdict(i) for i in parser(raw, fetched, 100000)]
        if not items:
            raise ValueError(f'{sid} archive returned no dated articles')
        exhausted = not any(p > page for p in later)
    else:
        config = EGF if sid == 'egf' else legacy.FEED_SOURCE_CONFIGS[sid]
        url = config['feed_url'] + ('?start-index=' + str(1 + (page - 1) * 25) + '&max-results=25&orderby=published'
              if sid == 'taiwanchi' else '?page=' + str(page - 1) if sid == 'gofed' else '?paged=' + str(page))
        try:
            raw = legacy.fetch_text(url)
        except HTTPError as error:
            if error.code == 404 and page > 1:
                return {'items': [], 'cursor': cursor, 'exhausted': True}
            raise
        root = ET.fromstring(raw)  # An HTML error page is never an empty feed.
        items = [asdict(i) for i in legacy.parse_generic_feed(config, raw, fetched, 100000)]
        if sid == 'taiwanchi':
            total = root.find('{http://a9.com/-/spec/opensearchrss/1.0/}totalResults')
            exhausted = total is not None and page * 25 >= int(total.text)
        if not items:
            if root.tag not in ('rss', '{http://www.w3.org/2005/Atom}feed'):
                raise ValueError('Archive did not return a news feed')
            # Filtered entries still count as a page, not the end of the archive.
            exhausted = not root.findall('.//item') and not root.findall('{http://www.w3.org/2005/Atom}entry')
    if any(not i.get('published_at') for i in items):
        raise ValueError(f'{sid} archive has undated articles; cursor retained for investigation')
    return {'items': items, 'cursor': next_cursor, 'exhausted': exhausted}
