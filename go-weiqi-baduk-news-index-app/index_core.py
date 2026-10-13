"""Independent full-text news index. No writes to the E-Journal archive."""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import time
from dataclasses import asdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.error import HTTPError
from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup, Comment
import trafilatura
from legacy_sources import (fetch_text, fetch_json, post_json, cwa_response_data,
                            azure_translate_texts, AZURE_TRANSLATOR_URL, parse_generic_feed)
from source_discovery import fetch_external_source_payload, ExternalNewsSyncConfig
from translation_pacing import pace_translation
from translation_budget import BudgetDeferred, article_lane, lane_for, reserve_translation
from name_index import annotate_people, name_key
from tournament_index import annotate_tournaments, search_words
from national_sources import NEW_SOURCE_IDS
from additional_sources import ADDITIONAL_IDS, SOURCES as ADDITIONAL_SOURCES

SOURCE_IDS = ('ej', 'egf', 'nihonkiin', 'kbaduk', 'hangukkiwon', 'aigp', 'cwa',
              'kansaikiin', 'britgo', 'igf', 'singapore', 'taiwanchi',
              'australiango', 'thaigo', 'nagf', 'dgob', 'pgetc', 'fedibergo', 'rfg', 'egj') + NEW_SOURCE_IDS + ADDITIONAL_IDS
SOURCE_NAMES = dict(zip(SOURCE_IDS, ('AGA E-Journal', 'European Go Federation', 'Nihon Ki-in',
    'Korea Baduk Federation', 'Hanguk Kiwon', 'Association of Indian Go Players',
    'Chinese Weiqi Association', 'Kansai Ki-in', 'British Go Association',
    'International Go Federation', 'Singapore Weiqi Association', 'Taiwan Chi Yuan',
    'Australian Go Association', 'Thailand Go Association', 'North American Go Federation',
    'Deutscher Go-Bund', 'Pandanet European Team Go Championship', 'Federacion Iberoamericana de Go',
    'Revue Française de Go', 'European Go Journal', 'Czech Go Association (Goweb)',
    'Belgian Go Federation', 'Irish Go Association', 'Go w Polsce (Poland)',
    'Ukrainian Go Federation', 'Argentine Go Association', 'Mexican Go Association', 'Chilean Go Federation')))
SOURCE_NAMES.update({sid: config['name'] for sid, config in ADDITIONAL_SOURCES.items()})
SELECTORS = {
    'austria': '.news-content .single-entry', 'italy': 'main .art-body, [itemprop="articleBody"]',
    'romania': 'article.type-post .post-content', 'malaysia': 'main .archive-body',
    'goweb': 'article .post-content', 'gofed': '.node > .content',
    'irishgo': 'main .entry-content', 'poland': '.node > .content',
    'ufgo': 'article .entry-content', 'argentinago': 'article [itemprop="articleBody"]',
    'mexicogo': 'article .prose', 'chilego': 'article .entry-content',
    'rfg': '.post-content .inner-article-content', 'egj': 'main article > .content',
    'ej': '.news-text', 'nihonkiin': '.entry_body', 'kbaduk': '#viewLayerout',
    'taiwanchi': '.post-body', 'britgo': '.field--name-body, .field-name-body',
    'kansaikiin': '.entry-content', 'igf': '.entry-content',
    'singapore': '.entry-content', 'australiango': '.entry-content',
    'thaigo': '.entry-content', 'nagf': '.entry-content', 'dgob': '.entry-content',
}

def now():
    return datetime.now(timezone.utc).isoformat()


def eastern_publication_date(value):
    """Use Eastern dates for timestamped articles; preserve publisher date-only values."""
    if not value:
        return ''
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if stamp.tzinfo is not None:
            stamp = stamp.astimezone(ZoneInfo('America/New_York'))
        return stamp.date().isoformat()
    except (ValueError, TypeError):
        return ''

def identity(item):
    return hashlib.sha256((item['source_id'] + ':' + str(item.get('source_article_id') or item['url'])).encode()).hexdigest()[:32]

def discover(source_id, limit=100):
    if source_id == 'egf':
        config = {'id': 'egf', 'name': 'European Go Federation', 'language': 'en',
                  'home_url': 'https://www.eurogofed.org/', 'feed_url': 'https://www.eurogofed.org/feed/'}
        return {'source': config, 'items': [asdict(i) for i in parse_generic_feed(
            config, fetch_text(config['feed_url']), now(), limit)]}
    if source_id != 'ej':
        return fetch_external_source_payload(source_id, ExternalNewsSyncConfig(limit), translate=False)
    markup = fetch_text('https://usgo.org/content.aspx?page_id=3&club_id=454497')
    ids = list(dict.fromkeys(re.findall(r'(?i)item_id=(\d+)', markup)))[:limit]
    if not ids:
        raise ValueError('No E-Journal article links found')
    return {'source': {'id': 'ej', 'name': 'AGA E-Journal', 'language': 'en'}, 'items': [
        {'source_id': 'ej', 'source_name': 'AGA E-Journal', 'source_language': 'en',
         'source_article_id': id_, 'title': '', 'published_at': None,
         'url': f'https://usgo.org/content.aspx?page_id=5&club_id=454497&item_id={id_}&action=view'} for id_ in ids]}

def extract_body(markup, source_id, url=''):
    soup = BeautifulSoup(markup, 'html.parser')
    for tag in soup.select('script,style,noscript,nav,footer,.sharedaddy,.share-buttons'):
        tag.decompose()
    selector = SELECTORS.get(source_id)
    root = soup.select_one(selector) if selector else None
    if source_id in ('ej', 'rfg', 'egj') + NEW_SOURCE_IDS + ADDITIONAL_IDS and root is None:
        raise ValueError('Public article body unavailable for ' + source_id)
    if root is not None:
        # E-Journal author/date metadata is not an article mention. It normally
        # sits outside .news-text; explicitly exclude it if nested there too.
        # Keep the original soup intact for publication-date extraction.
        body_root = BeautifulSoup(str(root), 'html.parser') if source_id == 'ej' else root
        if source_id == 'ej':
            for byline in body_root.select('.news-author'):
                byline.decompose()
        if source_id == 'austria':
            for noise in body_root.select('.post-meta, .post-navigation'):
                noise.decompose()
        if source_id == 'egj':
            # Newsletter bylines precede the prose. Preserve author mentions in the body.
            first = body_root.find(['p', 'i', 'em'])
            if first and re.match(r'^Written by\b', first.get_text(' ', strip=True), re.I):
                first.decompose()
        if source_id in NEW_SOURCE_IDS:
            for noise in body_root.select('.fb-social-like-widget, .sharedaddy, .post-tags, .wp-block-post-author'):
                noise.decompose()
        body = body_root.get_text('\n', strip=True)
        method = 'publisher article container'
    else:
        body = trafilatura.extract(str(soup), url=url, include_comments=False,
                                   include_tables=True, favor_recall=True) or ''
        method = 'article text extraction'
    if len(body.strip()) < 40:
        raise ValueError('Article text missing or too short to verify')
    return body.strip(), method, soup

def ej_article_html(markup, url):
    """Keep publisher prose, images and links without executable page content."""
    soup = BeautifulSoup(markup, 'html.parser')
    for tag in soup.select('script,style,noscript,iframe,object,embed,form,svg,math,template'):
        tag.decompose()
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    allowed = {'p', 'div', 'span', 'br', 'hr', 'strong', 'b', 'em', 'i', 'u', 's',
               'blockquote', 'ul', 'ol', 'li', 'h3', 'h4', 'h5', 'h6', 'a', 'img',
               'figure', 'figcaption', 'table', 'thead', 'tbody', 'tr', 'th', 'td',
               'sup', 'sub', 'pre', 'code'}
    for tag in list(soup.find_all(True)):
        if tag.name not in allowed:
            tag.unwrap()
            continue
        attrs = dict(tag.attrs)
        tag.attrs = {}
        if tag.name in ('a', 'img'):
            key = 'href' if tag.name == 'a' else 'src'
            target = urljoin(url, str(attrs.get(key, '')).strip())
            if attrs.get(key) and urlparse(target).scheme in ('http', 'https'):
                tag[key] = target
                if tag.name == 'a':
                    tag['target'] = '_blank'
                    tag['rel'] = 'noopener noreferrer'
                else:
                    tag['loading'] = 'lazy'
                    tag['referrerpolicy'] = 'no-referrer'
            elif tag.name == 'img':
                tag.decompose()
                continue
            for key in ('alt', 'title'):
                if key in attrs:
                    tag[key] = str(attrs[key])
        for key in ('colspan', 'rowspan'):
            if tag.name in ('th', 'td') and str(attrs.get(key, '')).isdigit():
                tag[key] = str(min(100, int(attrs[key])))
    return str(soup)


def fetch_article(item):
    if urlparse(item['url']).hostname in ('www.instagram.com', 'instagram.com', 'www.facebook.com', 'facebook.com'):
        raise ValueError('Social-media post text is unavailable to this public article reader')
    if item['source_id'] == 'cwa':
        data = cwa_response_data(fetch_json('https://wqapi.cwql.org.cn/news/publish/' + str(item['source_article_id'])))
        markup = data.get('newsContent', '') if isinstance(data, dict) else ''
        if not markup:
            raise ValueError('Full Chinese article body unavailable')
        body = BeautifulSoup(markup, 'html.parser').get_text('\n', strip=True)
        if len(body) < 40:
            raise ValueError('Chinese article body too short to verify')
        return dict(item), body, 'publisher article API'
    body, method, soup = extract_body(fetch_text(item['url']), item['source_id'], item['url'])
    item = dict(item)
    if item['source_id'] == 'ej':
        title = soup.select_one('h2.zero-top-margin')
        if title is None:
            raise ValueError('E-Journal article title unavailable')
        item['title'] = title.get_text(' ', strip=True)
        author = soup.select_one('.news-author')
        line = author.get_text(' ', strip=True) if author else ''
        item['ej_byline'] = line
        item['ej_html'] = ej_article_html(str(soup.select_one('.news-text')), item['url'])
        match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', line)
        if match:
            item['published_at'] = datetime.strptime(match[1], '%m/%d/%Y').date().isoformat()
    body = clean_article_text(item, body)
    return item, body, method

def clean_article_text(item, body):
    if item['source_id'] == 'romania':
        body = body.replace('Change the color to match your brand or vision, add your logo, choose the perfect layout, modify menu settings and more.', '').strip()
    if item['source_id'] == 'aigp':
        body = body.replace('Welcome to the Association of Indian Go Players!', '').strip()
    if item['source_id'] == 'singapore':
        body = '\n'.join(line for line in body.splitlines() if line.strip() not in ('MY ACCOUNT', 'LOGIN', 'WhatsApp us'))
    remaining = body.replace(item.get('title', ''), '').strip()
    if len(remaining) < 40:
        raise ValueError('Article has no verifiable body text beyond its headline')
    return body

def text_chunks(text, size=4000):
    # Every character is retained, including the tail of long articles.
    chunks = []
    while len(text) > size:
        boundary = max(text.rfind('\n', size // 2, size), text.rfind(' ', size // 2, size),
                       text.rfind('。', size // 2, size))
        end = boundary + 1 if boundary >= 0 else size
        chunks.append(text[:end])
        text = text[end:]
    if text:
        chunks.append(text)
    return chunks

def translate_full(text, language, translate=None, cache=None):
    cache = cache if cache is not None else {}
    chunks = text_chunks(text)
    out = []
    for chunk in chunks:
        key = hashlib.sha256((language + '\0' + chunk).encode()).hexdigest()
        result = cache.get(key)
        if not result:
            result = translate(chunk, language) if translate else translate_chunk(chunk, language)
        if not result or not result.strip():
            raise ValueError('Translation returned an empty segment')
        cache[key] = result
        out.append(result)
    return '\n'.join(out)

def translate_chunk(chunk, language):
    key = os.environ.get('AZURE_TRANSLATOR_KEY')
    if not key:
        raise ValueError('Full-text translation is not configured')
    for attempt in range(4):
        try:
            pace_translation(len(chunk))
            reserve_translation(chunk)
            result = azure_translate_texts([chunk], key, os.environ.get('AZURE_TRANSLATOR_REGION', ''),
                                          AZURE_TRANSLATOR_URL, language, 'en')
            return result[0] if len(result) == 1 else ''
        except (HTTPError, RuntimeError) as error:
            http_error = error if isinstance(error, HTTPError) else error.__cause__
            code = getattr(http_error, 'code', None)
            if code not in (429, 500, 502, 503, 504) or attempt == 3:
                raise
            retry_header = getattr(http_error, 'headers', {}).get('Retry-After', '')
            delay = int(retry_header) if retry_header.isdigit() else 60
            time.sleep(min(60, max(5, delay)))

def detect_article_language(body):
    """Belgian and Ukrainian archives contain articles in multiple languages."""
    key = os.environ.get('AZURE_TRANSLATOR_KEY')
    if not key:
        raise ValueError('Article language detection is not configured')
    headers = {'Ocp-Apim-Subscription-Key': key, 'Content-Type': 'application/json; charset=UTF-8'}
    if os.environ.get('AZURE_TRANSLATOR_REGION'):
        headers['Ocp-Apim-Subscription-Region'] = os.environ['AZURE_TRANSLATOR_REGION']
    sample = body[:4000]
    # Conservatively reserve detection text too; this also prevents requests
    # while this month's allowance is paused.
    reserve_translation(sample)
    pace_translation(len(sample))
    result = post_json('https://api.cognitive.microsofttranslator.com/detect?api-version=3.0',
                       [{'Text': sample}], headers=headers)
    if not result or not result[0].get('isTranslationSupported') or not result[0].get('language'):
        raise ValueError('Article language could not be verified')
    return result[0]['language']


def index_item(item, previous=None, fetch=fetch_article, translate=None):
    previous = previous or {}
    record = dict(previous)
    record.update(item)
    record['translation_cache'] = dict(previous.get('translation_cache', {}))
    record.update(id=identity(item), checked_at=now(), first_seen=previous.get('first_seen') or now())
    record.pop('budget_deferred', None)
    lane_token = article_lane.set(lane_for(record))
    try:
        item, body, method = fetch(item)
        record.update(item)
        record.pop('budget_deferred', None)
        record['translation_cache'] = dict(record.get('translation_cache', {}))
        article_lane.set(lane_for(record))
        # Retain fetched text even when language detection or allowance defers work.
        record.update(body_original=body, original_chars=len(body), extraction_method=method)
        digest = hashlib.sha256((item['title'] + '\n' + body).encode()).hexdigest()
        if digest == previous.get('body_hash') and previous.get('status') == 'ready':
            # Feed parsers supply empty helper fields; do not let a routine
            # refresh erase a complete translated title from the index.
            title_en = previous.get('title_en')
            if not title_en:
                language = previous.get('article_language') or item.get('source_language', 'en')
                title_en = item['title'] if language.startswith('en') else translate_full(
                    item['title'], language, translate, record['translation_cache'])
            record.update(title_en=title_en, status='ready', last_error=None)
            return annotate_tournaments(annotate_people(record))
        language = item.get('source_language', 'en')
        if item['source_id'] in ('gofed', 'ufgo'):
            language_hash = hashlib.sha256(body.encode()).hexdigest()
            language = previous.get('article_language') if previous.get('language_body_hash') == language_hash else None
            language = language or detect_article_language(body)
            record['language_body_hash'] = language_hash
        if language.startswith('en'):
            # Some nominally English feeds publish complete Chinese-language posts.
            for pattern, detected in ((r'[\uac00-\ud7af]', 'ko'), (r'[\u3040-\u30ff]', 'ja'),
                                      (r'[\u0e00-\u0e7f]', 'th'), (r'[\u4e00-\u9fff]', 'zh-Hans')):
                if len(re.findall(pattern, body)) > max(20, len(body) * .15):
                    language = detected
                    break
        record['article_language'] = language
        record.update(body_original=body, original_chars=len(body), extraction_method=method)
        # Translations are cached by the complete source text, never the feed excerpt.
        title_en = item['title'] if language.startswith('en') else translate_full(item['title'], language, translate, record['translation_cache'])
        body_en = body if language.startswith('en') else translate_full(body, language, translate, record['translation_cache'])
        record.update(body_original=body, body_en=body_en, title_en=title_en, body_hash=digest,
                      status='ready', last_error=None, extraction_method=method, indexed_at=now(),
                      translated=not language.startswith('en'), original_chars=len(body), english_chars=len(body_en))
    except Exception as error:
        if isinstance(error, BudgetDeferred):
            record['budget_deferred'] = True
        # Previously indexed text stays searchable if a later refresh fails.
        record['status'] = 'ready' if previous.get('status') == 'ready' else 'pending'
        if previous.get('status') == 'ready':
            for field in ('title_en', 'body_original', 'body_en', 'body_hash', 'indexed_at',
                          'translated', 'original_chars', 'english_chars', 'article_language', 'extraction_method',
                          'ej_html', 'ej_byline'):
                if field in previous:
                    record[field] = previous[field]
        record['last_error'] = f'{type(error).__name__}: {str(error)[:200]}'
    finally:
        article_lane.reset(lane_token)
    return annotate_tournaments(annotate_people(record))

def sync_source(source_id, previous=None, limit=100, max_items=None, progress=None, checkpoint=None):
    previous = previous or {}
    records = {r['id']: r for r in previous.get('items', [])}
    result = dict(previous, source_id=source_id, checked_at=now(), error=None)
    result.setdefault('source', {'id': source_id, 'name': SOURCE_NAMES[source_id]})
    try:
        payload = discover(source_id, limit)
        if not payload['items']:
            raise ValueError('Source returned no article links')
        result['source'] = payload['source']
        result['discovered'] = len(payload['items'])
        candidates = list(payload['items'])
        found_ids = {identity(i) for i in candidates}
        # Retry incomplete articles even after they disappear from a publisher's feed.
        candidates += sorted([r for r in records.values() if r.get('status') != 'ready' and r['id'] not in found_ids],
                             key=lambda r: r.get('checked_at', ''))[:10]
        if max_items:
            candidates = candidates[:max_items]
        for item in candidates:
            key = identity(item)
            records[key] = index_item(item, records.get(key))
            if progress:
                progress(records[key])
            if checkpoint:
                result['items'] = list(records.values())
                result['ready'] = sum(r.get('status') == 'ready' for r in records.values())
                result['pending'] = len(records) - result['ready']
                checkpoint(result)
        result['last_success'] = now()
        # Publish membership only after a completed recent-source run. Historical
        # retries are not members; backfill preserves this source-level snapshot.
        if source_id == 'ej':
            result['latest_daily'] = {
                'completed_at': result['last_success'],
                'article_ids': sorted(found_ids & {identity(i) for i in candidates}),
            }
    except Exception as error:
        result['error'] = f'{type(error).__name__}: {str(error)[:200]}'
    result['items'] = list(records.values())
    result['ready'] = sum(r.get('status') == 'ready' for r in records.values())
    result['pending'] = len(records) - result['ready']
    return result

def article_summary(body, title='', max_chars=500):
    """A bounded extractive summary from the English article's opening sentences."""
    lines = [re.sub(r'\s+', ' ', line).strip() for line in body.splitlines() if line.strip()]
    # Skip a repeated headline and short leading headings, bylines, or dates.
    title_key = re.sub(r'\W+', '', title).casefold()
    while lines and re.sub(r'\W+', '', lines[0]).casefold() == title_key:
        lines.pop(0)
    lead = next((i for i, line in enumerate(lines)
                 if len(line) >= 80 or (len(line) >= 40 and re.search(r'[.!?][\u201d\u2019"\']?$', line))), 0)
    text = ' '.join(lines[lead:])
    # Protect common abbreviations before locating sentence boundaries.
    protected = re.sub(r'\b(?:Mr|Mrs|Ms|Dr|Prof|St|Jr|Sr)\.',
                       lambda m: m[0].replace('.', '\u0000'), text)
    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z0-9\u201c"\'])', protected)
    selected = []
    for sentence in sentences[:3]:
        sentence = sentence.replace('\u0000', '.')
        if selected and re.fullmatch(r'\d+\.', sentence):
            break
        if selected and len(' '.join(selected + [sentence])) > max_chars:
            break
        selected.append(sentence)
        if len(' '.join(selected)) >= 300:
            break
    summary = ' '.join(selected)
    if len(summary) > max_chars:
        summary = summary[:max_chars - 1].rsplit(' ', 1)[0].rstrip(' ,;:') + '\u2026'
    return summary


def build_database(sources):
    db = sqlite3.connect(':memory:', check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.execute('CREATE TABLE articles(id TEXT PRIMARY KEY, source_id TEXT, source_name TEXT, title TEXT, url TEXT, published TEXT, translated INT, status TEXT, checked_at TEXT, summary TEXT, full_text TEXT, full_html TEXT, byline TEXT, daily_collection_at TEXT, published_day TEXT)')
    db.execute("CREATE VIRTUAL TABLE fulltext USING fts5(id UNINDEXED,title,body,tokenize='unicode61 remove_diacritics 2')")
    db.execute('CREATE TABLE people(article_id TEXT, name_key TEXT, name TEXT, PRIMARY KEY(article_id,name_key))')
    db.execute('CREATE INDEX people_key ON people(name_key)')
    db.execute('CREATE TABLE tournaments(article_id TEXT, event_key TEXT, name TEXT, search_text TEXT, series INT, PRIMARY KEY(article_id,event_key))')
    db.execute('CREATE INDEX tournaments_key ON tournaments(event_key)')
    for source in sources:
        daily = source.get('latest_daily', {})
        daily_ids = set(daily.get('article_ids', []))
        for item in source.get('items', []):
            if urlparse(item.get('url', '')).scheme not in ('http', 'https'):
                continue
            title = item.get('title_en') or item.get('title') or 'Article awaiting indexing'
            summary = article_summary(item.get('body_en') or '', title) if item.get('status') == 'ready' else ''
            show_full = (source.get('source_id') == 'ej' and item['source_id'] == 'ej'
                         and item['id'] in daily_ids and item.get('status') == 'ready')
            db.execute('INSERT OR IGNORE INTO articles VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (item['id'], item['source_id'], item.get('source_name', item['source_id']),
                        item.get('title_en') or item.get('title') or 'Article awaiting indexing', item['url'],
                        item.get('published_at') or '', bool(item.get('translated')), item.get('status', 'pending'), item.get('checked_at', ''), summary,
                        item.get('body_en', '') if show_full else '',
                        ej_article_html(item['ej_html'], item['url']) if show_full and item.get('ej_html') else '',
                        item.get('ej_byline', '') if show_full else '',
                        daily.get('completed_at', '') if show_full else '',
                        eastern_publication_date(item.get('published_at'))))
            if item.get('status') == 'ready':
                db.execute('INSERT INTO fulltext VALUES(?,?,?)', (item['id'], item.get('title_en') or item['title'], item['body_en']))
                for person in item.get('people', []):
                    db.execute('INSERT OR IGNORE INTO people VALUES(?,?,?)',
                               (item['id'], person['key'], person['name']))
                for event in item.get('tournaments', []):
                    searchable = ' '.join(search_words(' '.join([event['name']] + event.get('aliases', []))))
                    db.execute('INSERT OR IGNORE INTO tournaments VALUES(?,?,?,?,?)',
                               (item['id'], event['key'], event['name'], searchable, bool(event.get('series'))))
    db.commit()
    return db

def selected_sources(source=''):
    """Empty/legacy requests mean all; an explicit 'none' means no sources."""
    if not source or source == 'all':
        return None
    if source == 'none':
        return []
    if len(source) > 500:
        raise ValueError('Too many sources.')
    values = list(dict.fromkeys(value.strip() for value in source.split(',') if value.strip()))
    if not values or any(value not in SOURCE_IDS for value in values):
        raise ValueError('Unknown source selection.')
    return values


def directory_article_scope(source='', start='', end=''):
    conditions, params = [], []
    sources = selected_sources(source)
    if sources is not None:
        conditions.append('source_id IN (' + ','.join('?' for _ in sources) + ')' if sources else '0')
        params.extend(sources)
    for value, comparison in ((start, '>='), (end, '<=')):
        if value:
            datetime.strptime(value, '%Y-%m-%d')
            conditions.append(f'published_day{comparison}?')
            params.append(value)
    if not conditions:
        return [], []
    return ['article_id IN (SELECT id FROM articles WHERE ' + ' AND '.join(conditions) + ')'], params


def query_names(db, name='', offset=0, limit=20, source='', start='', end=''):
    if len(name) > 100:
        raise ValueError('Please use a shorter name.')
    key = name_key(name)
    conditions = ['name_key LIKE ?'] if key else []
    params = ['%' + key + '%'] if key else []
    article_conditions, article_params = directory_article_scope(source, start, end)
    conditions.extend(article_conditions)
    params.extend(article_params)
    clause = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
    if name.strip() and not key:
        return {'total': 0, 'items': [], 'offset': offset, 'limit': limit}
    total = db.execute('SELECT count(DISTINCT name_key) FROM people' + clause, params).fetchone()[0]
    rows = db.execute('SELECT name_key AS key, min(name) AS name, count(*) AS articles FROM people' +
                      clause + ' GROUP BY name_key ORDER BY articles DESC,name COLLATE NOCASE,name_key LIMIT ? OFFSET ?',
                      params + [limit, max(0, offset)]).fetchall()
    return {'total': total, 'items': [dict(row) for row in rows], 'offset': offset, 'limit': limit}


def query_tournaments(db, tournament='', offset=0, limit=20, source='', start='', end=''):
    if len(tournament) > 200:
        raise ValueError('Please use a shorter tournament name.')
    words = search_words(tournament)
    if tournament.strip() and not words:
        return {'total': 0, 'items': [], 'offset': offset, 'limit': limit}
    conditions = ['search_text LIKE ?' for _ in words]
    params = ['%' + word + '%' for word in words]
    article_conditions, article_params = directory_article_scope(source, start, end)
    conditions.extend(article_conditions)
    params.extend(article_params)
    clause = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
    total = db.execute('SELECT count(DISTINCT event_key) FROM tournaments' + clause, params).fetchone()[0]
    rows = db.execute('SELECT event_key AS key,min(name) AS name,count(*) AS articles,max(series) AS series FROM tournaments' +
                      clause + ' GROUP BY event_key ORDER BY articles DESC,name COLLATE NOCASE,event_key LIMIT ? OFFSET ?',
                      params + [limit, max(0, offset)]).fetchall()
    items = [dict(row) for row in rows]
    for item in items:
        item['scope'] = SOURCE_NAMES.get(item['key'].split(':')[0], '') if not item['series'] else ''
    return {'total': total, 'items': items, 'offset': offset, 'limit': limit}


def query_index(db, q='', source='', start='', end='', offset=0, limit=20, name='', person='', tournament='', tournament_id='', sort='date'):
    if sort not in ('date', 'source'):
        raise ValueError('Choose Date or Source for sorting.')
    if len(q) > 300:
        raise ValueError('Please use a search of 300 characters or fewer.')
    terms = re.findall(r'\w+', q, re.UNICODE)
    if q.strip() and not terms:
        raise ValueError('Enter at least one word to search.')
    where = []
    params = []
    if len(name) > 100 or len(person) > 100:
        raise ValueError('Please use a shorter name.')
    if len(tournament) > 200 or len(tournament_id) > 220:
        raise ValueError('Please use a shorter tournament name.')
    join = ''
    snippet = "''"
    if terms:
        join = ' JOIN fulltext f ON f.id=a.id'
        where.append('fulltext MATCH ?')
        params.append(' AND '.join('"' + term + '"' for term in terms))
        snippet = "snippet(fulltext,2,'','',' … ',36)"
    sources = selected_sources(source)
    if sources is not None:
        where.append('a.source_id IN (' + ','.join('?' for _ in sources) + ')' if sources else '0')
        params.extend(sources)
    if name.strip() or person:
        key = name_key(person or name)
        where.append('EXISTS (SELECT 1 FROM people p WHERE p.article_id=a.id AND p.name_key ' +
                     ('= ?)' if person else 'LIKE ?)'))
        params.append((key if person else '%' + key + '%') if key else '!no-name!')
    if tournament_id:
        where.append('EXISTS (SELECT 1 FROM tournaments t WHERE t.article_id=a.id AND t.event_key=?)')
        params.append(tournament_id)
    elif tournament.strip():
        words = search_words(tournament)
        if words:
            where.append('EXISTS (SELECT 1 FROM tournaments t WHERE t.article_id=a.id AND ' +
                         ' AND '.join('t.search_text LIKE ?' for _ in words) + ')')
            params.extend('%' + word + '%' for word in words)
        else:
            where.append('0')
    for value, comparison in ((start, '>='), (end, '<=')):
        if value:
            datetime.strptime(value, '%Y-%m-%d')
            where.append(f'a.published_day{comparison}?'); params.append(value)
    clause = (' WHERE ' + ' AND '.join(where)) if where else ''
    base = ' FROM articles a' + join + clause
    total = db.execute('SELECT count(*)' + base, params).fetchone()[0]
    order = ('a.source_name COLLATE NOCASE,a.source_id,' if sort == 'source' else '')
    order += 'a.published_day DESC,a.published DESC,a.id'
    rows = db.execute('SELECT a.*, ' + snippet + ' AS snippet' + base +
                      ' ORDER BY ' + order + ' LIMIT ? OFFSET ?', params + [limit, max(0, offset)]).fetchall()
    items = [dict(row) for row in rows]
    for item in items:
        item['tournaments'] = [dict(row) for row in db.execute(
            'SELECT event_key AS key,name,series FROM tournaments WHERE article_id=? ORDER BY name COLLATE NOCASE', (item['id'],))]
        # Keyword passages stay short, including for articles displayed in full.
        passage = item['snippet']
        match = re.search(r'\b(?:' + '|'.join(re.escape(term) for term in terms) + r')\b',
                          passage, re.IGNORECASE) if terms else None
        if len(passage) > 240 and match and match.end() > 237:
            start_at = max(0, match.start() - 80)
            passage = '\u2026 ' + passage[start_at:]
        item['snippet'] = passage if len(passage) <= 240 else passage[:237].rstrip() + ' \u2026'
    return {'total': total, 'items': items, 'offset': offset, 'limit': limit}
