"""Public search, daily source discovery, and isolated per-source collection."""
import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import List

import azure.functions as func
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from index_core import SOURCE_IDS, SOURCE_NAMES, build_database, query_index, query_names, query_tournaments, sync_source
from history_backfill import backfill_source, public_backfill, backfill_run_key
from translation_budget import translation_job

app = func.FunctionApp()
CONTAINER = 'go-news-index'
_lock = threading.RLock()
_cache = {'time': 0, 'db': None, 'sources': []}

def container():
    return BlobServiceClient.from_connection_string(os.environ['AzureWebJobsStorage']).get_container_client(CONTAINER)

@contextmanager
def source_lease(blob):
    lease = blob.acquire_lease(lease_duration=60)
    stop = threading.Event()
    def renew():
        while not stop.wait(20):
            try:
                lease.renew()
            except Exception:
                logging.exception('Source collection lease renewal failed')
                return
    worker = threading.Thread(target=renew, daemon=True)
    worker.start()
    try:
        yield lease
    finally:
        stop.set()
        worker.join(timeout=5)
        lease.release()

def load_sources():
    if os.environ.get('NEWS_INDEX_LOCAL_DATA'):
        data = Path(os.environ['NEWS_INDEX_LOCAL_DATA'])
        return [json.loads((data / f'{sid}.json').read_text(encoding='utf-8'))
                if (data / f'{sid}.json').exists() else {'source_id': sid, 'items': []}
                for sid in SOURCE_IDS]
    client = container()
    sources = []
    for sid in SOURCE_IDS:
        try:
            sources.append(json.loads(client.download_blob(f'sources/{sid}.json').readall()))
        except ResourceNotFoundError:
            sources.append({'source_id': sid, 'ready': 0, 'pending': 0, 'error': 'First collection pending', 'items': []})
    return sources

def snapshot():
    with _lock:
        if _cache['db'] is None or time.monotonic() - _cache['time'] > 60:
            sources = load_sources()
            db = build_database(sources)
            old = _cache['db']
            _cache.update(db=db, sources=sources, time=time.monotonic())
            if old:
                old.close()
        return _cache

def response(data, status=200):
    return func.HttpResponse(json.dumps(data, ensure_ascii=False), status_code=status,
                             mimetype='application/json', headers={'Cache-Control': 'no-store'})

@app.route(route='news', auth_level=func.AuthLevel.ANONYMOUS)
def news(req: func.HttpRequest):
    page = Path(__file__).with_name('index.html').read_text(encoding='utf-8')
    if os.environ.get('NEWS_INDEX_LOCAL_DATA'):
        page = page.replace('<main class="wrap">', '<main class="wrap"><p class="quiet">Preview · Daily updates and live translations are not active yet.</p>')
        page = page.replace('Checked daily · Newest first', 'Recent collection · Newest first')
    return func.HttpResponse(page, mimetype='text/html',
                             headers={'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'strict-origin-when-cross-origin',
                                      'Content-Security-Policy': "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; img-src 'self' data: https:; frame-ancestors 'self'"})

@app.route(route='news/search', auth_level=func.AuthLevel.ANONYMOUS)
def search(req: func.HttpRequest):
    try:
        with _lock:
            snap = snapshot()
            result = query_index(snap['db'], req.params.get('q', ''), req.params.get('source', ''),
                                 req.params.get('start', ''), req.params.get('end', ''),
                                 max(0, int(req.params.get('offset', '0'))),
                                 name=req.params.get('name', ''), person=req.params.get('person', ''),
                                 tournament=req.params.get('tournament', ''), tournament_id=req.params.get('tournament_id', ''),
                                 sort=req.params.get('sort', 'date'))
        return response(result)
    except (ValueError, OverflowError):
        return response({'error': 'Please check the keywords and dates, then try again.'}, 400)
    except Exception:
        logging.exception('News search unavailable')
        return response({'error': 'The index is temporarily unavailable. Please try again shortly.'}, 503)


@app.route(route='news/names', auth_level=func.AuthLevel.ANONYMOUS)
def names(req: func.HttpRequest):
    try:
        with _lock:
            result = query_names(snapshot()['db'], req.params.get('name', ''),
                                 max(0, int(req.params.get('offset', '0'))), source=req.params.get('source', ''),
                                 start=req.params.get('start', ''), end=req.params.get('end', ''))
        return response(result)
    except (ValueError, OverflowError):
        return response({'error': 'Please check the name and dates, then try again.'}, 400)
    except Exception:
        logging.exception('Name index unavailable')
        return response({'error': 'The names list is temporarily unavailable.'}, 503)

@app.route(route='news/status', auth_level=func.AuthLevel.ANONYMOUS)
def status(req: func.HttpRequest):
    try:
        with _lock:
            snap = snapshot()
            sources = snap['sources']
            today = datetime.now(timezone.utc).astimezone(ZoneInfo('America/New_York')).date().isoformat()
            latest_indexed_date = snap['db'].execute(
                "SELECT MAX(published_day) FROM articles "
                "WHERE status='ready' AND published_day<>'' AND published_day<=?",
                (today,)).fetchone()[0]
            result = [{'id': s['source_id'], 'name': s.get('source', {}).get('name', SOURCE_NAMES[s['source_id']]),
                       'checked_at': s.get('checked_at'), 'ready': s.get('ready', 0),
                       'pending': s.get('pending', 0), 'issue': bool(s.get('error')),
                       'oldest': min((i['published_at'][:10] for i in s.get('items', [])
                                      if i.get('status') == 'ready' and i.get('published_at')), default=None),
                       'backfill': public_backfill(s),
                       'refresh_issues': sum(bool(i.get('last_error')) for i in s.get('items', []))} for s in sources]
        return response({'sources': result, 'latest_indexed_date': latest_indexed_date, 'schedule': 'Preview' if os.environ.get('NEWS_INDEX_LOCAL_DATA') else 'Daily', 'title': 'Go/Weiqi/Baduk News Index',
                         'translation_policy': {'enabled': os.environ.get('NEWS_TRANSLATION_BUDGET_ENABLED') == 'true',
                             'resume_at': os.environ.get('NEWS_TRANSLATION_RESUME_AT'),
                             'backfill_hours_eastern': [9, 15, 21], 'backfill_daily_characters': 20000,
                             'backfill_monthly_characters': 600000, 'recent_monthly_characters': 600000,
                             'total_monthly_characters': 1200000}})
    except Exception:
        logging.exception('News status unavailable')
        return response({'error': 'Collection status is temporarily unavailable.'}, 503)


@app.route(route='news/tournaments', auth_level=func.AuthLevel.ANONYMOUS)
def tournaments(req: func.HttpRequest):
    try:
        with _lock:
            result = query_tournaments(snapshot()['db'], req.params.get('tournament', ''),
                                      max(0, int(req.params.get('offset', '0'))), source=req.params.get('source', ''),
                                 start=req.params.get('start', ''), end=req.params.get('end', ''))
        return response(result)
    except (ValueError, OverflowError):
        return response({'error': 'Please check the tournament name and dates, then try again.'}, 400)
    except Exception:
        logging.exception('Tournament index unavailable')
        return response({'error': 'The tournament list is temporarily unavailable.'}, 503)

@app.timer_trigger(schedule='0 0 11 * * *', arg_name='timer', run_on_startup=False)
@app.queue_output(arg_name='jobs', queue_name='go-news-index-jobs', connection='AzureWebJobsStorage')
def daily_collection(timer: func.TimerRequest, jobs: func.Out[List[str]]):
    jobs.set([json.dumps({'source_id': sid}) for sid in SOURCE_IDS])

@app.timer_trigger(schedule='0 0 * * * *', arg_name='timer', run_on_startup=False)
@app.queue_output(arg_name='jobs', queue_name='go-news-index-jobs', connection='AzureWebJobsStorage')
def daily_history(timer: func.TimerRequest, jobs: func.Out[List[str]]):
    run_key = backfill_run_key(scheduled=True)
    if run_key is None:
        return
    # Rotate the starting publisher so a small shared allowance is not always
    # consumed by the same sources at the front of the queue.
    day = datetime.now(timezone.utc).astimezone(ZoneInfo('America/New_York')).date().toordinal()
    start = day % len(SOURCE_IDS)
    ordered = SOURCE_IDS[start:] + SOURCE_IDS[:start]
    jobs.set([json.dumps({'source_id': sid, 'kind': 'backfill', 'run_key': run_key}) for sid in ordered])

@app.queue_trigger(arg_name='job', queue_name='go-news-index-jobs', connection='AzureWebJobsStorage')
def collect_source(job: func.QueueMessage):
    message = json.loads(job.get_body())
    sid = message['source_id']
    if sid not in SOURCE_IDS:
        raise ValueError('Unknown source')
    client = container()
    try:
        client.create_container()
    except ResourceExistsError:
        pass
    blob = client.get_blob_client(f'sources/{sid}.json')
    try:
        blob.upload_blob(json.dumps({'source_id': sid, 'items': []}), overwrite=False)
    except ResourceExistsError:
        pass
    # A per-source lease prevents overlapping workers from discarding history.
    with source_lease(blob) as lease:
        previous = json.loads(blob.download_blob(lease=lease).readall())
        def checkpoint(result):
            blob.upload_blob(json.dumps(result, ensure_ascii=False), overwrite=True, lease=lease,
                             content_settings=ContentSettings(content_type='application/json'))
        if message.get('kind') == 'backfill':
            with translation_job('backfill'):
                result = backfill_source(sid, previous, checkpoint=checkpoint, run_key=message.get('run_key'),
                                         max_items=int(os.environ.get('NEWS_BACKFILL_ITEM_LIMIT', '2')))
            checkpoint(result)
            if result.get('backfill', {}).get('error'):
                logging.error('Historical discovery failed for %s: %s', sid, result['backfill']['error'])
                raise RuntimeError('Historical discovery failed; queued for retry')
            return
        result = sync_source(sid, previous, limit=int(os.environ.get('NEWS_INDEX_ITEM_LIMIT', '100')), checkpoint=checkpoint)
        checkpoint(result)
        if result.get('error'):
            logging.error('News source %s discovery failed: %s', sid, result['error'])
            raise RuntimeError('Source discovery failed; queued for retry')
