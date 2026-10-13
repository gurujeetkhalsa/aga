"""Durable, conservative reservations before paid translation requests."""
import copy
import json
import os
import time
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobServiceClient

job_kind = ContextVar('translation_job_kind', default='recent')
article_lane = ContextVar('translation_article_lane', default='recent')


class BudgetDeferred(RuntimeError):
    """Leave the article and its partial cache for the next allowance."""


@contextmanager
def translation_job(kind):
    token = job_kind.set(kind)
    try:
        yield
    finally:
        job_kind.reset(token)


def lane_for(item, moment=None):
    moment = moment or datetime.now(timezone.utc)
    if job_kind.get() == 'backfill' or item.get('historical'):
        return 'backfill'
    try:
        stamp = datetime.fromisoformat(item['published_at'].replace('Z', '+00:00'))
        if stamp.tzinfo:
            stamp = stamp.astimezone(ZoneInfo('America/New_York'))
        if stamp.date() >= moment.astimezone(ZoneInfo('America/New_York')).date() - timedelta(days=6):
            return 'recent'
    except (ValueError, TypeError, KeyError):
        pass
    return 'backfill'


def reserve(state, characters, lane, moment, resume_at):
    """Pure reservation; UTC billing months, Eastern daily backfill allowance."""
    if moment < resume_at:
        raise BudgetDeferred('Translation resumes after the Azure quota reset')
    if characters <= 0 or lane not in ('recent', 'backfill'):
        raise ValueError('Invalid translation reservation')
    month = moment.astimezone(timezone.utc).strftime('%Y-%m')
    day = moment.astimezone(ZoneInfo('America/New_York')).date().isoformat()
    state = copy.deepcopy(state)
    if state.get('month') != month:
        state = {'month': month, 'recent': 0, 'backfill': 0, 'total': 0}
    if state.get('day') != day:
        state.update(day=day, backfill_today=0)
    if state['total'] + characters > 1200000 or state[lane] + characters > 600000:
        raise BudgetDeferred(lane + ' monthly translation allowance reached')
    if lane == 'backfill' and state['backfill_today'] + characters > 20000:
        raise BudgetDeferred('Historical daily translation allowance reached')
    state['total'] += characters
    state[lane] += characters
    if lane == 'backfill':
        state['backfill_today'] += characters
    state['updated_at'] = moment.isoformat()
    return state


def reserve_translation(text):
    if os.environ.get('NEWS_TRANSLATION_BUDGET_ENABLED') != 'true':
        return
    lane = article_lane.get()
    if lane == 'backfill' and job_kind.get() != 'backfill':
        raise BudgetDeferred('Older articles wait for a scheduled historical batch')
    # Azure counts supplementary Unicode characters as two code units.
    characters = len(text.encode('utf-16-le')) // 2
    blob = BlobServiceClient.from_connection_string(os.environ['AzureWebJobsStorage']).get_blob_client(
        'go-news-index', 'translation-budget.json')
    try:
        blob.upload_blob('{}', overwrite=False)
    except ResourceExistsError:
        pass
    except HttpResponseError as error:
        if getattr(error, 'error_code', None) != 'LeaseIdMissing':
            raise
    for attempt in range(120):
        try:
            lease = blob.acquire_lease(lease_duration=15)
        except HttpResponseError as error:
            if error.status_code != 409:
                raise
            time.sleep(.5)
            continue
        try:
            state = json.loads(blob.download_blob(lease=lease).readall())
            moment = datetime.now(timezone.utc)
            resume = datetime.fromisoformat(os.environ['NEWS_TRANSLATION_RESUME_AT'].replace('Z', '+00:00'))
            state = reserve(state, characters, lane, moment, resume)
            blob.upload_blob(json.dumps(state), overwrite=True, lease=lease)
        finally:
            lease.release()
        return
    raise BudgetDeferred('Translation allowance tracker is busy; retry next collection')
