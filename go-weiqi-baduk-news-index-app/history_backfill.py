"""Checkpointed archive backfill, newest publication day first, for every source."""
import copy
import hashlib
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from archive_discovery import archive_page
from index_core import SOURCE_NAMES, identity, index_item, now
from national_sources import NEW_SOURCE_IDS
from additional_sources import ADDITIONAL_IDS


def backfill_run_key(moment=None, scheduled=False):
    """One UTC key per batch; scheduled batches run at 09:00, 15:00, 21:00 Eastern."""
    moment = moment or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        raise ValueError('Backfill scheduling requires a timezone-aware time')
    eastern = moment.astimezone(ZoneInfo('America/New_York'))
    if scheduled and eastern.hour not in (9, 15, 21):
        return None
    return moment.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:00Z')


def completed_run(run_key, last_run_key):
    if not run_key or not last_run_key:
        return False
    if run_key == last_run_key:
        return True
    # A delayed delivery of an older hour must not consume another batch after
    # a newer hour has finished. Legacy date-only keys are midnight UTC.
    try:
        current = datetime.fromisoformat(run_key).replace(tzinfo=timezone.utc)
        previous = datetime.fromisoformat(last_run_key).replace(tzinfo=timezone.utc)
        return current <= previous
    except ValueError:
        return False


def backfill_source(sid, previous=None, checkpoint=None, run_key=None,
                    max_items=8, max_days=7, max_pages=4, discover_page=None, index=None):
    result = copy.deepcopy(previous or {'source_id': sid, 'items': []})
    result.setdefault('source', {'id': sid, 'name': SOURCE_NAMES[sid]})
    records = {i['id']: i for i in result.get('items', [])}
    state = result.setdefault('backfill', {})
    if completed_run(run_key, state.get('last_run_key')):
        return result
    dates = [i['published_at'][:10] for i in records.values() if i.get('published_at')]
    # Start new national sources at the top: pinned old RSS stories must not
    # move the starting boundary past newer articles absent from the recent feed.
    initial_day = max(dates) if dates and sid in NEW_SOURCE_IDS + ADDITIONAL_IDS else min(dates) if dates else date.today().isoformat()
    state.setdefault('through_day', initial_day)
    state.setdefault('started_at', now())
    state.setdefault('buffer', [])
    state.update(checked_at=now(), error=None, status='working')
    discover_page, index = discover_page or archive_page, index or index_item
    processed = days = pages = 0

    def save():
        result['items'] = list(records.values())
        result['ready'] = sum(i.get('status') == 'ready' for i in records.values())
        result['pending'] = len(records) - result['ready']
        state['buffered'] = len(state['buffer'])
        state['indexed'] = sum(bool(i.get('historical')) and i.get('status') == 'ready' for i in records.values())
        state['pending'] = sum(bool(i.get('historical')) and i.get('status') != 'ready' for i in records.values())
        if checkpoint:
            checkpoint(result)

    try:
        # Retry the existing backlog first, including articles discovered by the
        # daily feed. It shares the same historical character allowance.
        pending = sorted((r for r in records.values() if r.get('status') != 'ready'),
                         key=lambda r: r.get('checked_at', ''))
        # Leave space for archive discovery even when a publisher has permanent
        # missing-body failures; those retries must not stall its entire archive.
        retry_limit = min(max_items, max(1, max_items // 2))
        for old in pending[:retry_limit]:
            key = old['id']
            records[key] = index(old, old)
            processed += 1
            save()
            if records[key].get('budget_deferred'):
                state.update(status='waiting for translation allowance', last_run_key=run_key,
                             last_run_added=processed, completed_at=now())
                save()
                return result
        while processed < max_items and days < max_days:
            bound = state['through_day']
            # Buffer retains whole pages so a day split across pages is never skipped.
            eligible = [i for i in state['buffer'] if i['published_at'][:10] <= bound]
            target = max((i['published_at'][:10] for i in eligible), default=None)
            complete_day = target and (state.get('archive_end') or
                                       (sid != 'italy' and state.get('scanned_through', '9999') < target))
            if not complete_day and not state.get('archive_end'):
                if pages >= max_pages:
                    break
                payload = discover_page(sid, state.get('cursor'), bound)
                items = payload['items']
                signature = hashlib.sha256('\n'.join(sorted(identity(i) for i in items)).encode()).hexdigest()
                if items and signature == state.get('last_page_signature'):
                    raise ValueError('Publisher repeated an archive page; progress retained for retry')
                buffer = {identity(i): i for i in state['buffer']}
                # Keep ready boundary-day entries as well, to verify and advance that day.
                buffer.update({identity(i): i for i in items if i['published_at'][:10] <= bound})
                state['buffer'] = list(buffer.values())
                state.update(cursor=payload['cursor'], archive_end=payload['exhausted'],
                             last_page_signature=signature)
                if items:
                    # New-source feeds may pin old notices above newer reports.
                    # A following page's newest date, rather than a pinned oldest
                    # date, establishes a safe boundary before processing a day.
                    boundary = max if sid in NEW_SOURCE_IDS + ADDITIONAL_IDS else min
                    state['scanned_through'] = boundary(i['published_at'][:10] for i in items)
                pages += 1
                save()
                continue
            if target is None:
                if state.get('archive_end'):
                    state['status'] = 'archive scanned'
                break
            state['current_day'] = target
            day_items = [i for i in eligible if i['published_at'][:10] == target]
            # A checkpoint can resume partway through a busy day without refetching successes.
            attempted = set(state.get('day_attempted', []))
            for item in day_items:
                key = identity(item)
                if key in attempted or records.get(key, {}).get('status') == 'ready':
                    continue
                if processed >= max_items:
                    break
                records[key] = index(item, records.get(key))
                records[key]['historical'] = True
                if records[key].get('budget_deferred'):
                    # Keep this day and article in the buffer, with partial
                    # translation cache intact. Never advance past a budget stop.
                    state.update(status='waiting for translation allowance', last_run_key=run_key,
                                 last_run_added=processed, completed_at=now())
                    save()
                    return result
                attempted.add(key)
                state['day_attempted'] = sorted(attempted)
                processed += 1
                save()
            if any(identity(i) not in attempted and records.get(identity(i), {}).get('status') != 'ready'
                   for i in day_items):
                break
            # Failed text/translation remains pending for later historical retries.
            state['buffer'] = [i for i in state['buffer'] if i['published_at'][:10] < target]
            state.update(last_day=target, through_day=(date.fromisoformat(target) - timedelta(days=1)).isoformat(),
                         day_attempted=[], current_day=None)
            state['days_checked'] = state.get('days_checked', 0) + 1
            days += 1
            save()
        if state['status'] == 'working':
            state['status'] = 'continuing'
        state.update(last_run_key=run_key, last_run_added=processed, last_run_days=days, completed_at=now())
    except Exception as error:
        state.update(status='retrying', error=f'{type(error).__name__}: {str(error)[:200]}')
    save()
    return result


def public_backfill(source):
    state = source.get('backfill', {})
    view = {k: state.get(k) for k in ('status', 'through_day', 'current_day', 'last_day', 'checked_at',
                                     'indexed', 'pending', 'days_checked', 'buffered')}
    view['pending'] = sum(bool(i.get('historical')) and i.get('status') != 'ready' for i in source.get('items', []))
    return view
