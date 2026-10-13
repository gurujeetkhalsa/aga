"""Timer worker with durable claims and conservative handling of ambiguous sends.

The event lock is held across the bounded provider call. Cancellations and date
changes therefore cannot overtake an email's final eligibility check. The claim
is committed first; a process crash leaves evidence for manual reconciliation.
"""
import os
import time
from datetime import timedelta
from contextlib import contextmanager
from urllib.parse import quote
from event_domain import utc_now
from workshop_domain import email_is_eligible
from workshop_messages import build_message
from workshop_providers import GmailSender, ZoomClient, ProviderError, email_delivery_config
from workshop_repository import rows, sql_time


@contextmanager
def transaction(repo):
    conn=repo._connect()
    try:
        cursor=conn.cursor();yield cursor;conn.commit()
    except Exception:
        conn.rollback();raise
    finally:
        conn.close()


def lock_event(cursor,event_id):
    cursor.execute('''SELECT event_id, time_zone, slug FROM events.events WITH (UPDLOCK,HOLDLOCK)
                      WHERE event_id = ?''',event_id)
    return rows(cursor)[0]


def process_email(repo, sender, now):
    delivery=email_delivery_config()
    if delivery['mode']=='off':return False
    test_series=delivery['mode']=='test'
    with transaction(repo) as c:
        c.execute('''SELECT TOP (1) q.email_id, q.event_id FROM events.email_outbox q
            JOIN events.workshop_settings s ON s.event_id=q.event_id
            LEFT JOIN events.rehearsal_clocks rc ON rc.occurrence_id=q.occurrence_id AND s.is_test=1
            WHERE q.status='pending' AND q.due_at_utc <=
              CASE WHEN q.kind IN ('confirmed','waitlisted','cancelled','workshop_cancelled') THEN ? ELSE COALESCE(rc.simulated_at_utc,?) END
            AND (q.retry_after_utc IS NULL OR q.retry_after_utc<=?)
            AND (rc.stage IS NULL OR q.kind<>'followup' OR rc.stage=4)
            AND s.email_enabled=1 AND s.is_test=?
            ORDER BY q.due_at_utc,q.email_id''',sql_time(now),sql_time(now),sql_time(now),test_series)
        candidates=rows(c)
    if not candidates:return False
    candidate=candidates[0];eid=candidate['event_id'];mid=candidate['email_id']
    with transaction(repo) as c:
        lock_event(c,eid)
        c.execute("SELECT status FROM events.email_outbox WHERE email_id=?",mid)
        if c.fetchone()[0]!='pending':return True
        c.execute("UPDATE events.email_outbox SET status='sending', claimed_at_utc=? WHERE email_id=?",sql_time(now),mid)
    with transaction(repo) as c:
        event=lock_event(c,eid)
        c.execute('''SELECT q.email_id,q.kind,q.attempts,q.status,q.occurrence_id,
            o.title,o.starts_at_utc,o.ends_at_utc,o.lifecycle,ro.status AS selection_status,ro.attendance,
            c.first_name,c.last_name,c.email,m.join_url,m.sync_status
            FROM events.email_outbox q
            JOIN events.event_occurrences o ON o.occurrence_id=q.occurrence_id
            JOIN events.registration_occurrences ro ON ro.occurrence_id=q.occurrence_id AND ro.registration_id=q.registration_id
            JOIN events.registration_contacts c ON c.registration_id=q.registration_id
            LEFT JOIN events.workshop_meetings m ON m.occurrence_id=o.occurrence_id
            WHERE q.email_id=?''',mid)
        data=rows(c)[0];data['time_zone']=event['time_zone']
        base=os.environ.get('EVENTS_PUBLIC_BASE_URL','https://aga-events.azurewebsites.net').strip().rstrip('/')
        data['registration_url']=f"{base}/events/{quote(event['slug'],safe='')}"
        if data['status']!='sending':return True
        settings=repo._settings(c,eid)
        if not settings['email_enabled'] or bool(settings['is_test'])!=test_series:
            c.execute("UPDATE events.email_outbox SET status='pending' WHERE email_id=?",mid);return True
        effective_now=now
        if test_series:
            c.execute('SELECT simulated_at_utc FROM events.rehearsal_clocks WHERE occurrence_id=?',data['occurrence_id'])
            clocks=rows(c)
            if clocks:effective_now=clocks[0]['simulated_at_utc']
        if not email_is_eligible(data['kind'],data['selection_status'],data['lifecycle'],data['starts_at_utc'],data['ends_at_utc'],effective_now):
            c.execute("UPDATE events.email_outbox SET status='suppressed' WHERE email_id=?",mid);return True
        if data['kind']=='followup' and data['attendance'] not in ('attended','absent'):
            # Leave the same email queued without consuming a delivery attempt.
            # Advancing its due time lets other recipients proceed while Milan reviews it.
            c.execute("UPDATE events.email_outbox SET status='pending', retry_after_utc=?, last_error='Waiting for attendance to be recorded.' WHERE email_id=?",sql_time(now+timedelta(minutes=5)),mid)
            return True
        needs_link=data['kind']=='confirmed' or data['kind'].startswith('reminder_')
        if needs_link and (not data['join_url'] or data['sync_status']!='ready'):
            c.execute("UPDATE events.email_outbox SET status='pending', retry_after_utc=?, last_error='Waiting for Zoom joining details.' WHERE email_id=?",sql_time(now+timedelta(minutes=5)),mid)
            return True
        attempts=data['attempts']+1
        try:
            message=build_message(data,settings,now)
            provider_id=sender.send(message)
        except ProviderError as exc:
            status='uncertain' if exc.uncertain else ('pending' if exc.retryable and attempts<5 else 'failed')
            c.execute('''UPDATE events.email_outbox SET status=?,attempts=?,last_error=?,retry_after_utc=? WHERE email_id=?''',
                status,attempts,str(exc),sql_time(now+timedelta(minutes=min(60,2**attempts))),mid)
        except Exception:
            # Unknown exceptions could occur after a provider accepted a request.
            c.execute("UPDATE events.email_outbox SET status='uncertain',attempts=?,last_error='Delivery needs reconciliation.' WHERE email_id=?",attempts,mid)
        else:
            c.execute("UPDATE events.email_outbox SET status='sent',attempts=?,provider_message_id=?,sent_at_utc=?,last_error=NULL WHERE email_id=?",attempts,provider_id,sql_time(now),mid)
    return True


def prepare_meetings(repo,now):
    with transaction(repo) as c:
        c.execute('''SELECT event_id FROM events.workshop_settings WHERE zoom_host_email IS NOT NULL''')
        event_ids=[r[0] for r in c.fetchall()]
    for eid in event_ids:
        with transaction(repo) as c:
            lock_event(c,eid)
            c.execute('''INSERT INTO events.workshop_meetings(occurrence_id)
                SELECT o.occurrence_id FROM events.event_occurrences o
                WHERE o.event_id=? AND o.lifecycle='published' AND o.starts_at_utc>?
                AND NOT EXISTS(SELECT 1 FROM events.workshop_meetings m WHERE m.occurrence_id=o.occurrence_id)''',eid,sql_time(now))


def process_zoom(repo,zoom,now):
    with transaction(repo) as c:
        c.execute('''SELECT TOP (1) m.occurrence_id,o.event_id FROM events.workshop_meetings m
            JOIN events.event_occurrences o ON o.occurrence_id=m.occurrence_id
            WHERE m.source='zoom' AND m.sync_status='pending' ORDER BY m.updated_at_utc,m.occurrence_id''')
        candidates=rows(c)
    if not candidates:return False
    item=candidates[0];oid=item['occurrence_id'];eid=item['event_id']
    with transaction(repo) as c:
        lock_event(c,eid)
        c.execute("SELECT sync_status FROM events.workshop_meetings WHERE occurrence_id=?",oid)
        if c.fetchone()[0]!='pending':return True
        c.execute("UPDATE events.workshop_meetings SET sync_status='working',updated_at_utc=? WHERE occurrence_id=?",sql_time(now),oid)
    with transaction(repo) as c:
        event=lock_event(c,eid)
        occurrence=repo._lock_date(c,eid,oid)
        c.execute('SELECT * FROM events.workshop_meetings WHERE occurrence_id=?',oid)
        meeting=rows(c)[0]
        if meeting['sync_status']!='working':return True
        settings=repo._settings(c,eid)
        try:
            if occurrence['lifecycle']=='cancelled':
                if meeting['meeting_id']:zoom.delete(meeting['meeting_id'])
                status='cancelled'
            elif occurrence['starts_at_utc']<=now:
                status='failed'
                raise ProviderError('Workshop already started; review its Zoom meeting manually.')
            elif meeting['meeting_id']:
                zoom.update(meeting['meeting_id'],occurrence,event);status='ready'
            else:
                if not settings['zoom_host_email']:raise ProviderError('Configure the licensed AGA Zoom host email.')
                created=zoom.create(settings['zoom_host_email'],occurrence,event)
                meeting.update(created);status='ready'
        except ProviderError as exc:
            status='uncertain' if exc.uncertain else 'failed'
            c.execute('''UPDATE events.workshop_meetings SET sync_status=?,sync_error=?,updated_at_utc=? WHERE occurrence_id=?''',status,str(exc),sql_time(now),oid)
        except Exception:
            c.execute("UPDATE events.workshop_meetings SET sync_status='uncertain',sync_error='Zoom operation needs reconciliation.',updated_at_utc=? WHERE occurrence_id=?",sql_time(now),oid)
        else:
            c.execute('''UPDATE events.workshop_meetings SET sync_status=?,meeting_id=?,join_url=?,sync_error=NULL,updated_at_utc=? WHERE occurrence_id=?''',
                      status,meeting['meeting_id'],meeting['join_url'],sql_time(now),oid)
    return True


def recover_abandoned_claims(repo,now):
    # Never blindly replay an external mutation after a crash or lost database commit.
    with transaction(repo) as c:
        c.execute("UPDATE events.email_outbox SET status='uncertain',last_error='Worker stopped during delivery; check Gmail Sent before retrying.' WHERE status='sending' AND claimed_at_utc < ?",sql_time(now-timedelta(minutes=15)))
        c.execute("UPDATE events.workshop_meetings SET sync_status='uncertain',sync_error='Worker stopped; inspect Zoom before retrying.' WHERE sync_status='working' AND updated_at_utc < ?",sql_time(now-timedelta(minutes=15)))


def run_worker(repo, *, sender=None, zoom=None, now=None, limit=3):
    deadline=time.monotonic()+150
    fixed_now=now
    now=now or utc_now()
    recover_abandoned_claims(repo,now)
    if os.environ.get('EVENTS_ZOOM_ENABLED')=='true':
        prepare_meetings(repo,now)
        zoom=zoom or ZoomClient()
        for _ in range(limit):
            if time.monotonic()>=deadline:break
            if not process_zoom(repo,zoom,fixed_now or utc_now()):break
    if os.environ.get('EVENTS_EMAIL_ENABLED')=='true':
        sender=sender or GmailSender()
        for _ in range(limit):
            if time.monotonic()>=deadline:break
            if not process_email(repo,sender,fixed_now or utc_now()):break
