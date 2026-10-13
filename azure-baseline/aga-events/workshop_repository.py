"""Transactional workshop operations. Every capacity writer locks event then date."""
from contextlib import contextmanager
import json
from datetime import timedelta
from uuid import uuid4

from event_domain import EventConflict, EventError, EventForbidden, EventNotFound
from workshop_domain import registration_status, scheduled_messages, normalize_settings, validate_join_url, DEFAULT_ATTENDED_FOLLOWUP


def rows(cursor):
    from sql_repository import _row_dict
    return [_row_dict(cursor, r) for r in cursor.fetchall()]


def sql_time(value):
    from sql_repository import _sql_datetime
    return _sql_datetime(value)


def ready(value):
    from sql_repository import _json_ready
    return _json_ready(value)


class WorkshopRepositoryMixin:
    @staticmethod
    def _rehearsal_clock(cursor, occurrence_id):
        cursor.execute('SELECT stage,simulated_at_utc FROM events.rehearsal_clocks WHERE occurrence_id=?',occurrence_id)
        found=rows(cursor)
        return found[0] if found else None

    def rehearsal_clock(self, *, event_id, occurrence_id, principal_id, now, expected_stage=None):
        from workshop_providers import email_delivery_config
        if email_delivery_config()['mode']!='test':raise EventConflict('The rehearsal clock requires email test mode.')
        with self._workshop_transaction(event_id,principal_id,'can_manage_events') as (c,event):
            settings=self._settings(c,event_id)
            if not settings['is_test']:raise EventForbidden('The rehearsal clock is only available for a rehearsal series.')
            occurrence=self._lock_date(c,event_id,occurrence_id)
            if occurrence['lifecycle']!='published':raise EventConflict('Use a published rehearsal date.')
            clock=self._rehearsal_clock(c,occurrence_id)
            stage=clock['stage'] if clock else -1
            at=clock['simulated_at_utc'] if clock else now
            c.execute("""SELECT COUNT(*) FROM events.email_outbox WHERE occurrence_id=?
                AND status IN ('pending','sending','failed','uncertain')
                AND due_at_utc<=CASE WHEN kind IN ('confirmed','waitlisted','cancelled','workshop_cancelled') THEN ? ELSE ? END
                AND (kind<>'followup' OR ?=4)""",occurrence_id,sql_time(now),sql_time(at),stage)
            pending=c.fetchone()[0]
            if expected_stage is not None:
                if type(expected_stage) is not int or not -1<=expected_stage<=4:raise EventError('Invalid rehearsal step.')
                if expected_stage>stage:raise EventConflict('Refresh the rehearsal clock before advancing.')
                if expected_stage==stage and stage<4:
                    if stage>=0 and pending:raise EventConflict('Wait for this step’s emails to finish, or resolve their email status, before advancing.')
                    if stage==-1 and occurrence['starts_at_utc']<=now+timedelta(hours=24):
                        raise EventConflict('Choose a rehearsal date more than 24 hours in the future.')
                    stage+=1
                    at={0:now,1:occurrence['starts_at_utc']-timedelta(hours=24),
                        2:occurrence['starts_at_utc']-timedelta(hours=1),3:occurrence['ends_at_utc'],
                        4:occurrence['ends_at_utc']+timedelta(hours=settings['followup_hours'])}[stage]
                    if clock and at<clock['simulated_at_utc']:raise EventConflict('The next checkpoint precedes the current clock. Use a new rehearsal date.')
                    if clock:c.execute('UPDATE events.rehearsal_clocks SET stage=?,simulated_at_utc=?,updated_at_utc=? WHERE occurrence_id=?',stage,sql_time(at),sql_time(now),occurrence_id)
                    else:c.execute('INSERT INTO events.rehearsal_clocks(occurrence_id,stage,simulated_at_utc) VALUES (?,?,?)',occurrence_id,stage,sql_time(at))
                    self._audit_workshop(c,event_id,principal_id,'rehearsal.clock.advanced',dict(occurrence_id=occurrence_id,stage=stage,simulated_at_utc=at.isoformat()),now)
            return ready(dict(stage=stage,simulated_at_utc=at,pending=pending))

    @staticmethod
    def _check_meeting_editable(cursor, occurrence_id):
        cursor.execute('SELECT sync_status FROM events.workshop_meetings WHERE occurrence_id=?',occurrence_id)
        row=cursor.fetchone()
        if row and row[0] in {'working','uncertain'}:
            raise EventConflict('The Zoom meeting operation must finish or be reconciled before editing or cancelling this date.')

    def reconcile_meeting(self, *, event_id, occurrence_id, principal_id, payload, now, zoom):
        from event_domain import require_text
        note=require_text(payload.get('note'),'Reconciliation note',500)
        meeting_id=str(payload.get('meeting_id') or '').strip()
        if meeting_id and not meeting_id.isdigit():raise EventError('Enter the numeric Zoom meeting ID.')
        if not meeting_id and payload.get('confirmed_absent') is not True:
            raise EventError('Verify that no meeting was created before requesting a retry.')
        with self._workshop_transaction(event_id,principal_id,'can_manage_events') as (c,event):
            occurrence=self._lock_date(c,event_id,occurrence_id)
            c.execute('SELECT sync_status FROM events.workshop_meetings WHERE occurrence_id=?',occurrence_id)
            current=c.fetchone()
            if not current or current[0]!='uncertain':raise EventConflict('Only uncertain Zoom operations can be reconciled.')
            link=None
            if meeting_id:
                host=self._settings(c,event_id)['zoom_host_email']
                link=zoom.adopt(meeting_id,host,occurrence,event)
            c.execute('''UPDATE events.workshop_meetings SET meeting_id=?,join_url=?,sync_status='pending',
                sync_error=NULL,updated_at_utc=? WHERE occurrence_id=?''',meeting_id or None,link,sql_time(now),occurrence_id)
            self._audit_workshop(c,event_id,principal_id,'zoom.reconciled',dict(occurrence_id=occurrence_id,meeting_id=meeting_id,note=note),now)
        return {'status':'pending'}

    def get_meeting(self, *, event_id, occurrence_id, principal_id):
        with self._workshop_transaction(event_id, principal_id, 'can_manage_events') as (cursor, event):
            self._lock_date(cursor, event_id, occurrence_id)
            cursor.execute('SELECT meeting_id,join_url,source,sync_status,sync_error FROM events.workshop_meetings WHERE occurrence_id=?',occurrence_id)
            result=rows(cursor)
            return result[0] if result else dict(meeting_id=None,join_url=None,source='zoom',sync_status='not_configured',sync_error=None)

    def configure_meeting(self, *, event_id, occurrence_id, principal_id, payload, now):
        source=payload.get('source')
        if source not in {'manual','zoom'}:raise EventError('Choose Zoom scheduling or a manual joining link.')
        link=validate_join_url(payload.get('join_url')) if source=='manual' else None
        with self._workshop_transaction(event_id,principal_id,'can_manage_events') as (c,event):
            occurrence=self._lock_date(c,event_id,occurrence_id)
            if occurrence['lifecycle']!='published' or now>=occurrence['starts_at_utc']:
                raise EventConflict('Configure Zoom only for an upcoming published workshop.')
            c.execute('SELECT * FROM events.workshop_meetings WHERE occurrence_id=?',occurrence_id)
            current=rows(c)
            if current and current[0]['sync_status'] in {'working','uncertain'}:
                raise EventConflict('This Zoom operation needs reconciliation before making another change.')
            if current and current[0]['meeting_id'] and source=='manual':
                raise EventConflict('An automatically managed Zoom meeting cannot be replaced by a manual link.')
            if source=='zoom' and not self._settings(c,event_id)['zoom_host_email']:
                raise EventError('Save the licensed AGA Zoom host email in settings first.')
            c.execute('''IF NOT EXISTS(SELECT 1 FROM events.workshop_meetings WHERE occurrence_id=?)
                INSERT INTO events.workshop_meetings(occurrence_id) VALUES (?)''',occurrence_id,occurrence_id)
            c.execute('''UPDATE events.workshop_meetings SET source=?,join_url=COALESCE(?,join_url),
                sync_status=?,sync_error=NULL,updated_at_utc=? WHERE occurrence_id=?''',
                source,link,'ready' if source=='manual' else 'pending',sql_time(now),occurrence_id)
            self._audit_workshop(c,event_id,principal_id,'zoom.configured',dict(occurrence_id=occurrence_id,source=source),now)
        return {'status':'ready' if source=='manual' else 'pending'}

    def reconcile_email(self, *, event_id, email_id, principal_id, resolution, note, now):
        from event_domain import require_text
        note=require_text(note,'Reconciliation note',500)
        if resolution not in {'sent','retry'}:raise EventError('Choose sent or retry after checking the sender mailbox.')
        with self._workshop_transaction(event_id,principal_id) as (c,event):
            c.execute('SELECT status,registration_id FROM events.email_outbox WHERE email_id=? AND event_id=?',email_id,event_id)
            current=c.fetchone()
            if not current:raise EventNotFound('Email not found.')
            if current[0] not in {'failed','uncertain'}:raise EventConflict('Only failed or uncertain deliveries can be reconciled.')
            c.execute('''UPDATE events.email_outbox SET status=?,due_at_utc=?,last_error=NULL,
                sent_at_utc=?,attempts=0,retry_after_utc=NULL WHERE email_id=?''',
                'sent' if resolution=='sent' else 'pending',sql_time(now),sql_time(now) if resolution=='sent' else None,email_id)
            self._audit_workshop(c,event_id,principal_id,'email.reconciled',dict(email_id=email_id,resolution=resolution,note=note),now,current[1])
        return {'status':resolution}

    @contextmanager
    def _workshop_transaction(self, event_id, principal_id, permission='can_manage_registrations'):
        conn = self._connect()
        try:
            cursor = conn.cursor()
            event = self._authorize_workshop(cursor, event_id, principal_id, permission)
            yield cursor, event
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _authorize_workshop(cursor, event_id, principal_id, permission):
        if permission not in {'can_manage_events', 'can_manage_registrations'}:
            raise ValueError('Unknown permission')
        cursor.execute(f"""
            SELECT e.event_id, e.slug, e.name, e.time_zone, e.current_event_version_id
            FROM events.events e WITH (UPDLOCK, HOLDLOCK)
            JOIN events.organizer_memberships m ON m.organizer_id = e.organizer_id
            JOIN events.organizers org ON org.organizer_id = e.organizer_id
            WHERE e.event_id = ? AND m.principal_object_id = ? AND m.is_active = 1
              AND org.is_active = 1 AND m.{permission} = 1
            """, event_id, principal_id)
        result = rows(cursor)
        if not result:
            raise EventForbidden('You do not have permission for this workshop operation.')
        return result[0]

    @staticmethod
    def _lock_date(cursor, event_id, occurrence_id):
        cursor.execute('''SELECT * FROM events.event_occurrences WITH (UPDLOCK, HOLDLOCK)
                          WHERE event_id = ? AND occurrence_id = ?''', event_id, occurrence_id)
        result = rows(cursor)
        if not result:
            raise EventNotFound('Workshop date not found.')
        return result[0]

    @staticmethod
    def _audit_workshop(cursor, event_id, principal_id, action, details, now, registration_id=None):
        cursor.execute('''INSERT INTO events.audit_log
            (event_id, registration_id, action, actor_type, actor_principal_id, detail_json, created_at_utc)
            VALUES (?, ?, ?, 'organizer', ?, ?, ?)''', event_id, registration_id,
            action, principal_id, json.dumps(details), sql_time(now))

    @staticmethod
    def _settings(cursor, event_id):
        cursor.execute('SELECT * FROM events.workshop_settings WHERE event_id = ?', event_id)
        result = rows(cursor)
        if result:
            return result[0]
        return dict(event_id=event_id, sender_email='milan.mladenovic@usgo.org',
                    instructor_name='Milan', reminder_hours='[24,1]', followup_hours=24,
                    followup_text=DEFAULT_ATTENDED_FOLLOWUP,
                    email_enabled=False, zoom_host_email=None, is_test=False)

    @staticmethod
    def _queue_email(cursor, event_id, registration_id, occurrence_id, kind, due, suffix=''):
        key = f'{registration_id}:{occurrence_id}:{kind}:{suffix}'
        cursor.execute('''IF NOT EXISTS (SELECT 1 FROM events.email_outbox WHERE dedupe_key = ?)
            INSERT INTO events.email_outbox
                (event_id, registration_id, occurrence_id, kind, dedupe_key, due_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)''', key, event_id, registration_id, occurrence_id, kind, key, sql_time(due))

    def _queue_registration_emails(self, cursor, event_id, registration_id, occurrence, status, now, suffix=''):
        oid = occurrence['occurrence_id']
        self._queue_email(cursor, event_id, registration_id, oid, status, now, suffix)
        if status == 'confirmed':
            clock=self._rehearsal_clock(cursor,oid)
            if clock:
                from workshop_providers import email_delivery_config
                if email_delivery_config()['mode']=='test':now=clock['simulated_at_utc']
            for kind, due in scheduled_messages(occurrence['starts_at_utc'], occurrence['ends_at_utc'],
                                                self._settings(cursor, event_id), now):
                self._queue_email(cursor, event_id, registration_id, oid, kind, due, suffix)

    @staticmethod
    def _refresh_registration_status(cursor, registration_id, now):
        cursor.execute('SELECT status FROM events.registration_occurrences WHERE registration_id = ?', registration_id)
        status = registration_status(r[0] for r in cursor.fetchall())
        cursor.execute('UPDATE events.registrations SET status = ?, updated_at_utc = ? WHERE registration_id = ?',
                       status, sql_time(now), registration_id)

    def _promote_waitlist(self, cursor, event_id, occurrence, now):
        clock=self._rehearsal_clock(cursor,occurrence['occurrence_id'])
        if clock:
            from workshop_providers import email_delivery_config
            if email_delivery_config()['mode']=='test' and clock['simulated_at_utc']>=occurrence['starts_at_utc']:return
        if occurrence['lifecycle'] != 'published' or occurrence['starts_at_utc'] <= now:
            return
        oid = occurrence['occurrence_id']
        cursor.execute("SELECT COUNT(*) FROM events.registration_occurrences WHERE occurrence_id = ? AND status = 'confirmed'", oid)
        count = cursor.fetchone()[0]
        cursor.execute('''SELECT registration_occurrence_id, registration_id FROM events.registration_occurrences
            WHERE occurrence_id = ? AND status = 'waitlisted' ORDER BY assigned_at_utc, registration_occurrence_id''', oid)
        waiting = rows(cursor)
        free = len(waiting) if occurrence['capacity'] is None else max(0, occurrence['capacity'] - count)
        for selected in waiting[:free]:
            rid = selected['registration_id']
            cursor.execute("UPDATE events.registration_occurrences SET status = 'confirmed' WHERE registration_occurrence_id = ?",
                           selected['registration_occurrence_id'])
            cursor.execute("UPDATE events.email_outbox SET status = 'suppressed' WHERE registration_id = ? AND occurrence_id = ? AND status = 'pending'", rid, oid)
            self._refresh_registration_status(cursor, rid, now)
            self._queue_registration_emails(cursor, event_id, rid, occurrence, 'confirmed', now, 'promotion')
            self._audit_workshop(cursor, event_id, None, 'registration.promoted', {'occurrence_id': oid}, now, rid)

    def registration_statistics(self, *, event_id, principal_id, now, occurrence_id=None):
        """Count one signup per workshop date, retaining cancellations and referral history."""
        with self._workshop_transaction(event_id, principal_id) as (cursor, event):
            if occurrence_id is not None:
                self._lock_date(cursor, event_id, occurrence_id)
            cursor.execute('''SELECT r.referral_chapter_id AS chapter_id,
                ch.ChapterName AS chapter_name, ch.ChapterCode AS chapter_code,
                COUNT_BIG(*) AS signups,
                SUM(CASE WHEN ro.status='confirmed' THEN 1 ELSE 0 END) AS confirmed,
                SUM(CASE WHEN ro.status='waitlisted' THEN 1 ELSE 0 END) AS waitlisted,
                SUM(CASE WHEN ro.status='cancelled' THEN 1 ELSE 0 END) AS cancelled,
                SUM(CASE WHEN ro.status='confirmed' AND ro.attendance='attended' THEN 1 ELSE 0 END) AS attended,
                SUM(CASE WHEN ro.status='confirmed' AND ro.attendance='absent' THEN 1 ELSE 0 END) AS absent,
                SUM(CASE WHEN ro.status='confirmed' AND ro.attendance='unrecorded' AND o.ends_at_utc <= ? THEN 1 ELSE 0 END) AS needs_attendance
                FROM events.registrations r
                JOIN events.registration_occurrences ro ON ro.registration_id=r.registration_id
                JOIN events.event_occurrences o ON o.occurrence_id=ro.occurrence_id AND o.event_id=r.event_id
                LEFT JOIN membership.chapters ch ON ch.ChapterID=r.referral_chapter_id
                WHERE r.event_id=? AND (? IS NULL OR o.occurrence_id=?)
                GROUP BY r.referral_chapter_id, ch.ChapterName, ch.ChapterCode
                ORDER BY CASE WHEN r.referral_chapter_id IS NULL THEN 0 ELSE 1 END, ch.ChapterName, r.referral_chapter_id''',
                sql_time(now), event_id, occurrence_id, occurrence_id)
            chapters = rows(cursor)
        keys = ('signups','confirmed','waitlisted','cancelled','attended','absent','needs_attendance')
        if not any(row['chapter_id'] is None for row in chapters):
            chapters.insert(0, dict(chapter_id=None,chapter_name=None,chapter_code=None,**{k:0 for k in keys}))
        totals = {key:sum(int(row[key]) for row in chapters) for key in keys}
        return ready(dict(totals=totals,chapters=chapters,occurrence_id=occurrence_id,as_of_utc=now))

    def list_roster(self, *, event_id, occurrence_id, principal_id):
        with self._workshop_transaction(event_id, principal_id) as (cursor, event):
            occurrence = self._lock_date(cursor, event_id, occurrence_id)
            cursor.execute('''SELECT ro.registration_occurrence_id, ro.registration_id, ro.status,
                ro.attendance, ro.attendance_recorded_at_utc, ro.assigned_at_utc,
                c.first_name, c.last_name, c.email, c.city, c.state_province, c.country, c.ogs_handle
                FROM events.registration_occurrences ro JOIN events.registration_contacts c
                ON c.registration_id = ro.registration_id WHERE ro.occurrence_id = ?
                ORDER BY ro.assigned_at_utc, ro.registration_occurrence_id''', occurrence_id)
            roster = rows(cursor)
            cursor.execute('''SELECT email_id, registration_id, kind, status, due_at_utc,
                sent_at_utc, attempts, last_error FROM events.email_outbox
                WHERE event_id = ? AND occurrence_id = ? ORDER BY email_id DESC''', event_id, occurrence_id)
            return ready(dict(occurrence={k:v for k,v in occurrence.items() if k != 'row_version'},
                              roster=roster, emails=rows(cursor)))

    def set_ogs_handle(self, *, event_id, occurrence_id, selection_id, principal_id, ogs_handle, now):
        from event_domain import optional_text
        handle = optional_text(ogs_handle, 'OGS handle', 100)
        with self._workshop_transaction(event_id, principal_id) as (cursor, event):
            self._lock_date(cursor, event_id, occurrence_id)
            cursor.execute('''SELECT registration_id FROM events.registration_occurrences
                WHERE occurrence_id=? AND registration_occurrence_id=?''',occurrence_id,selection_id)
            selected=cursor.fetchone()
            if not selected:
                raise EventNotFound('Signup not found.')
            cursor.execute('UPDATE events.registration_contacts SET ogs_handle=? WHERE registration_id=?',handle,selected[0])
            self._audit_workshop(cursor,event_id,principal_id,'registration.ogs_handle_updated',
                dict(selection_id=selection_id,ogs_handle=handle),now,selected[0])
        return {'ogs_handle':handle}

    def set_attendance(self, *, event_id, occurrence_id, selection_id, principal_id, attendance, now):
        if attendance not in {'unrecorded', 'attended', 'absent'}:
            raise EventError('Choose unrecorded, attended or absent.')
        with self._workshop_transaction(event_id, principal_id) as (cursor, event):
            occurrence = self._lock_date(cursor, event_id, occurrence_id)
            effective_now=now
            if self._settings(cursor,event_id)['is_test']:
                from workshop_providers import email_delivery_config
                clock=self._rehearsal_clock(cursor,occurrence_id)
                if clock and email_delivery_config()['mode']=='test':effective_now=clock['simulated_at_utc']
            if effective_now < occurrence['starts_at_utc'] and attendance != 'unrecorded':
                raise EventConflict('Attendance can be recorded once the workshop starts.')
            cursor.execute('''SELECT registration_id, status FROM events.registration_occurrences
                WHERE occurrence_id = ? AND registration_occurrence_id = ?''', occurrence_id, selection_id)
            selected = cursor.fetchone()
            if not selected:
                raise EventNotFound('Signup not found.')
            if selected[1] != 'confirmed':
                raise EventConflict('Only confirmed signups can have attendance recorded.')
            cursor.execute('''UPDATE events.registration_occurrences SET attendance = ?,
                attendance_recorded_at_utc = ?, attendance_recorded_by = ? WHERE registration_occurrence_id = ?''',
                attendance, sql_time(now), principal_id, selection_id)
            cursor.execute("UPDATE events.email_outbox SET retry_after_utc=NULL WHERE occurrence_id=? AND registration_id=? AND kind='followup' AND status='pending' AND last_error='Waiting for attendance to be recorded.'",occurrence_id,selected[0])
            self._audit_workshop(cursor, event_id, principal_id, 'attendance.updated',
                                 dict(selection_id=selection_id, attendance=attendance), now, selected[0])
        return {'attendance': attendance}

    def cancel_signup(self, *, event_id, occurrence_id, selection_id, principal_id, now):
        with self._workshop_transaction(event_id, principal_id) as (cursor, event):
            occurrence = self._lock_date(cursor, event_id, occurrence_id)
            cursor.execute('''SELECT registration_id, status FROM events.registration_occurrences
                WHERE occurrence_id = ? AND registration_occurrence_id = ?''', occurrence_id, selection_id)
            selected = cursor.fetchone()
            if not selected:
                raise EventNotFound('Signup not found.')
            if selected[1] == 'cancelled':
                return {'status': 'cancelled'}
            self._cancel_selection(cursor, event_id, selected[0], occurrence_id, 'cancelled', now)
            self._audit_workshop(cursor, event_id, principal_id, 'registration.cancelled',
                                 {'selection_id': selection_id}, now, selected[0])
            self._promote_waitlist(cursor, event_id, occurrence, now)
        return {'status': 'cancelled'}

    def _cancel_selection(self, cursor, event_id, registration_id, occurrence_id, kind, now):
        cursor.execute('''UPDATE events.registration_occurrences SET status = 'cancelled', cancelled_at_utc = ?
            WHERE registration_id = ? AND occurrence_id = ?''', sql_time(now), registration_id, occurrence_id)
        cursor.execute('''UPDATE events.email_outbox SET status = 'suppressed' WHERE registration_id = ?
            AND occurrence_id = ? AND status = 'pending' ''', registration_id, occurrence_id)
        self._queue_email(cursor, event_id, registration_id, occurrence_id, kind, now)
        self._refresh_registration_status(cursor, registration_id, now)

    def cancel_occurrence(self, *, event_id, occurrence_id, principal_id, now):
        with self._workshop_transaction(event_id, principal_id, 'can_manage_events') as (cursor, event):
            occurrence = self._lock_date(cursor, event_id, occurrence_id)
            if occurrence['lifecycle'] == 'cancelled':
                return {'status': 'cancelled'}
            self._check_meeting_editable(cursor,occurrence_id)
            cursor.execute("UPDATE events.event_occurrences SET lifecycle = 'cancelled', updated_at_utc = ? WHERE occurrence_id = ?", sql_time(now), occurrence_id)
            cursor.execute("SELECT registration_id FROM events.registration_occurrences WHERE occurrence_id = ? AND status <> 'cancelled'", occurrence_id)
            for selected in cursor.fetchall():
                self._cancel_selection(cursor, event_id, selected[0], occurrence_id, 'workshop_cancelled', now)
            cursor.execute("UPDATE events.workshop_meetings SET sync_status = 'pending', updated_at_utc = ? WHERE occurrence_id = ? AND source = 'zoom'", sql_time(now), occurrence_id)
            self._audit_workshop(cursor, event_id, principal_id, 'occurrence.cancelled', {'occurrence_id': occurrence_id}, now)
        return {'status': 'cancelled'}

    def get_workshop_settings(self, *, event_id, principal_id):
        with self._workshop_transaction(event_id, principal_id, 'can_manage_events') as (cursor, event):
            result = self._settings(cursor, event_id)
            result['reminder_hours'] = json.loads(result['reminder_hours'])
            return result

    def save_workshop_settings(self, *, event_id, principal_id, payload, now):
        settings = normalize_settings(payload)
        with self._workshop_transaction(event_id, principal_id, 'can_manage_events') as (cursor, event):
            current = self._settings(cursor, event_id)
            if current['reminder_hours']!=settings['reminder_hours'] or current['followup_hours']!=settings['followup_hours']:
                cursor.execute('SELECT COUNT(*) FROM events.rehearsal_clocks rc JOIN events.event_occurrences o ON o.occurrence_id=rc.occurrence_id WHERE o.event_id=? AND rc.stage>0 AND o.lifecycle=\'published\'',event_id)
                if cursor.fetchone()[0]:raise EventConflict('Keep reminder timing fixed while a rehearsal is in progress. Email wording can still be edited.')
            if current['zoom_host_email'] != settings['zoom_host_email']:
                cursor.execute('''SELECT COUNT(*) FROM events.workshop_meetings m
                    JOIN events.event_occurrences o ON o.occurrence_id = m.occurrence_id
                    WHERE o.event_id = ? AND m.source = 'zoom' AND m.sync_status <> 'cancelled' ''', event_id)
                if cursor.fetchone()[0]:
                    raise EventConflict('Finish or cancel existing Zoom meetings before changing their host.')
            cursor.execute('''IF NOT EXISTS (SELECT 1 FROM events.workshop_settings WHERE event_id = ?)
                INSERT INTO events.workshop_settings(event_id) VALUES (?)''', event_id, event_id)
            cursor.execute('''UPDATE events.workshop_settings SET sender_email = ?, instructor_name = ?,
                reminder_hours = ?, followup_hours = ?, followup_text = ?, email_enabled = ?, zoom_host_email = ?
                WHERE event_id = ?''', settings['sender_email'], settings['instructor_name'], settings['reminder_hours'],
                settings['followup_hours'], settings['followup_text'], settings['email_enabled'], settings['zoom_host_email'], event_id)
            # Pick up pilot signups that predate this migration. Never resend a
            # confirmation merely because an operator saves settings again.
            cursor.execute('''SELECT ro.registration_id,ro.status,o.occurrence_id,o.starts_at_utc,o.ends_at_utc
                FROM events.registration_occurrences ro JOIN events.event_occurrences o ON o.occurrence_id=ro.occurrence_id
                WHERE o.event_id=? AND o.lifecycle='published' AND o.starts_at_utc>?
                AND ro.status IN ('confirmed','waitlisted')''',event_id,sql_time(now))
            selections=rows(cursor)
            timing_changed=(current['reminder_hours']!=settings['reminder_hours'] or current['followup_hours']!=settings['followup_hours'])
            revision=str(uuid4())
            for selected in selections:
                rid=selected['registration_id'];oid=selected['occurrence_id']
                cursor.execute('SELECT COUNT(*) FROM events.email_outbox WHERE registration_id=? AND occurrence_id=?',rid,oid)
                if cursor.fetchone()[0]==0:
                    self._queue_registration_emails(cursor,event_id,rid,selected,selected['status'],now)
                elif timing_changed and selected['status']=='confirmed':
                    cursor.execute("UPDATE events.email_outbox SET status='suppressed' WHERE registration_id=? AND occurrence_id=? AND status='pending' AND (kind LIKE 'reminder_%' OR kind='followup')",rid,oid)
                    for kind,due in scheduled_messages(selected['starts_at_utc'],selected['ends_at_utc'],settings,now):
                        # A reminder already delivered must not be sent again after a settings edit.
                        cursor.execute("SELECT COUNT(*) FROM events.email_outbox WHERE registration_id=? AND occurrence_id=? AND kind=? AND status IN ('sent','sending','uncertain')",rid,oid,kind)
                        if not cursor.fetchone()[0]:self._queue_email(cursor,event_id,rid,oid,kind,due,revision)
            self._audit_workshop(cursor, event_id, principal_id, 'workshop.settings.updated', settings, now)
        return settings
