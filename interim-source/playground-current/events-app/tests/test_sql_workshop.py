"""Opt-in real SQL Server tests, using a unique disposable schema.

Set EVENTS_TEST_SQL_CONNECTION_STRING. No production events schema is altered.
All repository SQL is redirected to the disposable schema, including migrations.
Provider adapters are fakes: these tests never send mail or call Zoom.
"""
import os
import json
import re
import unittest
from pathlib import Path
from uuid import uuid4
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from sql_repository import SqlEventRepository
from event_domain import normalize_contact, EventConflict, EventForbidden, EventNotFound, EventError
from workshop_worker import process_email, process_zoom, prepare_meetings
from workshop_providers import ProviderError


class RedirectCursor:
    def __init__(self, cursor, schema):self.cursor=cursor;self.schema=schema
    def execute(self, sql, *params):
        sql=sql.replace('events.',self.schema+'.').replace('events_',self.schema+'_')
        sql=sql.replace("name = N'events'",f"name = N'{self.schema}'").replace('CREATE SCHEMA events ',f'CREATE SCHEMA {self.schema} ')
        self.cursor.execute(sql,*params);return self
    def __getattr__(self,name):return getattr(self.cursor,name)


class RedirectConnection:
    def __init__(self,conn,schema):self.conn=conn;self.schema=schema
    def cursor(self):return RedirectCursor(self.conn.cursor(),self.schema)
    def __getattr__(self,name):return getattr(self.conn,name)
    def __enter__(self):return self
    def __exit__(self,typ,value,traceback):
        try:
            if typ:self.conn.rollback()
            else:self.conn.commit()
        finally:self.conn.close()


class TestRepository(SqlEventRepository):
    def _connect(self):return RedirectConnection(super()._connect(),self.schema)


@unittest.skipUnless(os.environ.get('EVENTS_TEST_SQL_CONNECTION_STRING'), 'Set an explicit SQL test connection to run database integration tests.')
class SqlWorkshopTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo=TestRepository(os.environ['EVENTS_TEST_SQL_CONNECTION_STRING'])
        cls.repo.schema='workshop_test_'+uuid4().hex[:12]
        cls.addClassCleanup(cls.cleanup_schema)
        with cls.repo._connect() as conn:
            c=conn.cursor()
            for name in ['001_repeating_event_foundation.sql','004_workshop_operations.sql','005_registrant_ogs_handle.sql','006_workshop_test_mode.sql','007_rehearsal_clock.sql','008_registration_chapter_referral.sql']:
                c.execute((Path(__file__).parents[1]/'sql'/name).read_text(encoding='utf-8'))
                while c.nextset():pass
            conn.commit()

    @classmethod
    def cleanup_schema(cls):
        schema=cls.repo.schema
        assert re.fullmatch(r'workshop_test_[a-f0-9]{12}',schema)
        # Cleanup is limited to objects whose schema is exactly this test's random schema.
        conn=super(TestRepository,cls.repo)._connect()
        try:
            c=conn.cursor()
            c.execute('''SELECT t.name, fk.name FROM sys.foreign_keys fk JOIN sys.tables t ON t.object_id=fk.parent_object_id
                         JOIN sys.schemas s ON s.schema_id=t.schema_id WHERE s.name=?''',schema)
            for table,fk in c.fetchall():
                assert ']' not in table+fk
                c.execute(f'ALTER TABLE [{schema}].[{table}] DROP CONSTRAINT [{fk}]')
            c.execute('SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id WHERE s.name=?',schema)
            for row in c.fetchall():
                assert ']' not in row[0]
                c.execute(f'DROP TABLE [{schema}].[{row[0]}]')
            c.execute('SELECT 1 FROM sys.schemas WHERE name=?',schema)
            if c.fetchone():c.execute(f'DROP SCHEMA [{schema}]')
            conn.commit()
        finally:conn.close()

    def setUp(self):
        self.delivery_patch=patch.dict(os.environ,{'EVENTS_EMAIL_MODE':'live'})
        self.delivery_patch.start();self.addCleanup(self.delivery_patch.stop)
        self.now=datetime(2026,9,7,12,tzinfo=timezone.utc)
        with self.repo._connect() as conn:
            c=conn.cursor()
            c.execute('UPDATE events.events SET current_event_version_id=NULL')
            for table in ['rehearsal_clocks','email_outbox','workshop_meetings','workshop_settings','audit_log','registration_occurrences','registration_contacts','registrations','event_occurrences','event_versions','events','organizer_memberships','organizers']:
                c.execute('DELETE FROM events.'+table)
            c.execute("INSERT INTO events.organizers(organizer_key,name) OUTPUT inserted.organizer_id VALUES ('test','Test')")
            org=c.fetchone()[0]
            c.execute('''INSERT INTO events.organizer_memberships(organizer_id,principal_object_id,can_manage_events,can_manage_registrations)
                VALUES (?,'admin',1,1),(?,'dates-only',1,0)''',org,org)
            c.execute("INSERT INTO events.events(organizer_id,slug,name,lifecycle) OUTPUT inserted.event_id VALUES (?,'test','Test workshop','published')",org)
            self.eid=c.fetchone()[0]
            c.execute('INSERT INTO events.event_versions(event_id,version_number) OUTPUT inserted.event_version_id VALUES (?,1)',self.eid)
            version=c.fetchone()[0]
            c.execute('UPDATE events.events SET current_event_version_id=? WHERE event_id=?',version,self.eid)
            conn.commit()
        self.date=dict(occurrence_key='test-date',title='Test date',time_zone='America/New_York',
            starts_at_utc=self.now+timedelta(days=2),ends_at_utc=self.now+timedelta(days=2,hours=1),
            registration_opens_at_utc=None,registration_closes_at_utc=self.now+timedelta(days=2),
            capacity=1,waitlist_enabled=True,location_name='Online')
        self.oid=self.repo.add_occurrence(event_id=self.eid,principal_id='admin',occurrence=self.date,now=self.now)['occurrence_id']

    def register(self,email='a@example.com',key=None):
        return self.repo.register_free(slug='test',occurrence_ids=[self.oid],
            contact=normalize_contact(dict(first_name='Test',last_name='Person',email=email)),idempotency_key=key or str(uuid4()),now=self.now)

    def roster(self):return self.repo.list_roster(event_id=self.eid,occurrence_id=self.oid,principal_id='admin')

    def test_chapter_referral_persists_and_retry_keeps_original_attribution(self):
        chapters = self.repo.list_referral_chapters()
        chapter = next(row for row in chapters if row['chapter_code'] == 'NGOC')
        contact = normalize_contact(dict(first_name='Chapter',last_name='Test',email='chapter@example.com'))
        args = dict(slug='test',occurrence_ids=[self.oid],contact=contact,idempotency_key='referral-test',now=self.now)
        result = self.repo.register_free(**args,referral_chapter_id=str(chapter['chapter_id']))
        again = self.repo.register_free(**args,referral_chapter_id=None)
        self.assertEqual(result['registration_id'],again['registration_id'])
        with self.repo._connect() as conn:
            c=conn.cursor()
            c.execute('SELECT referral_chapter_id FROM events.registrations WHERE registration_id = ?',result['registration_id'])
            self.assertEqual(c.fetchone()[0],chapter['chapter_id'])
        for index,ref in enumerate([None,'nonsense','2147483647']):
            contact = normalize_contact(dict(first_name='Other',last_name='Test',email=f'other{index}@example.com'))
            row = self.repo.register_free(slug='test',occurrence_ids=[self.oid],contact=contact,
                idempotency_key=f'no-ref-{index}',now=self.now,referral_chapter_id=ref)
            with self.repo._connect() as conn:
                c=conn.cursor()
                c.execute('SELECT referral_chapter_id FROM events.registrations WHERE registration_id = ?',row['registration_id'])
                self.assertIsNone(c.fetchone()[0])

    def test_registration_statistics_history_outcomes_filters_and_permissions(self):
        ngoc=next(row['chapter_id'] for row in self.repo.list_referral_chapters() if row['chapter_code']=='NGOC')
        with self.repo._connect() as conn:
            conn.cursor().execute('UPDATE events.event_occurrences SET capacity=10 WHERE occurrence_id=?',self.oid)
        ids=[]
        for index in range(5):
            result=self.repo.register_free(slug='test',occurrence_ids=[self.oid],
                contact=normalize_contact(dict(first_name='Stats',last_name='Test',email=f'stats{index}@example.com')),
                idempotency_key=f'stats-{index}',now=self.now,referral_chapter_id=ngoc if index>=2 else None)
            ids.append(result['registration_id'])
        second=self.repo.add_occurrence(event_id=self.eid,principal_id='admin',now=self.now,
            occurrence=dict(self.date,occurrence_key='second',starts_at_utc=self.now+timedelta(days=5),ends_at_utc=self.now+timedelta(days=5,hours=1)))['occurrence_id']
        empty=self.repo.add_occurrence(event_id=self.eid,principal_id='admin',now=self.now,
            occurrence=dict(self.date,occurrence_key='empty',starts_at_utc=self.now+timedelta(days=7),ends_at_utc=self.now+timedelta(days=7,hours=1)))['occurrence_id']
        with self.repo._connect() as conn:
            c=conn.cursor()
            for rid,status,attendance in [(ids[0],'confirmed','attended'),(ids[1],'confirmed','unrecorded'),
                    (ids[2],'confirmed','absent'),(ids[3],'waitlisted','unrecorded'),(ids[4],'cancelled','attended')]:
                c.execute('UPDATE events.registration_occurrences SET status=?,attendance=? WHERE registration_id=?',status,attendance,rid)
            c.execute("INSERT INTO events.registration_occurrences(registration_id,occurrence_id,status,assigned_at_utc) VALUES (?,?,'confirmed',?)",ids[0],second,self.now.replace(tzinfo=None))
        now=self.now+timedelta(days=3)
        args=dict(event_id=self.eid,principal_id='admin',now=now)
        stats=self.repo.registration_statistics(**args)
        self.assertEqual(stats['totals'],dict(signups=6,confirmed=4,waitlisted=1,cancelled=1,attended=1,absent=1,needs_attendance=1))
        self.assertEqual([(row['chapter_id'],row['signups']) for row in stats['chapters']],[(None,3),(ngoc,3)])
        filtered=self.repo.registration_statistics(**args,occurrence_id=self.oid)
        self.assertEqual(filtered['totals']['signups'],5)
        self.assertEqual(filtered['totals']['attended'],1) # Cancelled signup's former attendance is excluded.
        self.assertEqual(self.repo.registration_statistics(**args,occurrence_id=second)['totals']['needs_attendance'],0)
        self.assertEqual(self.repo.registration_statistics(**args,occurrence_id=empty)['totals']['signups'],0)
        for principal in ['dates-only','outsider']:
            with self.assertRaises(EventForbidden):self.repo.registration_statistics(event_id=self.eid,principal_id=principal,now=now)
        with self.assertRaises(EventForbidden):self.repo.registration_statistics(event_id=self.eid+99999,principal_id='admin',now=now)
        with self.assertRaises(EventNotFound):self.repo.registration_statistics(**args,occurrence_id=999999)

    def enable_email(self):
        self.repo.save_workshop_settings(event_id=self.eid,principal_id='admin',now=self.now,
            payload=dict(sender_email='milan.mladenovic@usgo.org',followup_text='Thanks',email_enabled=True,zoom_host_email='host@example.com'))
        with self.repo._connect() as conn:
            conn.cursor().execute("INSERT INTO events.workshop_meetings(occurrence_id,join_url,source,sync_status) VALUES (?,'https://zoom.us/j/123','manual','ready')",self.oid)
            conn.commit()

    def test_public_cutoff_all_dates_and_stale_submission(self):
        # Hidden legacy windows cannot restrict the date choices anymore.
        with self.repo._connect() as conn:
            conn.cursor().execute('UPDATE events.event_occurrences SET registration_opens_at_utc=?, registration_closes_at_utc=? WHERE occurrence_id=?',
                (self.now+timedelta(hours=1)).replace(tzinfo=None),
                (self.now+timedelta(hours=2)).replace(tzinfo=None), self.oid)
        self.register()
        for index in range(6):
            self.repo.add_occurrence(event_id=self.eid,principal_id='admin',now=self.now,
                occurrence=dict(self.date,occurrence_key=f'future-{index}',
                    starts_at_utc=self.now+timedelta(days=10+index),ends_at_utc=self.now+timedelta(days=10+index,hours=1)))
        self.assertEqual(len(self.repo.get_public_event('test',self.now)['occurrences']),7)
        self.now=self.date['starts_at_utc']-timedelta(hours=24)
        self.assertEqual(len(self.repo.get_public_event('test',self.now)['occurrences']),7)
        self.register('boundary@example.com')
        self.now+=timedelta(seconds=1)
        self.assertEqual(len(self.repo.get_public_event('test',self.now)['occurrences']),6)
        with self.assertRaisesRegex(EventConflict,'24 hours'):
            self.register('stale@example.com')

    def test_public_cutoff_uses_rehearsal_clock_without_exposing_it(self):
        with self.repo._connect() as conn:
            conn.cursor().execute('INSERT INTO events.rehearsal_clocks(occurrence_id,stage,simulated_at_utc) VALUES (?,2,?)',
                self.oid,(self.date['starts_at_utc']-timedelta(hours=1)).replace(tzinfo=None))
        with patch.dict(os.environ,{'EVENTS_EMAIL_MODE':'test','EVENTS_EMAIL_TEST_RECIPIENT':'gurujeet.khalsa@usgo.org'}):
            self.assertEqual(self.repo.get_public_event('test',self.now)['occurrences'],[])
            with self.assertRaisesRegex(EventConflict,'rehearsal step'):
                self.register()
        self.assertEqual(len(self.repo.get_public_event('test',self.now)['occurrences']),1)

    def test_parallel_last_place_and_atomic_email_queue(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(self.register,['a@example.com','b@example.com']))
        self.assertEqual(sorted(r['status'] for r in results),['confirmed','waitlisted'])
        result=self.roster()
        self.assertEqual(len(result['roster']),2)
        self.assertEqual(len(result['emails']),5) # confirmation + 2 reminders + followup + waitlist

    def test_ogs_handle_persists_clears_and_is_scoped_to_the_signup(self):
        self.register();self.register('b@example.com')
        original=self.roster()
        selection=original['roster'][0]['registration_occurrence_id']
        args=dict(event_id=self.eid,occurrence_id=self.oid,selection_id=selection,principal_id='admin',now=self.now)
        self.assertIsNone(original['roster'][0]['ogs_handle'])
        self.repo.set_ogs_handle(**args,ogs_handle='  AdaGo  ')
        self.assertEqual(self.roster()['roster'][0]['ogs_handle'],'AdaGo')
        self.assertIsNone(self.roster()['roster'][1]['ogs_handle'])
        self.assertNotIn('AdaGo',json.dumps(self.repo.get_public_event('test',self.now)))
        self.assertEqual(len(self.roster()['emails']),len(original['emails']))
        with self.assertRaises(EventForbidden):
            self.repo.set_ogs_handle(**dict(args,principal_id='dates-only'),ogs_handle='NoAccess')
        other=self.repo.add_occurrence(event_id=self.eid,principal_id='admin',occurrence=dict(self.date,occurrence_key='other'),now=self.now)['occurrence_id']
        with self.assertRaises(EventNotFound):
            self.repo.set_ogs_handle(**dict(args,occurrence_id=other),ogs_handle='WrongSignup')
        for invalid in (False,123,'x'*101):
            with self.assertRaises(EventError):self.repo.set_ogs_handle(**args,ogs_handle=invalid)
        self.assertEqual(self.roster()['roster'][0]['ogs_handle'],'AdaGo')
        with self.repo._connect() as conn:
            c=conn.cursor();c.execute((Path(__file__).parents[1]/'sql/005_registrant_ogs_handle.sql').read_text())
            while c.nextset():pass
        self.assertEqual(self.roster()['roster'][0]['ogs_handle'],'AdaGo')
        self.repo.set_ogs_handle(**args,ogs_handle=' ')
        self.assertIsNone(self.roster()['roster'][0]['ogs_handle'])

    def test_retry_is_idempotent_and_changed_submission_conflicts(self):
        first=self.register(key='stable');second=self.register(key='stable')
        self.assertEqual(first['registration_id'],second['registration_id'])
        self.assertEqual(len(self.roster()['emails']),4)
        with self.assertRaises(EventConflict):self.register('b@example.com','stable')
        with self.assertRaises(EventConflict):self.register()

    def test_cancel_promotes_next_waitlisted_and_suppresses_old_mail(self):
        self.register();self.register('b@example.com')
        selection=self.roster()['roster'][0]['registration_occurrence_id']
        args=dict(event_id=self.eid,occurrence_id=self.oid,selection_id=selection,principal_id='admin',now=self.now)
        self.repo.cancel_signup(**args);self.repo.cancel_signup(**args)
        result=self.roster()
        self.assertEqual([r['status'] for r in result['roster']],['cancelled','confirmed'])
        pending=[r for r in result['emails'] if r['status']=='pending']
        self.assertEqual(sorted(r['kind'] for r in pending),['cancelled','confirmed','followup','reminder_1','reminder_24'])

    def test_cancel_date_blocks_registration_and_cancels_all(self):
        self.register();self.register('b@example.com')
        self.repo.cancel_occurrence(event_id=self.eid,occurrence_id=self.oid,principal_id='admin',now=self.now)
        self.assertTrue(all(r['status']=='cancelled' for r in self.roster()['roster']))
        with self.assertRaises(EventConflict):self.register('c@example.com')
        self.assertEqual(self.repo.get_public_event('test',self.now)['occurrences'],[])

    def test_permissions_and_cross_event_date_are_enforced(self):
        with self.assertRaises(EventForbidden):self.repo.list_roster(event_id=self.eid,occurrence_id=self.oid,principal_id='dates-only')
        with self.assertRaises(EventForbidden):self.repo.cancel_occurrence(event_id=self.eid,occurrence_id=self.oid,principal_id='stranger',now=self.now)
        with self.assertRaises(EventNotFound):self.repo.cancel_occurrence(event_id=self.eid,occurrence_id=self.oid+10000,principal_id='admin',now=self.now)

    def test_attendance_and_past_date_visibility(self):
        self.register();self.register('b@example.com');roster=self.roster()['roster']
        args=dict(event_id=self.eid,occurrence_id=self.oid,principal_id='admin',attendance='attended')
        with self.assertRaises(EventConflict):self.repo.set_attendance(**args,selection_id=roster[0]['registration_occurrence_id'],now=self.now)
        after=self.now+timedelta(days=3)
        self.repo.set_attendance(**args,selection_id=roster[0]['registration_occurrence_id'],now=after)
        with self.assertRaises(EventConflict):self.repo.set_attendance(**args,selection_id=roster[1]['registration_occurrence_id'],now=after)
        self.assertEqual(self.roster()['roster'][0]['attendance'],'attended')
        self.assertEqual(len(self.repo.list_managed_occurrences(event_id=self.eid,principal_id='admin',now=after)['occurrences']),1)

    def test_worker_sends_confirmation_once_and_holds_uncertain_delivery(self):
        self.enable_email();self.register()
        class Sender:
            calls=0
            def send(self,message):self.calls+=1;return 'message1'
        sender=Sender()
        self.assertTrue(process_email(self.repo,sender,self.now))
        self.assertFalse(process_email(self.repo,sender,self.now))
        self.assertEqual(sender.calls,1)
        self.register('b@example.com')
        class FailingSender:
            def send(self,message):raise ProviderError('Timeout',uncertain=True)
        process_email(self.repo,FailingSender(),self.now)
        self.assertIn('uncertain',[m['status'] for m in self.roster()['emails']])
        self.assertFalse(process_email(self.repo,sender,self.now))

    def test_manual_signup_can_skip_confirmation_without_skipping_reminders(self):
        self.enable_email()
        args=dict(slug='test',occurrence_ids=[self.oid],
            contact=normalize_contact(dict(first_name='Test',last_name='Person',email='manual@example.com')),
            idempotency_key='manual-no-confirmation',now=self.now,principal_id='admin',managed_event_id=self.eid)
        result=self.repo.register_free(**args,send_confirmation_email=False)
        self.assertEqual(self.roster()['roster'][0]['status'],'confirmed')
        self.assertEqual({m['kind']:m['status'] for m in self.roster()['emails']},
            {'confirmed':'suppressed','reminder_24':'pending','reminder_1':'pending','followup':'pending'})
        messages=[]
        class Sender:
            def send(self,message):messages.append(message);return 'test-message'
        self.assertFalse(process_email(self.repo,Sender(),self.now))
        # A retry, even with a changed checkbox, cannot recreate the initial email.
        self.assertEqual(self.repo.register_free(**args,send_confirmation_email=True),result)
        self.repo.save_workshop_settings(event_id=self.eid,principal_id='admin',now=self.now,
            payload=dict(sender_email='milan.mladenovic@usgo.org',followup_text='Updated text',email_enabled=True,zoom_host_email='host@example.com'))
        self.assertFalse(process_email(self.repo,Sender(),self.now))
        self.assertTrue(process_email(self.repo,Sender(),self.date['starts_at_utc']-timedelta(hours=24)))
        self.assertEqual(len(messages),1)
        self.assertIn('Workshop tomorrow',messages[0]['Subject'])
        self.assertEqual(next(m for m in self.roster()['emails'] if m['kind']=='confirmed')['status'],'suppressed')
        with self.repo._connect() as conn:
            c=conn.cursor();c.execute("SELECT detail_json FROM events.audit_log WHERE action='registration.added_by_admin'")
            self.assertIs(json.loads(c.fetchone()[0])['send_confirmation_email'],False)

    def test_manual_signup_sends_confirmation_by_default(self):
        self.enable_email()
        self.repo.register_free(slug='test',occurrence_ids=[self.oid],
            contact=normalize_contact(dict(first_name='Test',last_name='Person',email='manual@example.com')),
            idempotency_key='manual-default',now=self.now,principal_id='admin',managed_event_id=self.eid)
        messages=[]
        class Sender:
            def send(self,message):messages.append(message);return 'test-message'
        self.assertTrue(process_email(self.repo,Sender(),self.now))
        self.assertFalse(process_email(self.repo,Sender(),self.now))
        self.assertEqual(len(messages),1)
        self.assertIn('Registration confirmed',messages[0]['Subject'])

    def test_only_registration_managers_can_skip_confirmation(self):
        args=dict(slug='test',occurrence_ids=[self.oid],
            contact=normalize_contact(dict(first_name='Test',last_name='Person',email='manual@example.com')),
            idempotency_key='manual-forbidden',now=self.now,send_confirmation_email=False)
        with self.assertRaises(EventForbidden):self.repo.register_free(**args)
        with self.assertRaises(EventForbidden):self.repo.register_free(**args,principal_id='dates-only',managed_event_id=self.eid)
        self.assertEqual(self.roster()['roster'],[])
        self.assertEqual(self.roster()['emails'],[])

    def test_worker_does_not_send_stale_reminders_after_cancellation(self):
        self.enable_email();self.register()
        selection=self.roster()['roster'][0]['registration_occurrence_id']
        self.repo.cancel_signup(event_id=self.eid,occurrence_id=self.oid,selection_id=selection,principal_id='admin',now=self.now)
        messages=[]
        class Sender:
            def send(self,message):messages.append(message);return 'message1'
        while process_email(self.repo,Sender(),self.now+timedelta(hours=25)):pass
        self.assertEqual(len(messages),1);self.assertIn('Registration cancelled',messages[0]['Subject'])

    def test_registration_rolls_back_if_email_queue_fails(self):
        with patch.object(self.repo,'_queue_email',side_effect=RuntimeError('simulated outbox failure')):
            with self.assertRaises(RuntimeError):self.register()
        self.assertEqual(self.roster()['roster'],[])
        self.assertEqual(self.roster()['emails'],[])
        self.assertEqual(self.register()['status'],'confirmed')

    def test_migration_can_be_applied_twice(self):
        with self.repo._connect() as conn:
            c=conn.cursor();c.execute((Path(__file__).parents[1]/'sql/004_workshop_operations.sql').read_text())
            while c.nextset():pass
        self.assertEqual(self.register()['status'],'confirmed')

    def test_zoom_create_sync_cancel_and_no_public_join_details(self):
        self.repo.save_workshop_settings(event_id=self.eid,principal_id='admin',now=self.now,
            payload=dict(sender_email='milan.mladenovic@usgo.org',followup_text='Thanks',zoom_host_email='host@example.com'))
        class Zoom:
            calls=[]
            def create(self,*args):self.calls.append('create');return dict(meeting_id='123',join_url='https://zoom.us/j/123')
            def update(self,*args):self.calls.append('update')
            def delete(self,*args):self.calls.append('delete')
        zoom=Zoom();prepare_meetings(self.repo,self.now)
        self.assertTrue(process_zoom(self.repo,zoom,self.now));self.assertFalse(process_zoom(self.repo,zoom,self.now))
        self.assertNotIn('join_url',json.dumps(self.repo.get_public_event('test',self.now)))
        self.repo.update_occurrence(event_id=self.eid,occurrence_id=self.oid,principal_id='admin',now=self.now,
            occurrence=dict(self.date,title='New title'))
        process_zoom(self.repo,zoom,self.now)
        self.repo.cancel_occurrence(event_id=self.eid,occurrence_id=self.oid,principal_id='admin',now=self.now)
        process_zoom(self.repo,zoom,self.now)
        self.assertEqual(zoom.calls,['create','update','delete'])
        self.assertEqual(self.repo.get_meeting(event_id=self.eid,occurrence_id=self.oid,principal_id='admin')['sync_status'],'cancelled')

    def test_uncertain_zoom_create_is_not_replayed_by_date_edit(self):
        self.repo.save_workshop_settings(event_id=self.eid,principal_id='admin',now=self.now,
            payload=dict(sender_email='milan.mladenovic@usgo.org',followup_text='Thanks',zoom_host_email='host@example.com'))
        class Zoom:
            calls=0
            def create(self,*args):self.calls+=1;raise ProviderError('Timeout',uncertain=True)
        zoom=Zoom();prepare_meetings(self.repo,self.now);process_zoom(self.repo,zoom,self.now)
        with self.assertRaises(EventConflict):self.repo.update_occurrence(event_id=self.eid,occurrence_id=self.oid,principal_id='admin',now=self.now,occurrence=self.date)
        self.assertFalse(process_zoom(self.repo,zoom,self.now));self.assertEqual(zoom.calls,1)

    def test_confirmations_wait_for_join_details_and_followup_is_delivered(self):
        self.repo.save_workshop_settings(event_id=self.eid,principal_id='admin',now=self.now,
            payload=dict(sender_email='milan.mladenovic@usgo.org',followup_text='Play Go again!',email_enabled=True))
        self.register();messages=[]
        class Sender:
            def send(self,message):messages.append(message);return 'message1'
        sender=Sender();process_email(self.repo,sender,self.now)
        self.assertEqual(messages,[])
        self.repo.configure_meeting(event_id=self.eid,occurrence_id=self.oid,principal_id='admin',
            payload=dict(source='manual',join_url='https://zoom.us/j/123'),now=self.now)
        process_email(self.repo,sender,self.now+timedelta(minutes=6))
        self.assertIn('Registration confirmed',messages[0]['Subject'])
        after=self.now+timedelta(days=4)
        self.repo.set_attendance(event_id=self.eid,occurrence_id=self.oid,principal_id='admin',
            selection_id=self.roster()['roster'][0]['registration_occurrence_id'],attendance='attended',now=after)
        while process_email(self.repo,sender,after):pass
        self.assertEqual(len(messages),2)
        self.assertIn('Play Go again!',messages[-1].get_content())

    def test_followup_holds_unrecorded_then_uses_latest_attended_text_once(self):
        self.enable_email();self.register();messages=[]
        class Sender:
            def send(self,message):messages.append(message);return 'message1'
        sender=Sender();after=self.now+timedelta(days=4)
        while process_email(self.repo,sender,after):pass
        self.assertEqual(messages,[])
        followup=next(m for m in self.roster()['emails'] if m['kind']=='followup')
        self.assertEqual(followup['status'],'pending')
        self.assertEqual(followup['last_error'],'Waiting for attendance to be recorded.')
        with self.repo._connect() as conn:
            self.assertEqual(conn.cursor().execute('SELECT attempts FROM events.email_outbox WHERE email_id=?',followup['email_id']).fetchone()[0],0)
        settings=self.repo.get_workshop_settings(event_id=self.eid,principal_id='admin')
        from workshop_domain import DEFAULT_ATTENDED_FOLLOWUP
        settings['followup_text']=DEFAULT_ATTENDED_FOLLOWUP+'\n\nExtra edited line.'
        self.repo.save_workshop_settings(event_id=self.eid,principal_id='admin',payload=settings,now=after)
        args=dict(event_id=self.eid,occurrence_id=self.oid,principal_id='admin',
                  selection_id=self.roster()['roster'][0]['registration_occurrence_id'],now=after)
        self.repo.set_attendance(**args,attendance='attended')
        while process_email(self.repo,sender,after+timedelta(minutes=6)):pass
        self.assertEqual(len(messages),1)
        self.assertEqual(self.repo.get_workshop_settings(event_id=self.eid,principal_id='admin')['followup_text'],settings['followup_text'])
        self.assertIn(settings['followup_text'].replace('**',''),messages[0].get_body(preferencelist=('plain',)).get_content())
        self.assertIn('<li>8 weekly online classes</li>',messages[0].get_body(preferencelist=('html',)).get_content())
        self.repo.set_attendance(**args,attendance='absent')
        self.assertFalse(process_email(self.repo,sender,after+timedelta(minutes=12)))
        self.assertEqual(len(messages),1)

    def test_unrecorded_does_not_block_absent_followup_for_another_person(self):
        self.repo.update_occurrence(event_id=self.eid,occurrence_id=self.oid,principal_id='admin',now=self.now,
                                   occurrence=dict(self.date,capacity=2))
        self.enable_email();self.register();self.register('b@example.com');messages=[]
        after=self.now+timedelta(days=4)
        self.repo.set_attendance(event_id=self.eid,occurrence_id=self.oid,principal_id='admin',
            selection_id=self.roster()['roster'][1]['registration_occurrence_id'],attendance='absent',now=after)
        class Sender:
            def send(self,message):messages.append(message);return 'message1'
        with patch.dict(os.environ,{'EVENTS_PUBLIC_BASE_URL':'https://workshop.test/'}):
            while process_email(self.repo,Sender(),after):pass
        self.assertEqual(len(messages),1)
        self.assertEqual(messages[0]['To'].addresses[0].addr_spec,'b@example.com')
        self.assertIn('https://workshop.test/events/test',messages[0].get_body(preferencelist=('plain',)).get_content())
        self.assertIn("weren't able to attend",messages[0].get_body(preferencelist=('plain',)).get_content())
        self.assertNotIn('Thanks',messages[0].get_body(preferencelist=('plain',)).get_content())
        self.assertEqual(sorted(m['status'] for m in self.roster()['emails'] if m['kind']=='followup'),['pending','sent'])

    def test_existing_pilot_signups_are_backfilled_only_once(self):
        self.register()
        with self.repo._connect() as conn:conn.cursor().execute('DELETE FROM events.email_outbox')
        args=dict(event_id=self.eid,principal_id='admin',now=self.now,
            payload=dict(sender_email='milan.mladenovic@usgo.org',followup_text='Thanks',email_enabled=True))
        self.repo.save_workshop_settings(**args);self.repo.save_workshop_settings(**args)
        self.assertEqual(len(self.roster()['emails']),4)

    def test_test_and_live_queues_are_isolated(self):
        self.enable_email();self.register();messages=[]
        class Sender:
            def send(self,message):messages.append(message);return 'test-receipt'
        before=self.roster()['emails']
        with patch.dict(os.environ,{'EVENTS_EMAIL_MODE':'test','EVENTS_EMAIL_TEST_RECIPIENT':'gurujeet.khalsa@usgo.org'}):
            self.assertFalse(process_email(self.repo,Sender(),self.now))
        self.assertEqual(self.roster()['emails'],before)
        with self.repo._connect() as conn:
            conn.cursor().execute('UPDATE events.workshop_settings SET is_test=1 WHERE event_id=?',self.eid)
        self.assertFalse(process_email(self.repo,Sender(),self.now))
        with patch.dict(os.environ,{'EVENTS_EMAIL_MODE':'test','EVENTS_EMAIL_TEST_RECIPIENT':'gurujeet.khalsa@usgo.org'}):
            self.assertTrue(process_email(self.repo,Sender(),self.now))
            self.assertFalse(process_email(self.repo,Sender(),self.now))
        self.assertEqual(len(messages),1)
        self.assertEqual(messages[0]['To'].addresses[0].display_name,'Test Person')
        self.assertFalse(process_email(self.repo,Sender(),self.now+timedelta(days=4)))

    def test_settings_cannot_change_series_test_classification(self):
        self.enable_email()
        settings=self.repo.get_workshop_settings(event_id=self.eid,principal_id='admin')
        self.assertFalse(settings['is_test'])
        settings['is_test']=True
        self.repo.save_workshop_settings(event_id=self.eid,principal_id='admin',payload=settings,now=self.now)
        self.assertFalse(self.repo.get_workshop_settings(event_id=self.eid,principal_id='admin')['is_test'])

    def test_manual_rehearsal_clock_pauses_for_attendance_and_never_replays(self):
        self.enable_email()
        self.repo.update_occurrence(event_id=self.eid,occurrence_id=self.oid,principal_id='admin',now=self.now,occurrence=dict(self.date,capacity=2))
        with self.repo._connect() as conn:conn.cursor().execute('UPDATE events.workshop_settings SET is_test=1 WHERE event_id=?',self.eid)
        args=dict(event_id=self.eid,occurrence_id=self.oid,principal_id='admin',now=self.now)
        messages=[]
        class Sender:
            def send(self,message):messages.append(message);return 'test-message'
        def drain(wall=None):
            for _ in range(20):
                if not process_email(self.repo,Sender(),wall or self.now):return
            self.fail('Worker did not stop at the paused clock.')
        with patch.dict(os.environ,{'EVENTS_EMAIL_MODE':'test','EVENTS_EMAIL_TEST_RECIPIENT':'gurujeet.khalsa@usgo.org'}):
            self.assertEqual(self.repo.rehearsal_clock(**args,expected_stage=-1)['stage'],0)
            self.register();self.register('b@example.com')
            with self.assertRaises(EventConflict):self.repo.rehearsal_clock(**args,expected_stage=0)
            drain();self.assertEqual(len(messages),2)
            self.assertEqual(self.repo.rehearsal_clock(**args,expected_stage=0)['stage'],1)
            self.assertEqual(self.repo.rehearsal_clock(**args,expected_stage=0)['stage'],1)
            drain();self.assertEqual(len(messages),4)
            self.assertIn('starts tomorrow',messages[-1].get_body(preferencelist=('plain',)).get_content())
            self.assertIn('OGS handle required',messages[-1]['Subject'])
            self.repo.rehearsal_clock(**args,expected_stage=1);drain();self.assertEqual(len(messages),6)
            self.repo.rehearsal_clock(**args,expected_stage=2)
            people=self.roster()['roster']
            self.repo.set_attendance(**args,selection_id=people[0]['registration_occurrence_id'],attendance='attended')
            settings=self.repo.get_workshop_settings(event_id=self.eid,principal_id='admin')
            settings['followup_text']='Edited while paused for attendance.'
            self.repo.save_workshop_settings(event_id=self.eid,principal_id='admin',now=self.now,payload=settings)
            drain(self.now+timedelta(days=10));self.assertEqual(len(messages),6)
            with self.assertRaises(EventConflict):self.register('late@example.com')
            self.repo.rehearsal_clock(**args,expected_stage=3);drain();self.assertEqual(len(messages),7)
            self.assertIn(settings['followup_text'],messages[-1].get_content())
            self.repo.set_attendance(**args,selection_id=people[1]['registration_occurrence_id'],attendance='absent')
            drain();self.assertEqual(len(messages),8)
            self.assertIn("weren't able to attend",messages[-1].get_body(preferencelist=('plain',)).get_content())
            self.assertEqual(self.repo.rehearsal_clock(**args,expected_stage=3)['stage'],4)
            drain();self.assertEqual(len(messages),8)

    def test_rehearsal_clock_permissions_and_automatic_initialization(self):
        self.enable_email();args=dict(event_id=self.eid,occurrence_id=self.oid,principal_id='admin',now=self.now)
        with self.assertRaises(EventConflict):self.repo.rehearsal_clock(**args)
        with patch.dict(os.environ,{'EVENTS_EMAIL_MODE':'test','EVENTS_EMAIL_TEST_RECIPIENT':'gurujeet.khalsa@usgo.org'}):
            with self.assertRaises(EventForbidden):self.repo.rehearsal_clock(**args)
            with self.repo._connect() as conn:conn.cursor().execute('UPDATE events.workshop_settings SET is_test=1 WHERE event_id=?',self.eid)
            oid=self.repo.add_occurrence(event_id=self.eid,principal_id='admin',now=self.now,occurrence=dict(self.date,occurrence_key='clock-date'))['occurrence_id']
            args['occurrence_id']=oid
            self.assertEqual(self.repo.rehearsal_clock(**args)['stage'],0)
            with self.assertRaises(EventForbidden):self.repo.rehearsal_clock(**dict(args,principal_id='stranger'),expected_stage=0)
            with self.assertRaises(EventNotFound):self.repo.rehearsal_clock(**dict(args,occurrence_id=oid+10000))
            with self.assertRaises(EventConflict):self.repo.rehearsal_clock(**args,expected_stage=2)
            self.repo.rehearsal_clock(**args,expected_stage=0)
            with self.assertRaises(EventConflict):self.repo.update_occurrence(event_id=self.eid,occurrence_id=oid,principal_id='admin',now=self.now,occurrence=dict(self.date,occurrence_key='clock-date'))
