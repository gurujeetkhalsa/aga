import base64
import json
import os
import unittest
from email import message_from_bytes, policy
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, Mock

from event_domain import EventError, EventForbidden
from workshop_domain import (registration_status, normalize_settings, scheduled_messages,
                             email_is_eligible, validate_join_url)
from workshop_messages import build_message
from workshop_providers import ZoomClient, GmailSender, ProviderError, http_json, email_delivery_config, prepare_email_delivery

NOW=datetime(2026,9,7,12,tzinfo=timezone.utc)


class WorkshopPolicyTests(unittest.TestCase):
    def test_cancellation_status_preserves_other_selections(self):
        self.assertEqual(registration_status(['cancelled','waitlisted']), 'waitlisted')
        self.assertEqual(registration_status(['cancelled','confirmed']), 'confirmed')
        self.assertEqual(registration_status(['cancelled','cancelled']), 'cancelled')
        self.assertEqual(registration_status(['waitlisted','confirmed']), 'mixed')

    def test_settings_validate_and_reminders_do_not_catch_up_late_signups(self):
        settings=normalize_settings(dict(sender_email='milan.mladenovic@usgo.org',followup_text='Thanks!'))
        start=NOW+timedelta(hours=2);end=start+timedelta(hours=1)
        scheduled=list(scheduled_messages(start,end,settings,NOW))
        self.assertEqual([kind for kind,_ in scheduled], ['reminder_1','followup'])
        self.assertEqual(scheduled[-1][1],end+timedelta(hours=24))
        for invalid in ([True], [0], [1.5], '24', [721]):
            with self.assertRaises(EventError):
                normalize_settings(dict(sender_email='a@example.com',followup_text='Thanks',reminder_hours=invalid))

    def test_only_participant_zoom_urls_are_accepted(self):
        self.assertEqual(validate_join_url('https://us02web.zoom.us/j/123?pwd=abc'), 'https://us02web.zoom.us/j/123?pwd=abc')
        for invalid in ['http://zoom.us/j/123','https://zoom.us.evil.test/j/1','https://zoom.us/s/123?zak=host','https://u:p@zoom.us/j/123','javascript:alert(1)']:
            with self.assertRaises(EventError):validate_join_url(invalid)

    def test_cancelled_waitlisted_and_past_selections_get_no_reminders(self):
        start=NOW+timedelta(hours=1);end=start+timedelta(hours=1)
        for status,lifecycle,now in [('cancelled','published',NOW),('waitlisted','published',NOW),('confirmed','cancelled',NOW),('confirmed','published',end)]:
            self.assertFalse(email_is_eligible('reminder_1',status,lifecycle,start,end,now))
        self.assertTrue(email_is_eligible('workshop_cancelled','cancelled','cancelled',start,end,NOW))
        self.assertFalse(email_is_eligible('followup','confirmed','published',start,end,NOW))
        self.assertTrue(email_is_eligible('followup','confirmed','published',start,end,end))


class MessageTests(unittest.TestCase):
    def setUp(self):
        self.data=dict(email_id=42,occurrence_id=7,kind='confirmed',title='Free 1-hour workshop',
            starts_at_utc=NOW,ends_at_utc=NOW+timedelta(hours=1),time_zone='America/New_York',
            first_name='Ada',email='ada@example.com',join_url='https://us02web.zoom.us/j/123?pwd=abc')
        self.settings=dict(sender_email='milan.mladenovic@usgo.org',instructor_name='Milan',followup_text='Play Go with your local chapter.')

    def test_confirmation_contains_join_link_and_utc_calendar(self):
        m=build_message(self.data,self.settings,NOW)
        self.assertEqual(m['From'],'milan.mladenovic@usgo.org')
        self.assertEqual(m['Reply-To'],'milan.mladenovic@usgo.org')
        plain=m.get_body(preferencelist=('plain',)).get_content()
        html=m.get_body(preferencelist=('html',)).get_content()
        self.assertTrue(plain.startswith('Hi Ada,'))
        self.assertIn('Your place is confirmed for',plain)
        self.assertIn('first 30 minutes',plain)
        self.assertIn('please email it to me',plain)
        self.assertIn('<a href="https://online-go.com/register">Online Go Server (OGS)</a>',html)
        self.assertIn('EDT',m.get_body().get_content())
        self.assertIn(self.data['join_url'],m.get_body().get_content())
        attachment=list(m.iter_attachments())[0]
        self.assertIn(b'DTSTART:20260907T120000Z',attachment.get_payload(decode=True))
        self.assertEqual(m['Message-ID'],'<aga-workshop-email-42@usgo.org>')

    def test_waitlist_cancellation_and_followup_never_disclose_join_url(self):
        for kind in ['waitlisted','cancelled','workshop_cancelled','followup']:
            m=build_message(dict(self.data,kind=kind,attendance='attended'),self.settings,NOW)
            self.assertNotIn(self.data['join_url'],m.as_string())
            self.assertEqual(list(m.iter_attachments()),[])
        self.assertIn('local chapter',build_message(dict(self.data,kind='followup',attendance='attended'),self.settings,NOW).get_content())

    def test_followup_uses_attendance_and_preserves_edited_body(self):
        text='Thanks for joining!\n\nResources: https://www.usgo.org/\nCourse details coming soon.'
        settings=dict(self.settings,followup_text=text)
        attended=build_message(dict(self.data,kind='followup',attendance='attended'),settings,NOW)
        self.assertEqual(attended.get_content(),f'Hi Ada,\n\n{text}\n\nBest,\nMilan\n')
        absent=build_message(dict(self.data,kind='followup',attendance='absent',registration_url='https://workshop.test/events/test'),settings,NOW)
        self.assertIn("weren't able to attend",absent.get_body(preferencelist=('plain',)).get_content())
        self.assertIn('https://workshop.test/events/test',absent.get_body(preferencelist=('plain',)).get_content())
        self.assertNotIn('Thanks for joining!',absent.get_body(preferencelist=('plain',)).get_content())
        self.assertNotIn(self.data['join_url'],absent.as_string())
        self.assertEqual(list(absent.iter_attachments()),[])
        self.assertIn('Join another beginner Go workshop',absent['Subject'])
        for attendance in ('unrecorded',None):
            with self.assertRaises(ValueError):
                build_message(dict(self.data,kind='followup',attendance=attendance),settings,NOW)

    def test_no_show_email_embeds_matching_qr_and_survives_test_delivery(self):
        from io import BytesIO
        from PIL import Image
        from workshop_qr import badge_png
        for rehearsal in (False,True):
            url='https://aga-events.azurewebsites.net/events/learn-go-in-an-hour'+('-rehearsal' if rehearsal else '')
            settings=dict(self.settings,is_test=rehearsal)
            m=build_message(dict(self.data,kind='followup',attendance='absent',registration_url=url,
                first_name='Ada <test> & friends'),settings,NOW)
            m=prepare_email_delivery(m,dict(mode='test',test_recipient='gurujeet.khalsa@usgo.org'))
            wire=message_from_bytes(m.as_bytes(),policy=policy.default)
            self.assertEqual(wire['To'].addresses[0].addr_spec,'gurujeet.khalsa@usgo.org')
            html=wire.get_body(preferencelist=('html',)).get_content()
            plain=wire.get_body(preferencelist=('plain',)).get_content()
            self.assertIn('Hi Ada &lt;test&gt; &amp; friends,',html)
            self.assertIn('<strong>8-week Beginner Go Course</strong>',html)
            self.assertIn('Once you attend a workshop',plain)
            self.assertIn(url,html);self.assertIn(url,plain)
            self.assertIn(f'href="{url}/workshop-qr"',html)
            self.assertIn(f'href="{url}/workshop-qr/png"',html)
            self.assertIn('Share card</a>',html)
            self.assertIn('Download</a>',html)
            self.assertIn('role="presentation"',html)
            self.assertIn(url+'/workshop-qr',plain)
            self.assertNotIn('<script',html)
            self.assertNotIn('<iframe',html)
            self.assertNotIn(self.data['join_url'],wire.as_string())
            self.assertFalse(any(p.get_content_type()=='text/calendar' for p in wire.walk()))
            images=[p for p in wire.walk() if p.get_content_type()=='image/png']
            self.assertEqual(len(images),1)
            self.assertEqual(images[0]['Content-ID'],'<upcoming-workshops@usgo.org>')
            self.assertEqual(images[0].get_content_disposition(),'inline')
            self.assertIn('src="cid:upcoming-workshops@usgo.org"',html)
            self.assertEqual(images[0].get_payload(decode=True),badge_png(url,test_label=rehearsal))
            self.assertEqual(Image.open(BytesIO(images[0].get_payload(decode=True))).format,'PNG')

    def test_attended_default_preserves_paragraphs_bullets_and_safe_html(self):
        from workshop_domain import DEFAULT_ATTENDED_FOLLOWUP
        saved=normalize_settings(dict(self.settings,followup_text=DEFAULT_ATTENDED_FOLLOWUP))
        self.assertEqual(saved['followup_text'],DEFAULT_ATTENDED_FOLLOWUP)
        message=build_message(dict(self.data,kind='followup',attendance='attended',first_name='Ada <friend>'),saved,NOW)
        plain=message.get_body(preferencelist=('plain',)).get_content()
        html=message.get_body(preferencelist=('html',)).get_content()
        self.assertIn('Hi Ada <friend>,',plain)
        self.assertIn('Hi Ada &lt;friend&gt;,',html)
        self.assertIn('<strong>Beginners Go Workshop</strong>',html)
        self.assertIn('<strong>8-week Beginners Go Course</strong>',html)
        self.assertIn('<strong>19×19 board</strong>',html)
        self.assertEqual(html.count('<li>'),4)
        self.assertIn("You don't need to do anything right now.",plain)
        self.assertIn('\n\n- 8 weekly online classes\n- Learning',plain)
        self.assertNotIn('**',plain)
        self.assertNotIn(self.data['join_url'],message.as_string())
        self.assertEqual(list(message.iter_attachments()),[])
        changed=dict(saved,followup_text='**<script>alert(1)</script>**\n\n- Safe bullet')
        html=build_message(dict(self.data,kind='followup',attendance='attended'),changed,NOW).get_body(preferencelist=('html',)).get_content()
        self.assertNotIn('<script>',html)
        self.assertIn('&lt;script&gt;',html)

    def test_one_hour_reminder_uses_short_label_and_preserves_zoom_passcode(self):
        from html.parser import HTMLParser
        class Links(HTMLParser):
            def __init__(self):super().__init__();self.links=[];self.text=[]
            def handle_starttag(self,tag,attrs):
                if tag=='a':self.links.append(dict(attrs)['href'])
            def handle_data(self,value):self.text.append(value)
        url='https://us02web.zoom.us/j/123?pwd=longPasscode&from=reminder'
        original=build_message(dict(self.data,kind='reminder_1',join_url=url,first_name='Ada <friend>'),self.settings,NOW)
        delivered=prepare_email_delivery(original,dict(mode='test',test_recipient='gurujeet.khalsa@usgo.org'))
        message=message_from_bytes(delivered.as_bytes(),policy=policy.default)
        parsed=Links();parsed.feed(message.get_body(preferencelist=('html',)).get_content())
        self.assertEqual(parsed.links,[url])
        visible=' '.join(parsed.text)
        self.assertIn('Join the workshop on Zoom',visible)
        self.assertNotIn(url,visible)
        self.assertIn('Hi Ada <friend>,',visible)
        self.assertIn('Instructor: Milan Mladenovic',visible)
        self.assertIn(url,message.get_body(preferencelist=('plain',)).get_content())
        self.assertEqual(list(original.iter_attachments())[0].get_payload(decode=True),list(message.iter_attachments())[0].get_payload(decode=True))
        self.assertIn(b'pwd=longPasscode',list(message.iter_attachments())[0].get_payload(decode=True))

    def test_sender_is_configurable_without_template_edits(self):
        m=build_message(self.data,dict(self.settings,sender_email='new@usgo.org'),NOW)
        self.assertEqual(m['Reply-To'],'new@usgo.org')
        self.assertIn('new@usgo.org',m.get_body().get_content())

    def test_preparation_reminder_uses_actual_date_links_and_calendar(self):
        for month, day, utc_hour, zone in [(9,24,22,'EDT'),(11,12,23,'EST')]:
            start=datetime(2026,month,day,utc_hour,tzinfo=timezone.utc)
            for lead in (24,48):
                with self.subTest(month=month,lead=lead):
                    data=dict(self.data,kind=f'reminder_{lead}',starts_at_utc=start,ends_at_utc=start+timedelta(hours=1))
                    message=build_message(data,self.settings,start-timedelta(hours=lead))
                    plain=message.get_body(preferencelist=('plain',)).get_content()
                    html=message.get_body(preferencelist=('html',)).get_content()
                    self.assertTrue(plain.startswith('Hi Ada,'))
                    self.assertIn('<p>Hi Ada,</p>',html)
                    self.assertNotIn('Hi everyone',plain)
                    self.assertIn(f'{day}, 2026 at 6:00 PM {zone}',plain)
                    self.assertEqual(plain.count(f'{day}, 2026 at 6:00 PM {zone}'),1 if lead==24 else 2)
                    if lead==24:
                        self.assertIn('starts tomorrow',plain)
                        self.assertIn('Workshop tomorrow — OGS handle required',message['Subject'])
                        self.assertIn('<strong>You need an OGS account and handle (username) to participate in the workshop.</strong>',html)
                        self.assertIn('please reply to this email with it today',plain)
                        self.assertIn('use your existing handle',plain)
                    else:
                        self.assertNotIn('tomorrow',plain)
                        self.assertIn('please email it to me',plain)
                    self.assertNotIn('August 28',plain)
                    self.assertIn('https://online-go.com/register',plain)
                    self.assertIn('<a href="https://online-go.com/register">Online Go Server (OGS)</a>',html)
                    self.assertIn(f'<a href="{data["join_url"]}">Zoom link for the workshop</a>',html)
                    self.assertEqual(message['Reply-To'],'milan.mladenovic@usgo.org')
                    self.assertEqual(len(list(message.iter_attachments())),1)

    def test_preparation_html_escapes_values_and_short_reminder_stays_brief(self):
        message=build_message(dict(self.data,kind='reminder_24',first_name='Ada <test> & friends'),dict(self.settings,instructor_name='Milan <test> & friends'),NOW)
        self.assertIn('<p>Hi Ada &lt;test&gt; &amp; friends,</p>',message.get_body(preferencelist=('html',)).get_content())
        self.assertIn('Milan &lt;test&gt; &amp; friends',message.get_body(preferencelist=('html',)).get_content())
        message=build_message(dict(self.data,kind='reminder_1'),self.settings,NOW)
        self.assertIn('>Join the workshop on Zoom</a>',message.get_body(preferencelist=('html',)).get_content())
        self.assertIn('Your Go workshop is coming up very shortly.',message.get_body().get_content())
        self.assertNotIn('8-week course',message.get_body().get_content())


class ProviderTests(unittest.TestCase):
    def test_live_monitor_copy_reaches_gmail_without_changing_participant_message(self):
        fixture=MessageTests();fixture.setUp()
        monitor='gurujeet.khalsa@usgo.org'
        env=dict(EVENTS_EMAIL_MODE='live',EVENTS_EMAIL_MONITOR_BCC=monitor,
                 EVENTS_ALLOWED_SENDERS=fixture.settings['sender_email'],GOOGLE_WORKSPACE_CLIENT_ID='fake',
                 GOOGLE_WORKSPACE_CLIENT_SECRET='fake',GOOGLE_WORKSPACE_REFRESH_TOKEN='fake')
        for kind,attendance in [('confirmed',None),('reminder_24',None),('reminder_1',None),
                                ('followup','attended'),('followup','absent'),('waitlisted',None),
                                ('cancelled',None),('workshop_cancelled',None)]:
            original=build_message(dict(fixture.data,kind=kind,attendance=attendance,
                registration_url='https://aga-events.azurewebsites.net/events/learn-go-in-an-hour'),fixture.settings,NOW)
            with self.subTest(kind=kind,attendance=attendance),patch.dict(os.environ,env),patch('workshop_providers.http_json',side_effect=[{'access_token':'fake'},{'id':'receipt'}]) as transport:
                self.assertEqual(GmailSender().send(original),'receipt')
            sent=message_from_bytes(base64.urlsafe_b64decode(transport.call_args.kwargs['payload']['raw']),policy=policy.default)
            expected=message_from_bytes(original.as_bytes(),policy=policy.default)
            self.assertEqual(sent['To'],original['To'])
            self.assertEqual(sent['Bcc'],monitor)
            self.assertEqual(sent['Subject'],original['Subject'])
            self.assertEqual([p.get_payload(decode=True) for p in sent.walk() if not p.is_multipart()],
                             [p.get_payload(decode=True) for p in expected.walk() if not p.is_multipart()])
            self.assertIsNone(original['Bcc'])

    def test_live_monitor_deduplicates_and_test_delivery_never_copies_monitor(self):
        fixture=MessageTests();fixture.setUp()
        original=build_message(fixture.data,fixture.settings,NOW)
        monitor='gurujeet.khalsa@usgo.org'
        original['Bcc']='existing@example.com'
        delivered=prepare_email_delivery(original,dict(mode='live',monitor_bcc=monitor))
        self.assertEqual({a.addr_spec for a in delivered['Bcc'].addresses},{'existing@example.com',monitor})
        self.assertIs(prepare_email_delivery(delivered,dict(mode='live',monitor_bcc=monitor)),delivered)
        self.assertIs(prepare_email_delivery(original,dict(mode='live',monitor_bcc='ada@example.com')),original)
        with patch.dict(os.environ,dict(EVENTS_EMAIL_MODE='test',EVENTS_EMAIL_TEST_RECIPIENT='test@example.com',EVENTS_EMAIL_MONITOR_BCC=monitor)):
            test=prepare_email_delivery(delivered,email_delivery_config())
        self.assertEqual(test['To'],'test@example.com');self.assertIsNone(test['Bcc'])
        for invalid in ['one@example.com,two@example.com','bad','x@example.com\r\nBcc: y@example.com']:
            with patch.dict(os.environ,dict(EVENTS_EMAIL_MODE='live',EVENTS_EMAIL_MONITOR_BCC=invalid)),self.assertRaises(ProviderError):email_delivery_config()

    def test_test_delivery_redirects_all_recipients_and_preserves_personalization_and_calendar(self):
        fixture=MessageTests();fixture.setUp()
        original=build_message(dict(fixture.data,last_name='Lovelace'),fixture.settings,NOW)
        original['Cc']='cc@example.com';original['Bcc']='bcc@example.com'
        original['Resent-To']='resent@example.com'
        config=dict(mode='test',test_recipient='gurujeet.khalsa@usgo.org')
        test=prepare_email_delivery(original,config)
        self.assertEqual(str(test['To']),config['test_recipient'])
        for header in ('Cc','Bcc','Resent-To'):self.assertIsNone(test[header])
        self.assertTrue(str(test['Subject']).startswith('[TEST] '))
        self.assertNotEqual(test['Message-ID'],original['Message-ID'])
        self.assertIn('Ada Lovelace <ada@example.com>',test.get_body(preferencelist=('plain',)).get_content())
        self.assertIn('Ada Lovelace &lt;ada@example.com&gt;',test.get_body(preferencelist=('html',)).get_content())
        self.assertIn('Hi Ada,',test.get_body(preferencelist=('plain',)).get_content())
        self.assertEqual(list(test.iter_attachments())[0].get_payload(decode=True),list(original.iter_attachments())[0].get_payload(decode=True))
        self.assertEqual(original['To'].addresses[0].addr_spec,'ada@example.com')
        env=dict(EVENTS_EMAIL_MODE='test',EVENTS_EMAIL_TEST_RECIPIENT=config['test_recipient'],
                 EVENTS_ALLOWED_SENDERS=fixture.settings['sender_email'],GOOGLE_WORKSPACE_CLIENT_ID='fake',
                 GOOGLE_WORKSPACE_CLIENT_SECRET='fake',GOOGLE_WORKSPACE_REFRESH_TOKEN='fake')
        with patch.dict(os.environ,env),patch('workshop_providers.http_json',side_effect=[{'access_token':'fake'},{'id':'receipt'}]) as transport:
            self.assertEqual(GmailSender().send(original),'receipt')
        payload=transport.call_args.kwargs['payload']['raw']
        sent=message_from_bytes(base64.urlsafe_b64decode(payload),policy=policy.default)
        self.assertEqual(sent['To'].addresses[0].addr_spec,config['test_recipient'])
        self.assertIsNone(sent['Bcc']);self.assertIsNone(sent['Cc']);self.assertIsNone(sent['Resent-To'])

    def test_delivery_mode_fails_closed(self):
        with patch.dict(os.environ,{},clear=True):self.assertEqual(email_delivery_config()['mode'],'off')
        for mode,recipient in [('typo',''),('test',''),('test','one@example.com,two@example.com'),('test','x@example.com\r\nBcc: y@example.com')]:
            with patch.dict(os.environ,{'EVENTS_EMAIL_MODE':mode,'EVENTS_EMAIL_TEST_RECIPIENT':recipient}),self.assertRaises(ProviderError):
                email_delivery_config()
        fixture=MessageTests();fixture.setUp()
        with patch.dict(os.environ,{'EVENTS_ALLOWED_SENDERS':fixture.settings['sender_email'],'EVENTS_EMAIL_MODE':'off'}),patch('workshop_providers.http_json') as transport,self.assertRaises(ProviderError):
            GmailSender().send(build_message(fixture.data,fixture.settings,NOW))
        transport.assert_not_called()

    def test_zoom_create_and_update_preserve_eastern_workshop_times(self):
        zoom=ZoomClient();event=dict(event_id=1,time_zone='America/New_York')
        # Zoom treats a non-Z start as local time in the supplied timezone.
        for start, expected in [
            (datetime(2026,9,24,22,tzinfo=timezone.utc),'2026-09-24T22:00:00Z'),
            (datetime(2026,11,12,23,tzinfo=timezone.utc),'2026-11-12T23:00:00Z'),
            (datetime(2026,9,24,18,tzinfo=timezone(timedelta(hours=-4))),'2026-09-24T22:00:00Z')]:
            occurrence=dict(occurrence_id=3,title='Workshop',starts_at_utc=start,ends_at_utc=start+timedelta(hours=1))
            with self.subTest(start=start),patch.object(zoom,'verify_host',return_value='host'),patch.object(zoom,'_request',return_value={'id':123,'join_url':'https://zoom.us/j/123'}) as transport:
                zoom.create('host@example.com',occurrence,event)
                zoom.update('123',occurrence,event)
                for call in transport.call_args_list:
                    self.assertEqual(call.args[2]['start_time'],expected)
                    self.assertEqual(call.args[2]['duration'],60)
                    self.assertEqual(call.args[2]['timezone'],'America/New_York')

    def test_zoom_rejects_unlicensed_and_foreign_account(self):
        zoom=ZoomClient()
        with patch.dict(os.environ,{'EVENTS_ZOOM_ACCOUNT_ID':'aga'}):
            for user in [dict(type=1,status='active',account_id='aga'),dict(type=2,status='active',account_id='other')]:
                with patch.object(zoom,'_request',return_value=user),self.assertRaises(ProviderError):
                    zoom.verify_host('host@example.com')

    def test_zoom_create_returns_only_participant_details(self):
        zoom=ZoomClient();event=dict(event_id=1,time_zone='America/New_York')
        occurrence=dict(occurrence_id=3,title='Workshop',starts_at_utc=NOW,ends_at_utc=NOW+timedelta(hours=1))
        with patch.object(zoom,'verify_host',return_value='user1'),patch.object(zoom,'_request',return_value={'id':123,'join_url':'https://zoom.us/j/123','start_url':'secret'}) as request_mock:
            result=zoom.create('host@example.com',occurrence,event)
            self.assertEqual(result,dict(meeting_id='123',join_url='https://zoom.us/j/123'))
            payload=request_mock.call_args.args[2]
            self.assertFalse(payload['settings']['use_pmi']);self.assertTrue(payload['settings']['waiting_room'])

    def test_mutation_timeout_requires_reconciliation(self):
        with patch('workshop_providers.request.urlopen',side_effect=TimeoutError),self.assertRaises(ProviderError) as caught:
            http_json('https://zoom.us/example',method='POST',mutation=True)
        self.assertTrue(caught.exception.uncertain)
        self.assertFalse(caught.exception.retryable)

    def test_sender_allowlist_checked_before_network(self):
        m=Mock();m.__getitem__=Mock(return_value='unapproved@example.com')
        with patch.dict(os.environ,{'EVENTS_ALLOWED_SENDERS':'milan.mladenovic@usgo.org'}),patch('workshop_providers.http_json') as transport,self.assertRaises(ProviderError):
            GmailSender().send(m)
        transport.assert_not_called()


class AuthTests(unittest.TestCase):
    def setUp(self):
        import function_app
        import azure.functions as func
        self.app=function_app;self.func=func

    def request(self,method='GET',roles=None,**headers):
        if roles is not None:
            principal={'userId':'authorized-id','claims':[{'typ':'roles','val':role} for role in roles]}
            headers['x-ms-client-principal']=base64.b64encode(json.dumps(principal).encode()).decode()
        return self.func.HttpRequest(method=method,url='https://aga-events.azurewebsites.net/api/events/manage/1/settings',headers=headers,body=b'')

    def test_anonymous_and_wrong_role_are_rejected(self):
        for req in [self.request(),self.request(roles=['Other']),self.request(**{'x-ms-client-principal':'garbage'})]:
            with self.assertRaises(EventError):self.app._organizer_principal(req)

    def test_malformed_principal_fails_closed(self):
        for payload in [[], {'claims':None}, {'claims':[3]}]:
            req=self.request(**{'x-ms-client-principal':base64.b64encode(json.dumps(payload).encode()).decode()})
            with self.assertRaises(EventError):self.app._organizer_principal(req)

    def test_cross_origin_and_missing_mutation_header_rejected(self):
        for headers in [{},{'x-events-admin':'1','origin':'https://evil.example'}]:
            with self.assertRaises(EventForbidden):self.app._organizer_principal(self.request('PUT',['Events.Organizer'],**headers))
        req=self.request('PUT',['Events.Organizer'],**{'x-events-admin':'1','origin':'https://aga-events.azurewebsites.net'})
        self.assertEqual(self.app._organizer_principal(req),'authorized-id')

    def test_local_override_is_disabled_in_production(self):
        with patch.dict(os.environ,{'AZURE_FUNCTIONS_ENVIRONMENT':'Production','EVENTS_LOCAL_ORGANIZER_PRINCIPAL_ID':'local'}):
            with self.assertRaises(EventError):self.app._organizer_principal(self.request(**{'x-events-local-principal':'local'}))

    def test_all_functions_index_successfully(self):
        names=[f.get_function_name() for f in self.app.app.get_functions()]
        self.assertIn('WorkshopNotificationsTimer',names)
        self.assertIn('WorkshopRoster',names)

    def test_signup_patch_routes_handle_and_rejects_mixed_updates(self):
        base=self.request('PATCH',['Events.Organizer'],**{'x-events-admin':'1'})
        for payload,status in [({'ogs_handle':'AdaGo'},200),({'ogs_handle':'AdaGo','attendance':'attended'},400)]:
            req=self.func.HttpRequest(method='PATCH',url=base.url,headers=base.headers,
                route_params={'event_id':'1','occurrence_id':'2','selection_id':'3'},body=json.dumps(payload).encode())
            repo=Mock();repo.set_ogs_handle.return_value={'ogs_handle':'AdaGo'}
            with patch.object(self.app,'_repository',return_value=repo):
                response=self.app.workshop_signup(req)
            self.assertEqual(response.status_code,status)
            repo.set_attendance.assert_not_called()
            if status==200:
                self.assertEqual(json.loads(response.get_body())['ogs_handle'],'AdaGo')
                self.assertEqual(repo.set_ogs_handle.call_args.kwargs['principal_id'],'authorized-id')
            else:repo.set_ogs_handle.assert_not_called()

    def test_manual_signup_confirmation_option_is_boolean_and_defaults_on(self):
        from contextlib import contextmanager
        base=self.request('POST',['Events.Organizer'],**{'x-events-admin':'1'})
        @contextmanager
        def transaction(*args):yield None,{'slug':'test'}
        for option,status in [({},201),({'send_confirmation_email':True},201),({'send_confirmation_email':False},201),
                              ({'send_confirmation_email':'false'},400),({'send_confirmation_email':None},400),({'send_confirmation_email':0},400)]:
            payload=dict(contact=dict(first_name='Ada',last_name='Test',email='ada@example.com'),idempotency_key='test',**option)
            req=self.func.HttpRequest(method='POST',url=base.url,headers=base.headers,
                route_params={'event_id':'1','occurrence_id':'2'},body=json.dumps(payload).encode())
            repo=Mock();repo._workshop_transaction.side_effect=transaction;repo.register_free.return_value={}
            with patch.object(self.app,'_repository',return_value=repo):response=self.app.workshop_roster(req)
            self.assertEqual(response.status_code,status)
            if status==201:
                self.assertIs(repo.register_free.call_args.kwargs['send_confirmation_email'],option.get('send_confirmation_email',True))
                self.assertEqual(repo.register_free.call_args.kwargs['principal_id'],'authorized-id')
            else:repo.register_free.assert_not_called()
