"""Bounded Gmail and AGA Zoom adapters. No credentials or response bodies in errors."""
import base64
import json
import os
from copy import deepcopy
from email.utils import getaddresses
from html import escape
from datetime import timezone
from urllib import request, parse, error
from workshop_domain import validate_join_url
from event_domain import EMAIL_RE


class ProviderError(Exception):
    def __init__(self, message, *, uncertain=False, retryable=False):
        super().__init__(message)
        self.uncertain = uncertain
        self.retryable = retryable


def required(name):
    value = os.environ.get(name, '').strip()
    if not value:
        raise ProviderError(f'Missing server setting: {name}')
    return value


def http_json(url, *, method='GET', headers=None, payload=None, form=None, mutation=False):
    headers = dict(headers or {})
    data = None
    if payload is not None:
        data = json.dumps(payload).encode()
        headers['Content-Type'] = 'application/json'
    if form is not None:
        data = parse.urlencode(form).encode()
        headers['Content-Type'] = 'application/x-www-form-urlencoded'
    try:
        with request.urlopen(request.Request(url, data=data, headers=headers, method=method), timeout=30) as response:
            raw = response.read()
            return json.loads(raw) if raw else {}
    except error.HTTPError as exc:
        raise ProviderError(f'Provider returned HTTP {exc.code}.',
            uncertain=mutation and exc.code >= 500, retryable=exc.code == 429 or (not mutation and exc.code >= 500)) from None
    except (error.URLError, TimeoutError, OSError, ValueError):
        raise ProviderError('Provider request did not return a usable result.', uncertain=mutation, retryable=not mutation) from None


def email_delivery_config():
    mode=os.environ.get('EVENTS_EMAIL_MODE','off').strip().lower()
    if mode not in ('off','test','live'):
        raise ProviderError('Invalid EVENTS_EMAIL_MODE; email delivery is blocked.')
    recipient=os.environ.get('EVENTS_EMAIL_TEST_RECIPIENT','').strip().lower()
    if mode=='test' and (not EMAIL_RE.fullmatch(recipient) or any(c in recipient for c in '\r\n,;')):
        raise ProviderError('Configure one valid EVENTS_EMAIL_TEST_RECIPIENT; test delivery is blocked.')
    monitor=os.environ.get('EVENTS_EMAIL_MONITOR_BCC','').strip().lower()
    if mode=='live' and monitor and (not EMAIL_RE.fullmatch(monitor) or any(c in monitor for c in '\r\n,;')):
        raise ProviderError('Configure one valid EVENTS_EMAIL_MONITOR_BCC; live delivery is blocked.')
    return dict(mode=mode,test_recipient=recipient if mode=='test' else None,
                monitor_bcc=monitor if mode=='live' else None)


def prepare_email_delivery(message, config):
    if config['mode']=='off':raise ProviderError('Email delivery mode is off.')
    if config['mode']=='live':
        monitor=config.get('monitor_bcc')
        recipients=getaddresses([str(value) for key in ('To','Cc','Bcc') for value in message.get_all(key,[])])
        if not monitor or monitor.lower() in {address.lower() for _,address in recipients}:return message
        result=deepcopy(message)
        existing=[str(value) for value in result.get_all('Bcc',[])]
        del result['Bcc']
        result['Bcc']=', '.join([*existing,monitor])
        return result
    result=deepcopy(message)
    intended=str(message['To'])
    for key in list(result.keys()):
        if key.lower() in ('to','cc','bcc') or key.lower().startswith('resent-'):
            del result[key]
    result['To']=config['test_recipient']
    result.replace_header('Subject','[TEST] '+str(result['Subject']))
    result.replace_header('Message-ID',str(result['Message-ID']).replace('<','<test-',1))
    result['X-AGA-Test-Mode']='true'
    banner=f'TEST EMAIL — Intended participant: {intended}\nDelivered only to: {config["test_recipient"]}\n\n'
    html_banner='<div style="border:2px solid #b45309;padding:12px"><strong>TEST EMAIL</strong><br>Intended participant: '+escape(intended)+'<br>Delivered only to: '+escape(config['test_recipient'])+'</div>'
    for part in result.walk():
        if part.is_multipart() or part.get_content_disposition()=='attachment':continue
        if part.get_content_type()=='text/plain':part.set_content(banner+part.get_content())
        elif part.get_content_type()=='text/html':
            body=part.get_content()
            body=body.replace('<body>','<body>'+html_banner,1) if '<body>' in body else html_banner+body
            part.set_content(body,subtype='html')
    return result


class GmailSender:
    def send(self, message):
        # Explicit allowlist prevents a settings edit from silently using an unauthorized From address.
        allowed = {s.strip().lower() for s in required('EVENTS_ALLOWED_SENDERS').split(',')}
        if str(message['From']).lower() not in allowed:
            raise ProviderError('Sender must be authorized in EVENTS_ALLOWED_SENDERS and Gmail.')
        message=prepare_email_delivery(message,email_delivery_config())
        token = http_json('https://oauth2.googleapis.com/token', method='POST', form={
            'client_id': required('GOOGLE_WORKSPACE_CLIENT_ID'),
            'client_secret': required('GOOGLE_WORKSPACE_CLIENT_SECRET'),
            'refresh_token': required('GOOGLE_WORKSPACE_REFRESH_TOKEN'), 'grant_type': 'refresh_token'})
        if not token.get('access_token'):
            raise ProviderError('Gmail authorization returned no access token.')
        result = http_json('https://gmail.googleapis.com/gmail/v1/users/me/messages/send', method='POST',
            headers={'Authorization': f"Bearer {token['access_token']}"},
            payload={'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}, mutation=True)
        if not result.get('id'):
            raise ProviderError('Gmail returned no message identifier.', uncertain=True)
        return str(result['id'])


class ZoomClient:
    def __init__(self):
        self.token = None

    def _request(self, path, method='GET', payload=None):
        if not self.token:
            basic = base64.b64encode(f"{required('EVENTS_ZOOM_CLIENT_ID')}:{required('EVENTS_ZOOM_CLIENT_SECRET')}".encode()).decode()
            result = http_json('https://zoom.us/oauth/token', method='POST',
                headers={'Authorization': f'Basic {basic}'},
                form={'grant_type': 'account_credentials', 'account_id': required('EVENTS_ZOOM_ACCOUNT_ID')})
            self.token = result.get('access_token')
            if not self.token:
                raise ProviderError('Zoom authorization returned no access token.')
        return http_json('https://api.zoom.us/v2'+path, method=method,
                         headers={'Authorization': f'Bearer {self.token}'}, payload=payload, mutation=method!='GET')

    def verify_host(self, email):
        user = self._request('/users/'+parse.quote(email, safe=''))
        if user.get('type') != 2 or user.get('status') != 'active':
            raise ProviderError('The AGA Zoom host must be active and licensed.')
        if user.get('account_id') != required('EVENTS_ZOOM_ACCOUNT_ID'):
            raise ProviderError('The Zoom host does not belong to the configured AGA account.')
        return user['id']

    @staticmethod
    def meeting_payload(occurrence, event):
        duration = max(1, int((occurrence['ends_at_utc']-occurrence['starts_at_utc']).total_seconds()/60))
        # Zoom requires a literal Z for UTC; +00:00 is interpreted as local time.
        start_time=occurrence['starts_at_utc'].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        return dict(topic=occurrence['title'], type=2, start_time=start_time,
                    duration=duration, timezone=event['time_zone'],
                    agenda=f"AGA Events workshop {event['event_id']}/{occurrence['occurrence_id']}",
                    settings=dict(use_pmi=False, waiting_room=True, join_before_host=False, auto_recording='none'))

    def create(self, host_email, occurrence, event):
        user_id = self.verify_host(host_email)
        result = self._request('/users/'+parse.quote(user_id, safe='')+'/meetings', 'POST', self.meeting_payload(occurrence, event))
        try:
            return dict(meeting_id=str(result['id']), join_url=validate_join_url(result['join_url']))
        except Exception:
            # A successful POST with an unusable response must never be repeated automatically.
            raise ProviderError('Zoom meeting creation needs reconciliation.', uncertain=True) from None

    def update(self, meeting_id, occurrence, event):
        self._request('/meetings/'+parse.quote(meeting_id, safe=''), 'PATCH', self.meeting_payload(occurrence, event))

    def adopt(self, meeting_id, host_email, occurrence, event):
        host_id=self.verify_host(host_email)
        meeting=self._request('/meetings/'+parse.quote(meeting_id,safe=''))
        marker=f"AGA Events workshop {event['event_id']}/{occurrence['occurrence_id']}"
        if meeting.get('host_id')!=host_id or meeting.get('agenda')!=marker or meeting.get('type')!=2:
            raise ProviderError('The meeting must match this workshop and its licensed AGA Zoom host.')
        return validate_join_url(meeting['join_url'])

    def delete(self, meeting_id):
        try:
            self._request('/meetings/'+parse.quote(meeting_id, safe=''), 'DELETE')
        except ProviderError as exc:
            if str(exc) != 'Provider returned HTTP 404.':
                raise
