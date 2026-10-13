"""Authorize the selected workshop mailbox; keep credentials in memory/Azure only."""
import base64
import hashlib
import json
from pathlib import Path
import secrets
import subprocess
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib import parse, request, error

AZ = r'C:\Program Files (x86)\Microsoft SDKs\Azure\CLI2\wbin\az.cmd'
GROUP = 'aga-data-platform'
SOURCE_APP = 'aga-clubexpress-mail'
TARGET_APP = 'aga-events'
SENDER = 'milan.mladenovic@usgo.org'
REDIRECT = 'http://127.0.0.1:8765/'
SEND_SCOPE = 'https://www.googleapis.com/auth/gmail.send'
SCOPES = {'openid', 'email', SEND_SCOPE}


def az(*args):
    result = subprocess.run([AZ, *args], capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError('Azure CLI request failed; no credential details logged.')
    return result.stdout.strip()


def call(url, *, bearer=None, payload=None, form=None, method=None):
    headers = {}
    data = None
    if bearer:
        headers['Authorization'] = 'Bearer ' + bearer
    if payload is not None:
        data = json.dumps(payload).encode()
        headers['Content-Type'] = 'application/json'
    if form is not None:
        data = parse.urlencode(form).encode()
        headers['Content-Type'] = 'application/x-www-form-urlencoded'
    try:
        with request.urlopen(request.Request(url, data=data, headers=headers, method=method), timeout=40) as response:
            body = response.read()
            return json.loads(body) if body else {}
    except error.HTTPError as exc:
        raise RuntimeError(f'Provider request failed (HTTP {exc.code}); response omitted.') from None


def settings_url(subscription, app, listing=False):
    return (f'https://management.azure.com/subscriptions/{subscription}/resourceGroups/{GROUP}'
            f'/providers/Microsoft.Web/sites/{app}/config/appsettings'
            + ('/list' if listing else '') + '?api-version=2022-03-01')


def verify_identity(access_token):
    identity = call('https://openidconnect.googleapis.com/v1/userinfo', bearer=access_token)
    if identity.get('email', '').lower() != SENDER or identity.get('email_verified') is not True:
        raise RuntimeError('Google authorization did not verify the selected workshop mailbox; nothing saved.')


def main():
    subscription = az('account', 'show', '--query', 'id', '-o', 'tsv')
    management_token = az('account', 'get-access-token', '--resource', 'https://management.azure.com/', '--query', 'accessToken', '-o', 'tsv')
    source = call(settings_url(subscription, SOURCE_APP, True), bearer=management_token, method='POST')['properties']
    client_id = source['GOOGLE_WORKSPACE_CLIENT_ID']
    client_secret = source['GOOGLE_WORKSPACE_CLIENT_SECRET']
    del source
    state = secrets.token_urlsafe(32)
    verifier = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip('=')
    auth_url = 'https://accounts.google.com/o/oauth2/v2/auth?' + parse.urlencode(dict(
        client_id=client_id, redirect_uri=REDIRECT, response_type='code',
        scope=' '.join(sorted(SCOPES)), access_type='offline', prompt='consent',
        include_granted_scopes='false', login_hint=SENDER, state=state,
        code_challenge=challenge, code_challenge_method='S256'))
    result = {}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            parsed = parse.urlparse(self.path)
            if parsed.path == '/start':
                self.send_response(302)
                self.send_header('Location', auth_url)
                self.send_header('Cache-Control', 'no-store')
                self.end_headers()
                return
            params = parse.parse_qs(parsed.query)
            if parsed.path != '/' or not secrets.compare_digest(params.get('state', [''])[0], state):
                self.send_response(400)
                self.end_headers()
                return
            if 'error' in params:
                result['error'] = True
            elif 'code' in params:
                result['code'] = params['code'][0]
            else:
                self.send_response(400)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Referrer-Policy', 'no-referrer')
            self.end_headers()
            self.wfile.write(b'<html><head><title>Workshop email authorization</title></head><body>'
                b'<h1>Authorization response received</h1><p>Return to Codex while the mailbox is verified.</p>'
                b'<script>history.replaceState(null,"","/received")</script></body></html>')

    with HTTPServer(('127.0.0.1', 8765), Handler) as server:
        server.timeout = 1
        print('READY: Open http://127.0.0.1:8765/start in the browser signed in to ' + SENDER, flush=True)
        deadline = time.monotonic() + 1200
        while not result and time.monotonic() < deadline:
            server.handle_request()
    if not result.get('code'):
        raise RuntimeError('Google authorization was declined or timed out; nothing saved.')
    tokens = call('https://oauth2.googleapis.com/token', form=dict(
        client_id=client_id, client_secret=client_secret, code=result.pop('code'),
        redirect_uri=REDIRECT, grant_type='authorization_code', code_verifier=verifier))
    if not tokens.get('refresh_token') or SEND_SCOPE not in tokens.get('scope', '').split():
        raise RuntimeError('Required offline Gmail sending permission was not granted; nothing saved.')
    granted_scopes = set(tokens.get('scope', '').split())
    if any(scope.startswith('https://www.googleapis.com/auth/gmail.') and scope != SEND_SCOPE for scope in granted_scopes):
        raise RuntimeError('Unexpected broader Gmail permission; nothing saved.')
    verify_identity(tokens['access_token'])
    refreshed = call('https://oauth2.googleapis.com/token', form=dict(
        client_id=client_id, client_secret=client_secret, refresh_token=tokens['refresh_token'],
        grant_type='refresh_token'))
    verify_identity(refreshed['access_token'])
    # Refetch target immediately before updating; never modify the source application.
    management_token = az('account', 'get-access-token', '--resource', 'https://management.azure.com/', '--query', 'accessToken', '-o', 'tsv')
    target = call(settings_url(subscription, TARGET_APP, True), bearer=management_token, method='POST')['properties']
    if target.get('EVENTS_EMAIL_ENABLED', '').lower() == 'true':
        raise RuntimeError('The live email worker is active; stop and review before replacing its authorization.')
    target.update(GOOGLE_WORKSPACE_CLIENT_ID=client_id, GOOGLE_WORKSPACE_CLIENT_SECRET=client_secret,
        GOOGLE_WORKSPACE_REFRESH_TOKEN=tokens['refresh_token'], EVENTS_ALLOWED_SENDERS=SENDER,
        EVENTS_EMAIL_ENABLED='false')
    call(settings_url(subscription, TARGET_APP), bearer=management_token, payload={'properties': target}, method='PUT')
    saved = call(settings_url(subscription, TARGET_APP, True), bearer=management_token, method='POST')['properties']
    keys = ['GOOGLE_WORKSPACE_CLIENT_ID', 'GOOGLE_WORKSPACE_CLIENT_SECRET', 'GOOGLE_WORKSPACE_REFRESH_TOKEN',
            'EVENTS_ALLOWED_SENDERS', 'EVENTS_EMAIL_ENABLED']
    if any(saved.get(key) != target[key] for key in keys):
        raise RuntimeError('Azure settings read-back differs; inspect configuration before continuing.')
    print('SUCCESS: Verified ' + SENDER + ', tested token refresh, and saved workshop-only Azure settings.', flush=True)
    print('Email worker remains disabled. No email was sent.', flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(str(exc) if isinstance(exc, RuntimeError) else 'Authorization failed; credential details omitted.', flush=True)
        raise SystemExit(1)
