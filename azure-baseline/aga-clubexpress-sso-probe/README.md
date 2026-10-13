# ClubExpress SSO Probe App

Temporary Azure Functions receiver for discovering what ClubExpress sends during its SSO redirect/callback.

## Endpoints

- `GET /api/s/<probe-token>` short start URL for ClubExpress fields with length limits.
- `GET|POST|OPTIONS /api/c/<probe-token>` short callback URL for ClubExpress fields with length limits.
- `GET /api/l/<probe-token>` short recent-captures URL.
- `GET /api/clubexpress-sso-probe-start/<probe-token>`
- `GET|POST|OPTIONS /api/clubexpress-sso-probe/<probe-token>`
- `GET /api/clubexpress-sso-probe-last/<probe-token>`

The `/start` endpoint redirects to the ClubExpress authorization URL:

```text
https://usgo.org/content.aspx?page_id=597&client_id=<CLIENT_ID>&scope=member-profile&redirect_uri=<CALLBACK_URL>&response_type=code&state=<STATE>
```

Add `?format=json` to inspect the generated URL without redirecting.

The probe captures:

- HTTP method and URL
- path and route parameters
- query parameters
- headers and cookies
- raw body text and base64
- parsed JSON bodies
- parsed `application/x-www-form-urlencoded` bodies

It logs each capture and returns the captured request as JSON in the browser.

## App Settings

Set these in Azure Function App settings:

- `SSO_PROBE_TOKEN`: long random value used in the callback URL path.
- `CLUBEXPRESS_OAUTH_CLIENT_ID`: OAuth client ID from ClubExpress.
- `CLUBEXPRESS_OAUTH_CLIENT_SECRET`: OAuth client secret from ClubExpress.
- `SSO_PROBE_MAX_CAPTURE_BYTES`: optional, default `65536`.
- `SSO_PROBE_CAPTURE_LIMIT`: optional, default `10`.
- `SSO_PROBE_CAPTURE_CONTAINER`: optional, default `clubexpress-sso-probe-captures`.
- `SSO_PROBE_STORAGE_CONNECTION_STRING`: optional. If omitted, the app uses `AzureWebJobsStorage`.

Optional ClubExpress OAuth settings:

- `CLUBEXPRESS_CLUB_DOMAIN`: optional, default `https://usgo.org`.
- `CLUBEXPRESS_OAUTH_AUTHORIZE_URL`: optional, default `<CLUBEXPRESS_CLUB_DOMAIN>/content.aspx`.
- `CLUBEXPRESS_OAUTH_TOKEN_URL`: optional single token URL override.
- `CLUBEXPRESS_OAUTH_TOKEN_URLS`: optional comma-separated diagnostic token URL candidate list.
- `CLUBEXPRESS_OAUTH_TOKEN_URL_RETRIES`: optional attempts per token URL candidate, default `4`.
- `CLUBEXPRESS_OAUTH_TENANT_DOMAIN`: optional ClubExpress tenant host such as `https://aga1.clubexpress.com`; when set and no token URL override is present, the probe tries `<tenant>/oauth/token.aspx` first. The temporary AGA probe defaults to the two ClubExpress-supported candidates `https://aga1.clubexpress.com/oauth/token.aspx` and `https://usgo.org/oauth/token.aspx`.
- `CLUBEXPRESS_OAUTH_SCOPE`: optional, default `member-profile`.
- `CLUBEXPRESS_SSO_REDIRECT_URI`: optional. If omitted, the probe derives the callback URL from the `/start` request.
- `CLUBEXPRESS_MEMBER_INFO_URL`: optional, default `https://ws.clubexpress.com/member_info.ashx`.

The separate ClubExpress web service key is not currently used by the OAuth probe because ClubExpress documented the member-info endpoint as using the OAuth access token. The optional `member_number` parameter is only sent when the callback or token response includes one.

The token request follows the ClubExpress implementation guide: it posts `grant_type=authorization_code`, `code`, and `redirect_uri` as `application/x-www-form-urlencoded`, while sending the client credentials in an HTTP Basic `Authorization` header. It also sends the guide's `Accept-Encoding`, `Connection`, `Host`, and Java/Apache HTTP client User-Agent headers. During diagnostics it retries each token URL candidate and records every attempt.

## ClubExpress URLs

Prefer the short URLs below because the ClubExpress SSO Options fields may truncate long values.

Register this as the SSO callback/redirect URL, replacing the host and token:

```text
https://<your-function-app>.azurewebsites.net/api/c/<SSO_PROBE_TOKEN>
```

Use this URL as the SSO Home URL:

```text
https://<your-function-app>.azurewebsites.net/api/s/<SSO_PROBE_TOKEN>
```

The longer compatibility URLs also work when the ClubExpress fields can hold them:

```text
https://<your-function-app>.azurewebsites.net/api/clubexpress-sso-probe/<SSO_PROBE_TOKEN>
```

```text
https://<your-function-app>.azurewebsites.net/api/clubexpress-sso-probe-start/<SSO_PROBE_TOKEN>
```

For a ClubExpress custom HTML page, link to the `/start` URL, not directly to the callback URL:

```html
<a href="https://<your-function-app>.azurewebsites.net/api/s/<SSO_PROBE_TOKEN>">
  Sign in with ClubExpress
</a>
```

After triggering SSO, visit:

```text
https://<your-function-app>.azurewebsites.net/api/clubexpress-sso-probe-last/<SSO_PROBE_TOKEN>
```

This returns recent in-memory captures and, when storage is available, recent persisted captures from blob storage.

## Expected Flow

1. The user opens `/api/clubexpress-sso-probe-start/<SSO_PROBE_TOKEN>`.
2. The probe redirects to ClubExpress `page_id=597`.
3. ClubExpress authenticates the user and redirects to `/api/clubexpress-sso-probe/<SSO_PROBE_TOKEN>?code=...&state=...`.
4. If the client ID and secret are configured, the probe exchanges the code at the configured token URL candidates. Client credentials are sent with HTTP Basic auth.
5. If the token response includes an access token, the probe calls `https://ws.clubexpress.com/member_info.ashx`. It includes `member_number` only when one is available.

## Security Notes

This is intentionally diagnostic and should be temporary.

- Use HTTPS.
- Use a long random `SSO_PROBE_TOKEN`.
- Do not store the ClubExpress client secret in code.
- Remove or disable the Function App after testing.
- OAuth codes, tokens, cookies, and client secrets are redacted from stored and returned captures.
- Member profile fields returned by ClubExpress can contain personal information. Keep the probe private and temporary.
