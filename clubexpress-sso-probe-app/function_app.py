import base64
import gzip
import json
import logging
import os
import secrets
import uuid
import zlib
from datetime import datetime, timezone
from http import HTTPStatus
from http.cookies import SimpleCookie
from typing import Any
from urllib import error, parse, request

import azure.functions as func
try:
    from azure.storage.blob import BlobServiceClient, ContentSettings
except Exception:
    BlobServiceClient = None
    ContentSettings = None


app = func.FunctionApp()

MAX_CAPTURE_BYTES = int(os.environ.get("SSO_PROBE_MAX_CAPTURE_BYTES", "65536"))
CAPTURE_LIMIT = int(os.environ.get("SSO_PROBE_CAPTURE_LIMIT", "10"))
CAPTURE_CONTAINER = os.environ.get("SSO_PROBE_CAPTURE_CONTAINER", "clubexpress-sso-probe-captures")
EXPECTED_PROBE_TOKEN = os.environ.get("SSO_PROBE_TOKEN", "")
CLUBEXPRESS_CLUB_DOMAIN = os.environ.get("CLUBEXPRESS_CLUB_DOMAIN", "https://usgo.org").rstrip("/")
OAUTH_AUTHORIZE_URL = os.environ.get("CLUBEXPRESS_OAUTH_AUTHORIZE_URL", "")
OAUTH_TOKEN_URL = os.environ.get("CLUBEXPRESS_OAUTH_TOKEN_URL", "")
OAUTH_TOKEN_URLS = os.environ.get("CLUBEXPRESS_OAUTH_TOKEN_URLS", "")
OAUTH_TENANT_DOMAIN = os.environ.get("CLUBEXPRESS_OAUTH_TENANT_DOMAIN", "").rstrip("/")
try:
    OAUTH_TOKEN_URL_RETRIES = max(1, int(os.environ.get("CLUBEXPRESS_OAUTH_TOKEN_URL_RETRIES", "4")))
except ValueError:
    OAUTH_TOKEN_URL_RETRIES = 4
OAUTH_CLIENT_ID = os.environ.get("CLUBEXPRESS_OAUTH_CLIENT_ID", "")
OAUTH_CLIENT_SECRET = os.environ.get("CLUBEXPRESS_OAUTH_CLIENT_SECRET", "")
OAUTH_SCOPE = os.environ.get("CLUBEXPRESS_OAUTH_SCOPE", "member-profile")
OAUTH_REDIRECT_URI = os.environ.get("CLUBEXPRESS_SSO_REDIRECT_URI", "")
MEMBER_INFO_URL = os.environ.get(
    "CLUBEXPRESS_MEMBER_INFO_URL",
    "https://ws.clubexpress.com/member_info.ashx",
)
STORAGE_CONNECTION_STRING = (
    os.environ.get("SSO_PROBE_STORAGE_CONNECTION_STRING")
    or os.environ.get("AzureWebJobsStorage")
    or ""
)
SENSITIVE_QUERY_KEYS = {
    "access_token",
    "authorization",
    "client_secret",
    "code",
    "id_token",
    "refresh_token",
    "token",
}
SENSITIVE_HEADER_KEYS = {
    "authorization",
    "cookie",
    "cookies",
    "set-cookie",
    "x-ms-client-principal",
}

_captures: list[dict[str, Any]] = []
_capture_container_client = None


def _now_utc() -> str:
    """Execute the now utc routine."""
    return datetime.now(timezone.utc).isoformat()


def _as_multi_value_dict(items: dict[str, str]) -> dict[str, list[str]]:
    """Execute the as multi value dict routine."""
    return {key: [value] for key, value in items.items()}


def _query_from_url(url: str) -> dict[str, list[str]]:
    """Query from url."""
    return parse.parse_qs(parse.urlsplit(url).query, keep_blank_values=True)


def _url_with_query(url: str, params: dict[str, str]) -> str:
    """Execute the url with query routine."""
    parts = parse.urlsplit(url)
    query_items = parse.parse_qsl(parts.query, keep_blank_values=True)
    query_items.extend((key, value) for key, value in params.items() if value is not None)
    return parse.urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            parse.urlencode(query_items),
            parts.fragment,
        )
    )


def _redacted_value(value: Any) -> str:
    """Execute the redacted value routine."""
    text = "" if value is None else str(value)
    return f"[redacted length={len(text)}]"


def _redact_url(url: str) -> str:
    """Execute the redact url routine."""
    parts = parse.urlsplit(url)
    query_items = []
    for key, value in parse.parse_qsl(parts.query, keep_blank_values=True):
        if key.lower() in SENSITIVE_QUERY_KEYS:
            query_items.append((key, _redacted_value(value)))
        else:
            query_items.append((key, value))
    return parse.urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            parse.urlencode(query_items),
            parts.fragment,
        )
    )


def _looks_like_sensitive_url(value: str) -> bool:
    """Execute the looks like sensitive url routine."""
    if "?" not in value:
        return False

    query = parse.urlsplit(value).query
    if not query:
        return False

    return any(key.lower() in SENSITIVE_QUERY_KEYS for key in parse.parse_qs(query, keep_blank_values=True))


def _redact_text(text: str) -> str:
    """Execute the redact text routine."""
    if not text:
        return text

    if _looks_like_sensitive_url(text):
        return _redact_url(text)

    parsed = parse.parse_qs(text, keep_blank_values=True)
    if parsed and any(key.lower() in SENSITIVE_QUERY_KEYS for key in parsed):
        safe_pairs = []
        for key, values in parsed.items():
            for value in values:
                safe_pairs.append(
                    (
                        key,
                        _redacted_value(value) if key.lower() in SENSITIVE_QUERY_KEYS else value,
                    )
                )
        return parse.urlencode(safe_pairs)

    return text


def _sanitize_for_output(value: Any, key_name: str = "") -> Any:
    """Execute the sanitize for output routine."""
    lowered_key = key_name.lower()
    if lowered_key in SENSITIVE_QUERY_KEYS or lowered_key in SENSITIVE_HEADER_KEYS:
        return _redacted_value(value)

    if isinstance(value, dict):
        return {key: _sanitize_for_output(item, str(key)) for key, item in value.items()}

    if isinstance(value, list):
        return [_sanitize_for_output(item, key_name) for item in value]

    if lowered_key == "url" and isinstance(value, str):
        return _redact_url(value)

    if lowered_key == "text" and isinstance(value, str):
        return _redact_text(value)

    if isinstance(value, str) and _looks_like_sensitive_url(value):
        return _redact_url(value)

    if lowered_key == "base64":
        return "[redacted; raw request body omitted from OAuth probe output]"

    return value


def _header_dict(req: func.HttpRequest) -> dict[str, str]:
    """Execute the header dict routine."""
    return {key: value for key, value in req.headers.items()}


def _cookies_from_header(headers: dict[str, str]) -> dict[str, str]:
    """Execute the cookies from header routine."""
    cookie_header = headers.get("cookie") or headers.get("Cookie") or ""
    if not cookie_header:
        return {}

    cookie = SimpleCookie()
    cookie.load(cookie_header)
    return {key: morsel.value for key, morsel in cookie.items()}


def _body_text(body: bytes) -> tuple[str, bool]:
    """Execute the body text routine."""
    clipped = body[:MAX_CAPTURE_BYTES]
    text = clipped.decode("utf-8", errors="replace")
    return text, len(body) > MAX_CAPTURE_BYTES


def _content_type(headers: dict[str, str]) -> str:
    """Execute the content type routine."""
    return (headers.get("content-type") or headers.get("Content-Type") or "").lower()


def _parsed_body(body_text: str, content_type: str) -> dict[str, Any] | None:
    """Execute the parsed body routine."""
    if not body_text:
        return None

    if "application/json" in content_type:
        try:
            return {"json": json.loads(body_text)}
        except json.JSONDecodeError as exc:
            return {"json_error": str(exc)}

    if "application/x-www-form-urlencoded" in content_type:
        return {"form": parse.parse_qs(body_text, keep_blank_values=True)}

    return None


def _request_url_without_query(url: str) -> str:
    """Execute the request url without query routine."""
    parts = parse.urlsplit(url)
    return parse.urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _authorization_url() -> str:
    """Execute the authorization url routine."""
    return OAUTH_AUTHORIZE_URL or f"{CLUBEXPRESS_CLUB_DOMAIN}/content.aspx"


def _dedupe_urls(urls: list[str]) -> list[str]:
    """Execute the dedupe urls routine."""
    seen = set()
    deduped = []
    for url in urls:
        normalized = url.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _token_urls() -> list[str]:
    """Execute the token urls routine."""
    if OAUTH_TOKEN_URLS:
        return _dedupe_urls(
            [url for url in OAUTH_TOKEN_URLS.replace("\n", ",").split(",")]
        )

    if OAUTH_TOKEN_URL:
        return [OAUTH_TOKEN_URL]

    urls = []
    if OAUTH_TENANT_DOMAIN:
        urls.append(f"{OAUTH_TENANT_DOMAIN}/oauth/token.aspx")

    urls.extend(
        [
            "https://aga1.clubexpress.com/oauth/token.aspx",
            f"{CLUBEXPRESS_CLUB_DOMAIN}/oauth/token.aspx",
        ]
    )
    return _dedupe_urls(urls)


def _basic_auth_header(client_id: str, client_secret: str) -> str:
    """Execute the basic auth header routine."""
    credentials = f"{client_id}:{client_secret}".encode("utf-8")
    return f"Basic {base64.b64encode(credentials).decode('ascii')}"


def _callback_url_for_request(req: func.HttpRequest, probe_token: str) -> str:
    """Execute the callback url for request routine."""
    if OAUTH_REDIRECT_URI:
        return OAUTH_REDIRECT_URI

    parts = parse.urlsplit(req.url)
    callback_path = parts.path
    replacements = (
        (f"s/{probe_token}", f"c/{probe_token}"),
        (
            f"clubexpress-sso-probe-start/{probe_token}",
            f"clubexpress-sso-probe/{probe_token}",
        ),
    )
    for start_path, callback_route in replacements:
        if start_path in callback_path:
            callback_path = callback_path.replace(start_path, callback_route, 1)
            break
    return parse.urlunsplit((parts.scheme, parts.netloc, callback_path, "", ""))


def _build_authorization_start(req: func.HttpRequest, probe_token: str) -> dict[str, str]:
    """Build authorization start."""
    client_id = req.params.get("client_id") or OAUTH_CLIENT_ID
    if not client_id:
        raise ValueError("Missing CLUBEXPRESS_OAUTH_CLIENT_ID app setting or client_id query parameter.")

    redirect_uri = req.params.get("redirect_uri") or _callback_url_for_request(req, probe_token)
    state = req.params.get("state") or secrets.token_urlsafe(24)
    scope = req.params.get("scope") or OAUTH_SCOPE
    authorize_url = _authorization_url()
    existing_query = parse.parse_qs(parse.urlsplit(authorize_url).query)
    auth_params = {
        "client_id": client_id,
        "scope": scope,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "state": state,
    }
    if "page_id" not in existing_query:
        auth_params = {"page_id": "597", **auth_params}

    return {
        "authorization_url": _url_with_query(authorize_url, auth_params),
        "callback_url": redirect_uri,
        "client_id": client_id,
        "scope": scope,
        "state": state,
    }


def _build_capture(req: func.HttpRequest, probe_token: str) -> dict[str, Any]:
    """Build capture."""
    body = req.get_body()
    body_text, body_clipped = _body_text(body)
    headers = _header_dict(req)
    content_type = _content_type(headers)
    parsed_body = _parsed_body(body_text, content_type)

    capture: dict[str, Any] = {
        "timestamp_utc": _now_utc(),
        "method": req.method,
        "url": req.url,
        "path": parse.urlsplit(req.url).path,
        "probe_token_path_segment": probe_token,
        "headers": headers,
        "cookies": _cookies_from_header(headers),
        "query": _query_from_url(req.url),
        "azure_params": _as_multi_value_dict(dict(req.params)),
        "route_params": dict(req.route_params),
        "body": {
            "content_type": content_type,
            "byte_count": len(body),
            "is_clipped": body_clipped,
            "text": body_text,
            "base64": base64.b64encode(body[:MAX_CAPTURE_BYTES]).decode("ascii"),
        },
    }

    if parsed_body is not None:
        capture["parsed_body"] = parsed_body

    if not EXPECTED_PROBE_TOKEN:
        capture["probe_warning"] = (
            "SSO_PROBE_TOKEN is not configured. Any token path segment will be accepted."
        )

    return capture


def _remember_capture(capture: dict[str, Any]) -> None:
    """Execute the remember capture routine."""
    _captures.append(capture)
    del _captures[:-CAPTURE_LIMIT]


def _get_capture_container_client():
    """Return capture container client."""
    global _capture_container_client

    if _capture_container_client is not None:
        return _capture_container_client

    if BlobServiceClient is None:
        logging.warning("azure-storage-blob is unavailable; captures will only be stored in memory.")
        return None

    if not STORAGE_CONNECTION_STRING:
        logging.warning("No storage connection string is configured; captures will only be stored in memory.")
        return None

    try:
        service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        container_client = service_client.get_container_client(CAPTURE_CONTAINER)
        try:
            container_client.create_container()
        except Exception as exc:
            if "ContainerAlreadyExists" not in repr(exc) and "container already exists" not in str(exc).lower():
                raise
        _capture_container_client = container_client
        return container_client
    except Exception:
        logging.exception("Unable to initialize SSO probe capture blob container.")
        return None


def _persist_capture(capture: dict[str, Any]) -> dict[str, Any]:
    """Execute the persist capture routine."""
    container_client = _get_capture_container_client()
    if container_client is None:
        return {"persisted": False, "reason": "blob storage unavailable; retained in memory only"}

    timestamp = capture["timestamp_utc"].replace(":", "").replace("+", "Z")
    blob_name = f"capture-{timestamp}-{uuid.uuid4().hex}.json"
    document = dict(capture)
    document["storage"] = {
        "container": CAPTURE_CONTAINER,
        "blob": blob_name,
    }

    try:
        kwargs: dict[str, Any] = {"overwrite": False}
        if ContentSettings is not None:
            kwargs["content_settings"] = ContentSettings(content_type="application/json")
        container_client.upload_blob(blob_name, json.dumps(document, indent=2, sort_keys=True), **kwargs)
        return {"persisted": True, "container": CAPTURE_CONTAINER, "blob": blob_name}
    except Exception as exc:
        logging.exception("Unable to persist SSO probe capture to blob storage.")
        return {"persisted": False, "reason": repr(exc)}


def _recent_persisted_captures(limit: int) -> tuple[list[dict[str, Any]], str | None]:
    """Execute the recent persisted captures routine."""
    container_client = _get_capture_container_client()
    if container_client is None:
        return [], "blob storage unavailable"

    try:
        blobs = []
        for blob in container_client.list_blobs(name_starts_with="capture-"):
            blobs.append(blob)
        blobs.sort(key=lambda blob: blob.last_modified, reverse=True)

        captures = []
        for blob in blobs[:limit]:
            raw = container_client.download_blob(blob.name).readall()
            captures.append(json.loads(raw.decode("utf-8")))
        return captures, None
    except Exception as exc:
        logging.exception("Unable to read recent SSO probe captures from blob storage.")
        return [], repr(exc)


def _json_response(payload: dict[str, Any] | list[dict[str, Any]], status_code: int = 200) -> func.HttpResponse:
    """Execute the json response routine."""
    return func.HttpResponse(
        json.dumps(payload, indent=2, sort_keys=True),
        status_code=status_code,
        mimetype="application/json",
    )


def _is_authorized_probe_token(probe_token: str) -> bool:
    """Return whether authorized probe token."""
    return not EXPECTED_PROBE_TOKEN or probe_token == EXPECTED_PROBE_TOKEN


def _first_capture_value(capture: dict[str, Any], *names: str) -> str | None:
    """Execute the first capture value routine."""
    lowered_names = {name.lower() for name in names}
    query = capture.get("query", {})
    for key, values in query.items():
        if key.lower() in lowered_names and values:
            return values[0]

    parsed_form = capture.get("parsed_body", {}).get("form", {})
    for key, values in parsed_form.items():
        if key.lower() in lowered_names and values:
            return values[0]

    return None


def _parse_response_body(body_text: str) -> Any:
    """Parse response body."""
    if not body_text:
        return ""

    try:
        return json.loads(body_text)
    except json.JSONDecodeError:
        pass

    if body_text.lstrip().startswith("<"):
        return body_text

    parsed_form = parse.parse_qs(body_text, keep_blank_values=True)
    if parsed_form:
        return {
            key: values[0] if len(values) == 1 else values
            for key, values in parsed_form.items()
        }

    return body_text


def _decode_http_body(raw_body: bytes, headers: Any) -> str:
    """Decode http body."""
    encoding = ""
    try:
        encoding = (headers.get("Content-Encoding") or "").lower()
    except Exception:
        encoding = ""

    try:
        if "gzip" in encoding:
            raw_body = gzip.decompress(raw_body)
        elif "deflate" in encoding:
            try:
                raw_body = zlib.decompress(raw_body)
            except zlib.error:
                raw_body = zlib.decompress(raw_body, -zlib.MAX_WBITS)
    except Exception as exc:
        prefix = f"[unable to decode {encoding} response body: {exc!r}] "
        return prefix + raw_body.decode("utf-8", errors="replace")

    return raw_body.decode("utf-8", errors="replace")


def _first_nested_value(value: Any, *names: str) -> str | None:
    """Execute the first nested value routine."""
    lowered_names = {name.lower() for name in names}
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).lower() in lowered_names and item not in (None, ""):
                if isinstance(item, list):
                    return str(item[0]) if item else None
                return str(item)
        for item in value.values():
            found = _first_nested_value(item, *names)
            if found:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _first_nested_value(item, *names)
            if found:
                return found
    return None


def _member_number_from_response(capture: dict[str, Any], token_body: Any) -> str | None:
    """Execute the member number from response routine."""
    return (
        _first_capture_value(capture, "member_number", "membernumber", "member_no", "member")
        or _first_nested_value(
            token_body,
            "member_number",
            "membernumber",
            "member_no",
            "member",
            "member_id",
            "memberid",
        )
    )


def _try_fetch_member_info(access_token: str, member_number: str | None = None) -> dict[str, Any]:
    """Execute the try fetch member info routine."""
    params = {"access_token": access_token}
    if member_number:
        params["member_number"] = member_number

    member_info_url = _url_with_query(MEMBER_INFO_URL, params)

    try:
        member_req = request.Request(member_info_url, method="GET")
        with request.urlopen(member_req, timeout=15) as response:
            response_body = response.read(MAX_CAPTURE_BYTES).decode("utf-8", errors="replace")
            return {
                "attempted": True,
                "url": member_info_url,
                "status": response.status,
                "headers": dict(response.headers.items()),
                "body": _parse_response_body(response_body),
            }
    except error.HTTPError as exc:
        response_body = exc.read(MAX_CAPTURE_BYTES).decode("utf-8", errors="replace")
        return {
            "attempted": True,
            "url": member_info_url,
            "status": exc.code,
            "headers": dict(exc.headers.items()) if exc.headers else {},
            "body": _parse_response_body(response_body),
            "error": repr(exc),
        }
    except Exception as exc:
        return {
            "attempted": True,
            "url": member_info_url,
            "error": repr(exc),
        }


def _try_exchange_authorization_code(capture: dict[str, Any]) -> dict[str, Any] | None:
    """Execute the try exchange authorization code routine."""
    code = _first_capture_value(capture, "code")
    if not code:
        return None

    if not (OAUTH_CLIENT_ID and OAUTH_CLIENT_SECRET):
        return {
            "attempted": False,
            "reason": (
                "Authorization code was present, but CLUBEXPRESS_OAUTH_CLIENT_ID "
                "and CLUBEXPRESS_OAUTH_CLIENT_SECRET must both be configured before token exchange."
            ),
            "required_settings": [
                "CLUBEXPRESS_OAUTH_CLIENT_ID",
                "CLUBEXPRESS_OAUTH_CLIENT_SECRET",
            ],
        }

    redirect_uri = OAUTH_REDIRECT_URI or _request_url_without_query(capture["url"])
    post_fields = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    post_data = parse.urlencode(post_fields).encode("utf-8")

    token_attempts = []
    common_exchange = {
        "attempted": True,
        "client_auth": "http_basic",
        "redirect_uri": redirect_uri,
        "request_body_fields": sorted(post_fields),
    }

    for token_url in _token_urls():
        for attempt_number in range(1, OAUTH_TOKEN_URL_RETRIES + 1):
            token_req = request.Request(
                token_url,
                data=post_data,
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip,deflate",
                    "Authorization": _basic_auth_header(OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET),
                    "Connection": "Keep-Alive",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Host": parse.urlsplit(token_url).netloc,
                    "User-Agent": "Apache-HttpClient/4.5.5 (Java/15)",
                },
                method="POST",
            )

            try:
                with request.urlopen(token_req, timeout=15) as response:
                    response_body = _decode_http_body(
                        response.read(MAX_CAPTURE_BYTES),
                        response.headers,
                    )
                    parsed_response = _parse_response_body(response_body)
                    exchange = {
                        **common_exchange,
                        "token_url": token_url,
                        "token_url_attempt_number": attempt_number,
                        "token_url_candidates": _token_urls(),
                        "token_url_retries_per_candidate": OAUTH_TOKEN_URL_RETRIES,
                        "attempts": token_attempts,
                        "status": response.status,
                        "headers": dict(response.headers.items()),
                        "body": parsed_response,
                    }

                    access_token = _first_nested_value(parsed_response, "access_token", "token")
                    member_number = _member_number_from_response(capture, parsed_response)
                    if access_token:
                        exchange["member_info"] = _try_fetch_member_info(access_token, member_number)
                    return exchange
            except error.HTTPError as exc:
                response_body = _decode_http_body(
                    exc.read(MAX_CAPTURE_BYTES),
                    exc.headers or {},
                )
                attempt = {
                    "token_url": token_url,
                    "attempt_number": attempt_number,
                    "status": exc.code,
                    "headers": dict(exc.headers.items()) if exc.headers else {},
                    "body": _parse_response_body(response_body),
                    "error": repr(exc),
                }
                token_attempts.append(attempt)
                if exc.code not in {404, 500, 502, 503, 504}:
                    return {
                        **common_exchange,
                        "token_url": token_url,
                        "token_url_candidates": _token_urls(),
                        "token_url_retries_per_candidate": OAUTH_TOKEN_URL_RETRIES,
                        "attempts": token_attempts,
                        **attempt,
                    }
            except Exception as exc:
                token_attempts.append(
                    {
                        "token_url": token_url,
                        "attempt_number": attempt_number,
                        "error": repr(exc),
                    }
                )

    return {
        **common_exchange,
        "token_url": token_attempts[-1]["token_url"] if token_attempts else None,
        "token_url_candidates": _token_urls(),
        "token_url_retries_per_candidate": OAUTH_TOKEN_URL_RETRIES,
        "attempts": token_attempts,
        "status": token_attempts[-1].get("status") if token_attempts else None,
        "error": "All configured ClubExpress token URL candidates failed.",
    }


@app.route(
    route="clubexpress-sso-probe-start/{probe_token}",
    auth_level=func.AuthLevel.ANONYMOUS,
    methods=["GET"],
)
def clubexpress_sso_probe_start(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the clubexpress-sso-probe-start/{probe_token} Azure Function endpoint."""
    probe_token = req.route_params.get("probe_token", "")
    if not _is_authorized_probe_token(probe_token):
        return _json_response({"error": "not found"}, status_code=HTTPStatus.NOT_FOUND)

    try:
        start = _build_authorization_start(req, probe_token)
    except ValueError as exc:
        return _json_response(
            {
                "error": str(exc),
                "required_settings": [
                    "CLUBEXPRESS_OAUTH_CLIENT_ID",
                ],
                "optional_settings": [
                    "CLUBEXPRESS_CLUB_DOMAIN",
                    "CLUBEXPRESS_OAUTH_AUTHORIZE_URL",
                    "CLUBEXPRESS_OAUTH_SCOPE",
                    "CLUBEXPRESS_SSO_REDIRECT_URI",
                ],
            },
            status_code=HTTPStatus.BAD_REQUEST,
        )

    if req.params.get("format") == "json" or req.params.get("dry_run") == "1":
        return _json_response(
            {
                "message": "ClubExpress SSO probe start URL.",
                "authorization_url": start["authorization_url"],
                "callback_url": start["callback_url"],
                "scope": start["scope"],
                "state": start["state"],
                "html_link": (
                    f'<a href="{start["authorization_url"]}">'
                    "Sign in with ClubExpress</a>"
                ),
                "notes": [
                    "Use the authorization_url as the link target, or link to this /start route.",
                    "The callback URL must match the value registered on the ClubExpress SSO Options page.",
                ],
            }
        )

    return func.HttpResponse(
        "",
        status_code=HTTPStatus.FOUND,
        headers={"Location": start["authorization_url"]},
    )


@app.route(
    route="clubexpress-sso-probe/{probe_token}",
    auth_level=func.AuthLevel.ANONYMOUS,
    methods=["GET", "POST", "OPTIONS"],
)
def clubexpress_sso_probe(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the clubexpress-sso-probe/{probe_token} Azure Function endpoint."""
    probe_token = req.route_params.get("probe_token", "")
    if not _is_authorized_probe_token(probe_token):
        return _json_response({"error": "not found"}, status_code=HTTPStatus.NOT_FOUND)

    capture = _build_capture(req, probe_token)
    token_exchange = _try_exchange_authorization_code(capture)
    if token_exchange is not None:
        capture["oauth_token_exchange"] = token_exchange

    output_capture = _sanitize_for_output(capture)
    _remember_capture(output_capture)
    output_capture["storage"] = _persist_capture(output_capture)
    logging.warning("ClubExpress SSO probe received: %s", json.dumps(output_capture, sort_keys=True))

    return _json_response(
        {
            "message": "ClubExpress SSO probe received this request.",
            "capture": output_capture,
            "notes": [
                "This endpoint is for temporary diagnostics only.",
                "Remove or disable it after testing ClubExpress SSO.",
                "OAuth codes, tokens, cookies, and client secrets are redacted from stored and returned captures.",
            ],
        }
    )


@app.route(
    route="s/{probe_token}",
    auth_level=func.AuthLevel.ANONYMOUS,
    methods=["GET"],
)
def clubexpress_sso_probe_start_short(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the s/{probe_token} Azure Function endpoint."""
    return clubexpress_sso_probe_start(req)


@app.route(
    route="c/{probe_token}",
    auth_level=func.AuthLevel.ANONYMOUS,
    methods=["GET", "POST", "OPTIONS"],
)
def clubexpress_sso_probe_short(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the c/{probe_token} Azure Function endpoint."""
    return clubexpress_sso_probe(req)


@app.route(
    route="clubexpress-sso-probe-last/{probe_token}",
    auth_level=func.AuthLevel.ANONYMOUS,
    methods=["GET"],
)
def clubexpress_sso_probe_last(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the clubexpress-sso-probe-last/{probe_token} Azure Function endpoint."""
    probe_token = req.route_params.get("probe_token", "")
    if not _is_authorized_probe_token(probe_token):
        return _json_response({"error": "not found"}, status_code=HTTPStatus.NOT_FOUND)

    persisted_captures, persisted_error = _recent_persisted_captures(CAPTURE_LIMIT)
    return _json_response(
        {
            "message": "Recent in-memory ClubExpress SSO probe captures.",
            "capture_count": len(_captures),
            "captures": _captures,
            "persisted_capture_count": len(persisted_captures),
            "persisted_captures": persisted_captures,
            "persisted_error": persisted_error,
            "warning": (
                "In-memory captures can disappear when Azure Functions recycles or scales instances. "
                "Persisted captures come from blob storage when available."
            ),
        }
    )


@app.route(
    route="l/{probe_token}",
    auth_level=func.AuthLevel.ANONYMOUS,
    methods=["GET"],
)
def clubexpress_sso_probe_last_short(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the l/{probe_token} Azure Function endpoint."""
    return clubexpress_sso_probe_last(req)
