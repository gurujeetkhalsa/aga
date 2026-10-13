# Copyright 2026, American Go Association, All rights reserved

from __future__ import annotations

import json
import logging
from pathlib import Path
from urllib.parse import quote, urlsplit

import azure.functions as func

import chapter_finder_update_support as portal
from admin_auth import authorize_reviewer
from sql_adapter import SqlAdapter, get_sql_connection_string


app = func.FunctionApp()
_SQL_CONNECTION_STRING: str | None = None


def _connection_string() -> str | None:
    global _SQL_CONNECTION_STRING
    if _SQL_CONNECTION_STRING is None:
        _SQL_CONNECTION_STRING = get_sql_connection_string()
    return _SQL_CONNECTION_STRING


def _adapter() -> SqlAdapter:
    connection_string = _connection_string()
    if not connection_string:
        raise RuntimeError("SQL connection is not configured.")
    return SqlAdapter(connection_string)


def _headers(content_type: str, *, private: bool = True) -> dict[str, str]:
    headers = {
        "Content-Type": content_type,
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "no-referrer",
        "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
        "Content-Security-Policy": (
            "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; "
            "script-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'self' https://usgo.org https://www.usgo.org"
        ),
    }
    if private:
        headers["Cache-Control"] = "no-store, private"
        headers["Pragma"] = "no-cache"
    return headers


def _json(payload: Any, status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload, default=portal.json_safe),
        status_code=status_code,
        headers=_headers("application/json; charset=utf-8"),
    )


def _text(message: str, status_code: int) -> func.HttpResponse:
    return func.HttpResponse(
        message,
        status_code=status_code,
        headers=_headers("text/plain; charset=utf-8"),
    )


def _html(filename: str) -> func.HttpResponse:
    markup = (Path(__file__).resolve().parent / filename).read_text(encoding="utf-8")
    return func.HttpResponse(markup, status_code=200, headers=_headers("text/html; charset=utf-8"))


def _options() -> func.HttpResponse:
    headers = _headers("application/json; charset=utf-8")
    headers.update(
        {
            "Access-Control-Allow-Headers": "Authorization, Content-Type",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        }
    )
    return func.HttpResponse("", status_code=204, headers=headers)


def _request_json(req: func.HttpRequest) -> dict[str, Any]:
    try:
        value = req.get_json()
    except ValueError as exc:
        raise portal.PortalError("Request body must be JSON.") from exc
    if not isinstance(value, dict):
        raise portal.PortalError("Request body must be a JSON object.")
    return value


def _bearer_token(req: func.HttpRequest) -> str:
    authorization = (req.headers.get("Authorization") or "").strip()
    if not authorization.lower().startswith("bearer "):
        raise portal.PortalError("This chapter link is invalid.", 401)
    token = authorization[7:].strip()
    if not token:
        raise portal.PortalError("This chapter link is invalid.", 401)
    return token


def _login_redirect(req: func.HttpRequest) -> str:
    parsed = urlsplit(getattr(req, "url", "") or "/api/chapter-finder/review")
    path = parsed.path or "/api/chapter-finder/review"
    if parsed.query:
        path += f"?{parsed.query}"
    return f"/.auth/login/aad?post_login_redirect_uri={quote(path, safe='')}"


def _reviewer(req: func.HttpRequest, adapter: SqlAdapter, *, html: bool = False) -> tuple[dict[str, Any] | None, func.HttpResponse | None]:
    result = authorize_reviewer(req.headers, adapter)
    if result.ok and result.principal:
        return {
            "principal_name": result.principal.principal_name,
            "display_name": result.principal.display_name,
            "permission_code": result.admin_row.get("Permission_Code") if result.admin_row else None,
        }, None
    if html and result.status_code == 401:
        headers = _headers("text/plain; charset=utf-8")
        headers["Location"] = _login_redirect(req)
        return None, func.HttpResponse("", status_code=302, headers=headers)
    return None, _json({"ok": False, "error": result.error or "Authorization failed."}, result.status_code)


def _handle_error(exc: Exception) -> func.HttpResponse:
    if isinstance(exc, portal.PortalError):
        return _json({"ok": False, "error": str(exc)}, exc.status_code)
    logging.exception("Chapter Finder portal request failed")
    return _json({"ok": False, "error": "The request could not be completed."}, 500)


@app.function_name(name="ChapterFinderUpdatePage")
@app.route(route="chapter-finder/update", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_finder_update_page(req: func.HttpRequest) -> func.HttpResponse:
    return _html("chapter_update.html")


@app.function_name(name="ChapterFinderUpdateContext")
@app.route(route="chapter-finder/update/context", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_finder_update_context(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return _options()
    try:
        context = portal.load_access_context(_adapter(), _bearer_token(req), record_use=True)
        return _json({"ok": True, **portal.context_payload(context)})
    except Exception as exc:
        return _handle_error(exc)


@app.function_name(name="ChapterFinderUpdateMember")
@app.route(route="chapter-finder/update/member", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_finder_update_member(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return _options()
    try:
        raw_agaid = (req.params.get("agaid") or "").strip()
        if not raw_agaid.isdigit() or int(raw_agaid) <= 0:
            raise portal.PortalError("Enter a valid AGA ID.")
        member = portal.lookup_member_for_access_token(_adapter(), _bearer_token(req), int(raw_agaid))
        return _json({"ok": True, "member": member})
    except Exception as exc:
        return _handle_error(exc)


@app.function_name(name="ChapterFinderUpdateSubmit")
@app.route(route="chapter-finder/update/submit", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_finder_update_submit(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return _options()
    try:
        result = portal.submit_update(_adapter(), _bearer_token(req), _request_json(req))
        return _json({"ok": True, **result}, 201)
    except Exception as exc:
        return _handle_error(exc)


@app.function_name(name="ChapterFinderReviewPage")
@app.route(route="chapter-finder/review", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_finder_review_page(req: func.HttpRequest) -> func.HttpResponse:
    try:
        adapter = _adapter()
        _, error = _reviewer(req, adapter, html=True)
        if error:
            return error
        return _html("chapter_review.html")
    except Exception as exc:
        return _handle_error(exc)


@app.function_name(name="ChapterFinderReviewMe")
@app.route(route="chapter-finder/review/me", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_finder_review_me(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return _options()
    try:
        adapter = _adapter()
        reviewer, error = _reviewer(req, adapter)
        return error or _json({"ok": True, "reviewer": reviewer})
    except Exception as exc:
        return _handle_error(exc)


@app.function_name(name="ChapterFinderReviewRequests")
@app.route(route="chapter-finder/review/requests", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_finder_review_requests(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return _options()
    try:
        adapter = _adapter()
        reviewer, error = _reviewer(req, adapter)
        if error:
            return error
        limit_text = (req.params.get("limit") or "100").strip()
        if not limit_text.isdigit():
            raise portal.PortalError("Limit must be a positive integer.")
        rows = portal.list_review_requests(adapter, int(limit_text))
        return _json({"ok": True, "reviewer": reviewer, "requests": rows})
    except Exception as exc:
        return _handle_error(exc)


@app.function_name(name="ChapterFinderReviewRequest")
@app.route(route="chapter-finder/review/request", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_finder_review_request(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return _options()
    try:
        adapter = _adapter()
        reviewer, error = _reviewer(req, adapter)
        if error:
            return error
        request_id = _positive_request_id(req.params.get("request_id"))
        detail = portal.load_review_request(adapter, request_id)
        return _json({"ok": True, "reviewer": reviewer, **detail})
    except Exception as exc:
        return _handle_error(exc)


@app.function_name(name="ChapterFinderReviewApprove")
@app.route(route="chapter-finder/review/approve", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_finder_review_approve(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return _options()
    try:
        adapter = _adapter()
        reviewer, error = _reviewer(req, adapter)
        if error:
            return error
        body = _request_json(req)
        request_id = _positive_request_id(body.get("request_id"))
        result = portal.approve_request(adapter, request_id, reviewer["principal_name"], body.get("review_notes"))
        return _json({"ok": True, "request": result})
    except Exception as exc:
        return _handle_error(exc)


@app.function_name(name="ChapterFinderReviewReject")
@app.route(route="chapter-finder/review/reject", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_finder_review_reject(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return _options()
    try:
        adapter = _adapter()
        reviewer, error = _reviewer(req, adapter)
        if error:
            return error
        body = _request_json(req)
        request_id = _positive_request_id(body.get("request_id"))
        portal.reject_request(adapter, request_id, reviewer["principal_name"], body.get("review_notes"))
        return _json({"ok": True, "request_id": request_id, "status": "rejected"})
    except Exception as exc:
        return _handle_error(exc)


def _positive_request_id(value: Any) -> int:
    try:
        request_id = int(value)
    except (TypeError, ValueError) as exc:
        raise portal.PortalError("A valid request ID is required.") from exc
    if request_id <= 0:
        raise portal.PortalError("A valid request ID is required.")
    return request_id
