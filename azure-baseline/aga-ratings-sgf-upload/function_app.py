from __future__ import annotations

import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import quote

import azure.functions as func

import auth
import importer
import storage


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
APP_ROOT = Path(__file__).resolve().parent
LOGGER = logging.getLogger("ratings_sgf_upload.function")


def _headers(content_type: str) -> dict[str, str]:
    return {
        "Cache-Control": "no-store",
        "Content-Type": content_type,
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "Referrer-Policy": "no-referrer",
    }


def _json(payload: dict[str, Any], status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload, default=str),
        status_code=status_code,
        headers=_headers("application/json; charset=utf-8"),
    )


def _authorize(req: func.HttpRequest) -> tuple[auth.Authorization, func.HttpResponse | None]:
    result = auth.authorize(req.headers)
    if result.ok:
        return result, None
    return result, _json({"ok": False, "error": result.error, "authorization": asdict(result)}, result.status_code)


def _body(req: func.HttpRequest) -> dict[str, Any]:
    try:
        value = req.get_json()
    except ValueError as exc:
        raise ValueError("Request body must be JSON.") from exc
    if not isinstance(value, dict):
        raise ValueError("Request body must be a JSON object.")
    return value


def _archive_request(body: dict[str, Any]) -> dict[str, Any]:
    encoded = str(body.get("archive_base64") or "").strip()
    if not encoded:
        raise ValueError("Archive ZIP data is required.")
    return {
        "archive_base64": encoded,
        "archive_name": str(body.get("archive_name") or "sgf-archive.zip").strip(),
        "replace_existing": body.get("replace_existing") is True,
    }


@app.function_name(name="SgfUploadHealth")
@app.route(route="sgf-upload/health", methods=["GET"])
def health(req: func.HttpRequest) -> func.HttpResponse:
    return _json({"ok": True, "service": "ratings-sgf-upload"})


@app.function_name(name="SgfUploadSqlHealth")
@app.route(route="sgf-upload/health/sql", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def sql_health(req: func.HttpRequest) -> func.HttpResponse:
    try:
        rows = storage.query_rows("SELECT TOP (1) [Permission_Code] FROM [ratings].[admin_permissions]")
        return _json({"ok": True, "query_completed": True, "returned_row": bool(rows)})
    except Exception as exc:
        LOGGER.exception("SGF uploader SQL self-test failed")
        return _json({"ok": False, "query_completed": False, "error_type": type(exc).__name__}, 500)


@app.function_name(name="SgfUploadPage")
@app.route(route="sgf-upload", methods=["GET"])
def page(req: func.HttpRequest) -> func.HttpResponse:
    authorization = auth.authorize(req.headers)
    if not authorization.ok:
        if authorization.status_code == 401:
            target = quote(req.url, safe="")
            headers = _headers("text/plain; charset=utf-8")
            headers["Location"] = f"/.auth/login/aad?post_login_redirect_uri={target}"
            return func.HttpResponse("", status_code=302, headers=headers)
        return func.HttpResponse(authorization.error or "Not authorized.", status_code=authorization.status_code, headers=_headers("text/plain; charset=utf-8"))
    return func.HttpResponse((APP_ROOT / "sgf_upload.html").read_text(encoding="utf-8"), headers=_headers("text/html; charset=utf-8"))


@app.function_name(name="SgfUploadMe")
@app.route(route="sgf-upload/me", methods=["GET"])
def me(req: func.HttpRequest) -> func.HttpResponse:
    authorization, error = _authorize(req)
    if error:
        return error
    return _json({"ok": True, "authorization": auth.public_payload(authorization)})


@app.function_name(name="SgfUploadPreview")
@app.route(route="sgf-upload/preview", methods=["POST"])
def preview(req: func.HttpRequest) -> func.HttpResponse:
    authorization, error = _authorize(req)
    if error:
        return error
    try:
        request = _archive_request(_body(req))
        payload = importer.preview(**request)
        payload["authorization"] = auth.public_payload(authorization)
        return _json(payload)
    except ValueError as exc:
        return _json({"ok": False, "error": str(exc)}, 400)
    except Exception:
        return _json({"ok": False, "error": "SGF archive preview failed."}, 500)


@app.function_name(name="SgfUploadApply")
@app.route(route="sgf-upload/apply", methods=["POST"])
def apply(req: func.HttpRequest) -> func.HttpResponse:
    authorization, error = _authorize(req)
    if error:
        return error
    try:
        body = _body(req)
        if body.get("confirm_apply") is not True:
            raise ValueError("confirm_apply=true is required before uploading SGFs.")
        plan_hash = str(body.get("plan_hash") or "").strip()
        if not plan_hash:
            raise ValueError("plan_hash is required. Run preview before applying.")
        payload = importer.apply(**_archive_request(body), expected_plan_hash=plan_hash)
        payload["authorization"] = auth.public_payload(authorization)
        return _json(payload)
    except ValueError as exc:
        return _json({"ok": False, "error": str(exc)}, 400)
    except Exception:
        return _json({"ok": False, "error": "SGF archive upload failed."}, 500)
