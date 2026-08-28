# Copyright 2026, American Go Association, All rights reserved

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from urllib.parse import quote, urlsplit

import azure.functions as func

import chapter_rewards_admin_support as rewards
from admin_auth import REWARDS_REDEMPTIONS_PERMISSION, authorize_admin_permission
from sql_adapter import SqlAdapter


app = func.FunctionApp()
SQL_CONNECTION_STRING = None


def _sql_connection_string() -> str | None:
    """Execute the sql connection string routine."""
    global SQL_CONNECTION_STRING
    if SQL_CONNECTION_STRING is None:
        from sql_adapter import get_sql_connection_string

        SQL_CONNECTION_STRING = get_sql_connection_string()
    return SQL_CONNECTION_STRING


def _utc_now_text() -> str:
    """Execute the utc now text routine."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _json_default(value):
    """Execute the json default routine."""
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return rewards.json_safe_value(value)


def _json_response(payload: dict, status_code: int = 200) -> func.HttpResponse:
    """Execute the json response routine."""
    return func.HttpResponse(
        json.dumps(payload, default=_json_default),
        status_code=status_code,
        headers=rewards.response_headers("application/json; charset=utf-8"),
    )


def _options_response() -> func.HttpResponse:
    """Execute the options response routine."""
    return func.HttpResponse("", status_code=204, headers=rewards.response_headers("application/json; charset=utf-8"))


def _with_debug(payload: dict, **debug_fields) -> dict:
    """Execute the with debug routine."""
    enriched = dict(payload)
    enriched["_debug"] = {key: value for key, value in debug_fields.items() if value is not None}
    return enriched


def _adapter_or_error() -> tuple[SqlAdapter | None, func.HttpResponse | None]:
    """Execute the adapter or error routine."""
    conn_str = _sql_connection_string()
    if not conn_str:
        return None, func.HttpResponse(
            "Missing SQL connection string. Set SQL_CONNECTION_STRING or MYSQL_SYNC_SQL_CONNECTION_STRING.",
            status_code=500,
            headers=rewards.response_headers("text/plain; charset=utf-8"),
        )
    return SqlAdapter(conn_str), None


def _login_redirect(req: func.HttpRequest) -> str:
    """Execute the login redirect routine."""
    raw_url = getattr(req, "url", "") or "/api/chapter-rewards/admin"
    parsed = urlsplit(raw_url)
    redirect_path = parsed.path or "/api/chapter-rewards/admin"
    if parsed.query:
        redirect_path = f"{redirect_path}?{parsed.query}"
    return f"/.auth/login/aad?post_login_redirect_uri={quote(redirect_path, safe='')}"


def _authorization_response(
    req: func.HttpRequest,
    adapter: SqlAdapter,
    *,
    html: bool = False,
) -> tuple[dict | None, func.HttpResponse | None]:
    """Execute the authorization response routine."""
    result = authorize_admin_permission(
        req.headers,
        adapter,
        required_permission=REWARDS_REDEMPTIONS_PERMISSION,
        feature_label="Chapter Rewards",
    )
    if result.ok:
        return {
            "principal_name": result.principal.principal_name if result.principal else None,
            "principal_id": result.principal.principal_id if result.principal else None,
            "identity_provider": result.principal.identity_provider if result.principal else None,
            "permission_code": result.admin_row.get("Permission_Code") if result.admin_row else None,
        }, None

    if html and result.status_code == 401:
        headers = rewards.response_headers("text/plain; charset=utf-8")
        headers["Location"] = _login_redirect(req)
        return None, func.HttpResponse("", status_code=302, headers=headers)

    message = result.error or "Rewards authorization failed."
    if html:
        return None, func.HttpResponse(message, status_code=result.status_code, headers=rewards.response_headers("text/plain; charset=utf-8"))
    return None, _json_response(
        {
            "ok": False,
            "error": message,
            "authorization": {
                "status_code": result.status_code,
                "principal_name": result.principal.principal_name if result.principal else None,
            },
        },
        status_code=result.status_code,
    )


def _request_json(req: func.HttpRequest) -> tuple[dict | None, func.HttpResponse | None]:
    """Execute the request json routine."""
    try:
        body = req.get_json()
    except ValueError:
        return None, rewards._rewards_manual_debit_error("Request body must be JSON.")
    if not isinstance(body, dict):
        return None, rewards._rewards_manual_debit_error("Request body must be a JSON object.")
    return body, None


def _parse_rewards_limit(req: func.HttpRequest, default: int = 150, maximum: int = 500) -> tuple[int | None, func.HttpResponse | None]:
    """Parse rewards limit."""
    raw_limit = (req.params.get("limit") or "").strip()
    if not raw_limit:
        return default, None
    if not raw_limit.isdigit():
        return None, func.HttpResponse("Query parameter 'limit' must be a positive integer.", status_code=400)
    limit = int(raw_limit)
    if limit < 1 or limit > maximum:
        return None, func.HttpResponse(f"Query parameter 'limit' must be between 1 and {maximum}.", status_code=400)
    return limit, None


def _parse_rewards_chapter_code(req: func.HttpRequest) -> tuple[str | None, func.HttpResponse | None]:
    """Parse rewards chapter code."""
    chapter_code = (req.params.get("chapter_code") or req.params.get("chapter") or "").strip()
    if not chapter_code:
        return None, func.HttpResponse("Query parameter 'chapter_code' is required.", status_code=400)
    if len(chapter_code) > 64:
        return None, func.HttpResponse("Query parameter 'chapter_code' is too long.", status_code=400)
    return chapter_code, None


def _bayrate_run_id_from_body(body: dict) -> tuple[int | None, func.HttpResponse | None]:
    """Parse and validate a Bayrate run identifier from an admin request."""
    try:
        run_id = int(str(body.get("bayrate_run_id") or "").strip())
    except ValueError:
        return None, rewards._rewards_manual_debit_error("bayrate_run_id must be a positive integer.")
    if run_id <= 0:
        return None, rewards._rewards_manual_debit_error("bayrate_run_id must be a positive integer.")
    return run_id, None


def _authorized_adapter(req: func.HttpRequest) -> tuple[SqlAdapter | None, dict | None, func.HttpResponse | None]:
    """Execute the authorized adapter routine."""
    adapter, error = _adapter_or_error()
    if error:
        return None, None, error
    authorization, auth_error = _authorization_response(req, adapter)
    if auth_error:
        return None, None, auth_error
    return adapter, authorization, None


@app.function_name(name="ChapterRewardsAdminPage")
@app.route(route="chapter-rewards/admin", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_admin_page(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the ChapterRewardsAdminPage Azure Function endpoint."""
    adapter, error = _adapter_or_error()
    if error:
        return error
    _, auth_error = _authorization_response(req, adapter, html=True)
    if auth_error:
        return auth_error
    markup = (Path(__file__).resolve().parent / "chapter_rewards_admin.html").read_text(encoding="utf-8")
    return func.HttpResponse(markup, status_code=200, headers=rewards.response_headers("text/html; charset=utf-8"))


@app.function_name(name="ChapterRewardsAdminMe")
@app.route(route="chapter-rewards/admin/me", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_admin_me(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the ChapterRewardsAdminMe Azure Function endpoint."""
    if req.method == "OPTIONS":
        return _options_response()
    _, authorization, error = _authorized_adapter(req)
    if error:
        return error
    return _json_response({"ok": True, "authorization": authorization})


@app.function_name(name="ChapterRewardsAdminBalances")
@app.route(route="chapter-rewards/balances", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_admin_balances(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the ChapterRewardsAdminBalances Azure Function endpoint."""
    if req.method == "OPTIONS":
        return _options_response()
    started = perf_counter()
    adapter, authorization, error = _authorized_adapter(req)
    if error:
        return error
    try:
        rows = adapter.query_rows(rewards.REWARDS_PUBLIC_BALANCES_SQL)
        chapters = [rewards._rewards_balance_payload(row) for row in rows]
        return _json_response(
            _with_debug(
                {
                    "ok": True,
                    "authorization": authorization,
                    "generated_at": _utc_now_text(),
                    "summary": rewards._rewards_summary_payload(chapters),
                    "chapters": chapters,
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return rewards._rewards_manual_debit_error(f"Rewards balance report failed: {exc}", status_code=500)


@app.function_name(name="ChapterRewardsAdminChapter")
@app.route(route="chapter-rewards/chapter", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_admin_chapter(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the ChapterRewardsAdminChapter Azure Function endpoint."""
    if req.method == "OPTIONS":
        return _options_response()
    started = perf_counter()
    adapter, authorization, error = _authorized_adapter(req)
    if error:
        return error
    chapter_code, error = _parse_rewards_chapter_code(req)
    if error:
        return error
    limit, error = _parse_rewards_limit(req)
    if error:
        return error
    try:
        balance_rows = adapter.query_rows(rewards.REWARDS_PUBLIC_CHAPTER_BALANCE_SQL, (chapter_code,))
        if not balance_rows:
            return func.HttpResponse(f"No rewards balance found for chapter '{chapter_code}'.", status_code=404)
        transactions = [
            rewards._rewards_transaction_payload(row)
            for row in adapter.query_rows(rewards.REWARDS_PUBLIC_TRANSACTIONS_SQL, (chapter_code, limit))
        ]
        lots = [
            rewards._rewards_lot_payload(row)
            for row in adapter.query_rows(rewards.REWARDS_PUBLIC_LOTS_SQL, (chapter_code, limit))
        ]
        breakdown = [
            rewards._rewards_breakdown_payload(row)
            for row in adapter.query_rows(rewards.REWARDS_PUBLIC_BREAKDOWN_SQL, (chapter_code,))
        ]
        redemptions = [
            rewards._rewards_redemption_payload(row)
            for row in adapter.query_rows(rewards.REWARDS_PUBLIC_REDEMPTIONS_SQL, (limit, chapter_code))
        ]
        return _json_response(
            _with_debug(
                {
                    "ok": True,
                    "authorization": authorization,
                    "generated_at": _utc_now_text(),
                    "chapter": rewards._rewards_balance_payload(balance_rows[0]),
                    "transactions": transactions,
                    "lots": lots,
                    "breakdown": breakdown,
                    "redemptions": redemptions,
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return rewards._rewards_manual_debit_error(f"Rewards chapter report failed: {exc}", status_code=500)


@app.function_name(name="ChapterRewardsAdminOptions")
@app.route(route="chapter-rewards/admin/options", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_admin_options(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the ChapterRewardsAdminOptions Azure Function endpoint."""
    if req.method == "OPTIONS":
        return _options_response()
    started = perf_counter()
    adapter, authorization, error = _authorized_adapter(req)
    if error:
        return error
    try:
        rows = adapter.query_rows(rewards.REWARDS_ADMIN_CHAPTER_OPTIONS_SQL)
        return _json_response(
            _with_debug(
                {
                    "ok": True,
                    "authorization": authorization,
                    "chapters": [rewards._rewards_admin_chapter_payload(row) for row in rows],
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return rewards._rewards_manual_debit_error(f"Rewards admin options failed: {exc}", status_code=500)


@app.function_name(name="ChapterRewardsAdminReconciliations")
@app.route(route="chapter-rewards/admin/reconciliations", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_admin_reconciliations(req: func.HttpRequest) -> func.HttpResponse:
    """List persisted Bayrate reward reconciliations and their application status."""
    if req.method == "OPTIONS":
        return _options_response()
    started = perf_counter()
    adapter, authorization, error = _authorized_adapter(req)
    if error:
        return error
    try:
        rows = adapter.query_rows(rewards.REWARDS_BAYRATE_RECONCILIATIONS_SQL)
        return _json_response(
            _with_debug(
                {
                    "ok": True,
                    "authorization": authorization,
                    "reconciliations": [
                        rewards._rewards_bayrate_reconciliation_summary_payload(row) for row in rows
                    ],
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return rewards._rewards_manual_debit_error(
            f"Bayrate reconciliation list failed: {exc}", status_code=500
        )


@app.function_name(name="ChapterRewardsAdminReconciliationPreview")
@app.route(route="chapter-rewards/admin/reconciliation-preview", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_admin_reconciliation_preview(req: func.HttpRequest) -> func.HttpResponse:
    """Preview the ledger changes for one persisted Bayrate reconciliation."""
    if req.method == "OPTIONS":
        return _options_response()
    started = perf_counter()
    adapter, authorization, error = _authorized_adapter(req)
    if error:
        return error
    body, body_error = _request_json(req)
    if body_error:
        return body_error
    run_id, run_error = _bayrate_run_id_from_body(body)
    if run_error:
        return run_error
    try:
        rows = adapter.query_rows(
            rewards.REWARDS_BAYRATE_RECONCILIATION_EXEC_SQL,
            (run_id, True, authorization.get("principal_name"), authorization.get("principal_id")),
        )
        if not rows:
            return rewards._rewards_manual_debit_error(
                "Bayrate reconciliation preview did not return a result.", status_code=500
            )
        return _json_response(
            _with_debug(
                {
                    "ok": True,
                    "authorization": authorization,
                    "reconciliation": rewards._rewards_bayrate_reconciliation_payload(rows[0]),
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return rewards._rewards_manual_debit_error(
            f"Bayrate reconciliation preview failed: {exc}", status_code=500
        )


@app.function_name(name="ChapterRewardsAdminReconciliationApply")
@app.route(route="chapter-rewards/admin/reconciliation-apply", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_admin_reconciliation_apply(req: func.HttpRequest) -> func.HttpResponse:
    """Apply one confirmed Bayrate reconciliation exactly once."""
    if req.method == "OPTIONS":
        return _options_response()
    started = perf_counter()
    adapter, authorization, error = _authorized_adapter(req)
    if error:
        return error
    body, body_error = _request_json(req)
    if body_error:
        return body_error
    if body.get("confirm_reconciliation") is not True:
        return rewards._rewards_manual_debit_error(
            "confirm_reconciliation=true is required before applying a Bayrate reconciliation."
        )
    run_id, run_error = _bayrate_run_id_from_body(body)
    if run_error:
        return run_error
    try:
        rows = adapter.query_rows(
            rewards.REWARDS_BAYRATE_RECONCILIATION_EXEC_SQL,
            (run_id, False, authorization.get("principal_name"), authorization.get("principal_id")),
        )
        if not rows:
            return rewards._rewards_manual_debit_error(
                "Bayrate reconciliation application did not return a result.", status_code=500
            )
        return _json_response(
            _with_debug(
                {
                    "ok": True,
                    "authorization": authorization,
                    "reconciliation": rewards._rewards_bayrate_reconciliation_payload(rows[0]),
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return rewards._rewards_manual_debit_error(
            f"Bayrate reconciliation application failed: {exc}", status_code=500
        )


@app.function_name(name="ChapterRewardsAdminDebitPreview")
@app.route(route="chapter-rewards/admin/debit-preview", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_admin_debit_preview(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the ChapterRewardsAdminDebitPreview Azure Function endpoint."""
    if req.method == "OPTIONS":
        return _options_response()
    started = perf_counter()
    adapter, authorization, error = _authorized_adapter(req)
    if error:
        return error
    body, body_error = _request_json(req)
    if body_error:
        return body_error
    request, request_error = rewards._rewards_manual_debit_request_from_body(body, authorization, generate_request_id=False)
    if request_error:
        return request_error
    try:
        rows = adapter.query_rows(rewards.REWARDS_MANUAL_DEBIT_SQL, rewards._rewards_manual_debit_params(request, dry_run=True))
        if not rows:
            return rewards._rewards_manual_debit_error("Manual debit preview did not return a result.", status_code=500)
        preview = rewards._rewards_manual_debit_payload(rows[0])
        return _json_response(
            _with_debug(
                {"ok": True, "authorization": authorization, "preview": preview},
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return rewards._rewards_manual_debit_error(f"Manual debit preview failed: {exc}", status_code=500)


@app.function_name(name="ChapterRewardsAdminDebitPost")
@app.route(route="chapter-rewards/admin/debit", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_admin_debit_post(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the ChapterRewardsAdminDebitPost Azure Function endpoint."""
    if req.method == "OPTIONS":
        return _options_response()
    started = perf_counter()
    adapter, authorization, error = _authorized_adapter(req)
    if error:
        return error
    body, body_error = _request_json(req)
    if body_error:
        return body_error
    if body.get("confirm_debit") is not True:
        return rewards._rewards_manual_debit_error("confirm_debit=true is required before posting a manual debit.")
    request, request_error = rewards._rewards_manual_debit_request_from_body(body, authorization, generate_request_id=True)
    if request_error:
        return request_error
    try:
        adapter.execute_statements([(rewards.REWARDS_MANUAL_DEBIT_SQL, rewards._rewards_manual_debit_params(request, dry_run=False))])
        rows = adapter.query_rows(rewards.REWARDS_MANUAL_DEBIT_LOOKUP_SQL, (request["external_request_id"],))
        if not rows:
            return rewards._rewards_manual_debit_error("Manual debit posted but could not be reloaded.", status_code=500)
        debit = rewards._rewards_manual_debit_payload(rows[0])
        return _json_response(
            _with_debug(
                {"ok": True, "authorization": authorization, "debit": debit},
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return rewards._rewards_manual_debit_error(f"Manual debit post failed: {exc}", status_code=500)


@app.function_name(name="ChapterRewardsAdminRedemption")
@app.route(route="chapter-rewards/admin/redemption", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_admin_redemption(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the ChapterRewardsAdminRedemption Azure Function endpoint."""
    if req.method == "OPTIONS":
        return _options_response()
    adapter, authorization, error = _authorized_adapter(req)
    if error:
        return error
    try:
        redemption_id = int(str(req.params.get("redemption_id") or "").strip())
    except ValueError:
        return rewards._rewards_manual_debit_error("redemption_id must be a positive integer.")
    if redemption_id <= 0:
        return rewards._rewards_manual_debit_error("redemption_id must be a positive integer.")
    try:
        rows = adapter.query_rows(rewards.REWARDS_ADMIN_REDEMPTION_SQL, (redemption_id,))
        if not rows:
            return rewards._rewards_manual_debit_error("Redemption was not found.", status_code=404)
        receipt_rows = adapter.query_rows(rewards.REWARDS_RECEIPTS_SQL, (redemption_id,))
        receipts = [rewards._rewards_receipt_payload(row) for row in receipt_rows]
        return _json_response(
            {
                "ok": True,
                "authorization": authorization,
                "redemption": rewards._rewards_admin_redemption_payload(rows[0], receipts),
            }
        )
    except Exception as exc:
        return rewards._rewards_manual_debit_error(f"Redemption detail failed: {exc}", status_code=500)


@app.function_name(name="ChapterRewardsAdminRedemptionNotes")
@app.route(route="chapter-rewards/admin/redemption-notes", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_admin_redemption_notes(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the ChapterRewardsAdminRedemptionNotes Azure Function endpoint."""
    if req.method == "OPTIONS":
        return _options_response()
    adapter, authorization, error = _authorized_adapter(req)
    if error:
        return error
    body, body_error = _request_json(req)
    if body_error:
        return body_error
    redemption_id, id_error = rewards._positive_body_int(body, "redemption_id")
    if id_error:
        return id_error
    notes = str(body.get("notes") or "").strip()
    if len(notes) > 4000:
        return rewards._rewards_manual_debit_error("notes must be 4000 characters or fewer.")
    try:
        adapter.execute_statements(
            [
                (
                    rewards.REWARDS_UPDATE_NOTES_SQL,
                    (redemption_id, notes or None, authorization.get("principal_name"), authorization.get("principal_id")),
                )
            ]
        )
        rows = adapter.query_rows(rewards.REWARDS_ADMIN_REDEMPTION_SQL, (redemption_id,))
        receipt_rows = adapter.query_rows(rewards.REWARDS_RECEIPTS_SQL, (redemption_id,))
        return _json_response(
            {
                "ok": True,
                "authorization": authorization,
                "redemption": rewards._rewards_admin_redemption_payload(
                    rows[0],
                    [rewards._rewards_receipt_payload(row) for row in receipt_rows],
                ),
            }
        )
    except Exception as exc:
        return rewards._rewards_manual_debit_error(f"Notes update failed: {exc}", status_code=500)


@app.function_name(name="ChapterRewardsAdminReceiptUpload")
@app.route(route="chapter-rewards/admin/receipt-upload", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_admin_receipt_upload(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the ChapterRewardsAdminReceiptUpload Azure Function endpoint."""
    if req.method == "OPTIONS":
        return _options_response()
    adapter, authorization, error = _authorized_adapter(req)
    if error:
        return error
    body, body_error = _request_json(req)
    if body_error:
        return body_error
    redemption_id, id_error = rewards._positive_body_int(body, "redemption_id")
    if id_error:
        return id_error
    files = body.get("files")
    if not isinstance(files, list) or not files:
        return rewards._rewards_manual_debit_error("At least one receipt file is required.")
    if len(files) > rewards.REWARDS_MAX_RECEIPT_FILES:
        return rewards._rewards_manual_debit_error(f"Upload at most {rewards.REWARDS_MAX_RECEIPT_FILES} receipt files at a time.")

    try:
        container = rewards._rewards_receipt_container_client()
        for item in files:
            if not isinstance(item, dict):
                return rewards._rewards_manual_debit_error("Each receipt file must be an object.")
            filename, content_type, content = rewards._decode_receipt_upload(item)
            blob_name = rewards._rewards_receipt_blob_name(redemption_id, filename)
            blob = container.get_blob_client(blob_name)
            blob.upload_blob(content, overwrite=False)
            try:
                adapter.execute_statements(
                    [
                        (
                            rewards.REWARDS_ADD_RECEIPT_SQL,
                            (
                                redemption_id,
                                rewards.REWARDS_RECEIPT_CONTAINER,
                                blob_name,
                                filename,
                                content_type,
                                len(content),
                                hashlib.sha256(content).hexdigest(),
                                authorization.get("principal_name"),
                                authorization.get("principal_id"),
                            ),
                        )
                    ]
                )
            except Exception:
                try:
                    blob.delete_blob(delete_snapshots="include")
                except Exception:
                    pass
                raise
        receipt_rows = adapter.query_rows(rewards.REWARDS_RECEIPTS_SQL, (redemption_id,))
        receipt_payloads = [rewards._rewards_receipt_payload(row) for row in receipt_rows]
        return _json_response(
            {
                "ok": True,
                "authorization": authorization,
                "uploaded": receipt_payloads,
                "receipts": receipt_payloads,
            }
        )
    except ValueError as exc:
        return rewards._rewards_manual_debit_error(str(exc))
    except Exception as exc:
        return rewards._rewards_manual_debit_error(f"Receipt upload failed: {exc}", status_code=500)


@app.function_name(name="ChapterRewardsAdminReceiptDelete")
@app.route(route="chapter-rewards/admin/receipt-delete", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_admin_receipt_delete(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the ChapterRewardsAdminReceiptDelete Azure Function endpoint."""
    if req.method == "OPTIONS":
        return _options_response()
    adapter, authorization, error = _authorized_adapter(req)
    if error:
        return error
    body, body_error = _request_json(req)
    if body_error:
        return body_error
    receipt_id, id_error = rewards._positive_body_int(body, "receipt_id")
    if id_error:
        return id_error
    try:
        adapter.execute_statements(
            [
                (
                    rewards.REWARDS_DELETE_RECEIPT_SQL,
                    (receipt_id, authorization.get("principal_name"), authorization.get("principal_id")),
                )
            ]
        )
        return _json_response(
            {
                "ok": True,
                "authorization": authorization,
                "receipt": {"receipt_id": receipt_id, "is_deleted": True},
            }
        )
    except Exception as exc:
        return rewards._rewards_manual_debit_error(f"Receipt removal failed: {exc}", status_code=500)


@app.function_name(name="ChapterRewardsAdminReceiptFile")
@app.route(route="chapter-rewards/admin/receipts/{receipt_id}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_admin_receipt_file(req: func.HttpRequest) -> func.HttpResponse:
    """Handle the ChapterRewardsAdminReceiptFile Azure Function endpoint."""
    adapter, _, error = _authorized_adapter(req)
    if error:
        return error
    try:
        receipt_id = int(str(req.route_params.get("receipt_id") or "").strip())
    except ValueError:
        return func.HttpResponse("receipt_id must be a positive integer.", status_code=400)
    try:
        rows = adapter.query_rows(rewards.REWARDS_RECEIPT_LOOKUP_SQL, (receipt_id,))
        if not rows:
            return func.HttpResponse("Receipt was not found.", status_code=404)
        receipt = rows[0]
        container = rewards._rewards_receipt_container_client()
        blob = container.get_blob_client(receipt["Blob_Name"])
        content = blob.download_blob().readall()
        content_type = rewards._rewards_text(receipt.get("Content_Type")) or "application/octet-stream"
        filename = rewards._safe_rewards_blob_component(rewards._rewards_text(receipt.get("Original_File_Name")) or f"receipt-{receipt_id}")
        headers = rewards.response_headers(content_type)
        headers["Content-Disposition"] = f'inline; filename="{filename}"'
        return func.HttpResponse(content, status_code=200, headers=headers)
    except Exception as exc:
        return func.HttpResponse(f"Receipt download failed: {exc}", status_code=500)
