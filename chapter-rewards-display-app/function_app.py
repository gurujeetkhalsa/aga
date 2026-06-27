import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from time import perf_counter

import azure.functions as func

import chapter_rewards_support as rewards


app = func.FunctionApp()
SQL_CONNECTION_STRING = rewards.get_sql_connection_string()


def _utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _json_default(value):
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return rewards.json_safe_value(value)


def _json_response(payload: dict, status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload, default=_json_default),
        status_code=status_code,
        headers=rewards.response_headers("application/json; charset=utf-8"),
    )


def _with_debug(payload: dict, **debug_fields) -> dict:
    enriched = dict(payload)
    enriched["_debug"] = {key: value for key, value in debug_fields.items() if value is not None}
    return enriched


def _get_conn_str_or_error() -> tuple[str | None, func.HttpResponse | None]:
    if SQL_CONNECTION_STRING:
        return SQL_CONNECTION_STRING, None
    return None, func.HttpResponse(
        "Missing SQL connection string. Set SQL_CONNECTION_STRING or MYSQL_SYNC_SQL_CONNECTION_STRING.",
        status_code=500,
        headers=rewards.response_headers("text/plain; charset=utf-8"),
    )


def _parse_rewards_limit(req: func.HttpRequest, default: int = 150, maximum: int = 500) -> tuple[int | None, func.HttpResponse | None]:
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
    chapter_code = (req.params.get("chapter_code") or req.params.get("chapter") or "").strip()
    if not chapter_code:
        return None, func.HttpResponse("Query parameter 'chapter_code' is required.", status_code=400)
    if len(chapter_code) > 64:
        return None, func.HttpResponse("Query parameter 'chapter_code' is too long.", status_code=400)
    return chapter_code, None


@app.function_name(name="ChapterRewardsDisplayPage")
@app.route(route="chapter-rewards", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_display_page(req: func.HttpRequest) -> func.HttpResponse:
    markup = (Path(__file__).resolve().parent / "chapter_rewards.html").read_text(encoding="utf-8")
    return func.HttpResponse(
        markup,
        status_code=200,
        headers=rewards.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="ChapterRewardsBalances")
@app.route(route="chapter-rewards/balances", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_balances(req: func.HttpRequest) -> func.HttpResponse:
    started = perf_counter()
    conn_str, error = _get_conn_str_or_error()
    if error:
        return error
    try:
        rows = rewards.query_rows(conn_str, rewards.REWARDS_PUBLIC_BALANCES_SQL, [])
        chapters = [rewards._rewards_balance_payload(row) for row in rows]
        return _json_response(
            _with_debug(
                {
                    "ok": True,
                    "generated_at": _utc_now_text(),
                    "summary": rewards._rewards_summary_payload(chapters),
                    "chapters": chapters,
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return func.HttpResponse(f"Rewards balance report failed: {exc}", status_code=500)


@app.function_name(name="ChapterRewardsChapter")
@app.route(route="chapter-rewards/chapter", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def chapter_rewards_chapter(req: func.HttpRequest) -> func.HttpResponse:
    started = perf_counter()
    conn_str, error = _get_conn_str_or_error()
    if error:
        return error
    chapter_code, error = _parse_rewards_chapter_code(req)
    if error:
        return error
    limit, error = _parse_rewards_limit(req)
    if error:
        return error
    try:
        balance_rows = rewards.query_rows(conn_str, rewards.REWARDS_PUBLIC_CHAPTER_BALANCE_SQL, [chapter_code])
        if not balance_rows:
            return func.HttpResponse(f"No rewards balance found for chapter '{chapter_code}'.", status_code=404)
        transactions = [
            rewards._rewards_transaction_payload(row)
            for row in rewards.query_rows(conn_str, rewards.REWARDS_PUBLIC_TRANSACTIONS_SQL, [chapter_code, limit])
        ]
        lots = [
            rewards._rewards_lot_payload(row)
            for row in rewards.query_rows(conn_str, rewards.REWARDS_PUBLIC_LOTS_SQL, [chapter_code, limit])
        ]
        breakdown = [
            rewards._rewards_breakdown_payload(row)
            for row in rewards.query_rows(conn_str, rewards.REWARDS_PUBLIC_BREAKDOWN_SQL, [chapter_code])
        ]
        redemptions = [
            rewards._rewards_redemption_payload(row)
            for row in rewards.query_rows(conn_str, rewards.REWARDS_PUBLIC_REDEMPTIONS_SQL, [limit, chapter_code])
        ]
        return _json_response(
            _with_debug(
                {
                    "ok": True,
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
        return func.HttpResponse(f"Rewards chapter report failed: {exc}", status_code=500)
