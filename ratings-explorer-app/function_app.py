import json
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from urllib.parse import quote, urlsplit

import azure.functions as func

import ratings_explorer_support as explorer

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

try:
    from bayrate.auth import authorize_bayrate_admin
    from bayrate.commit_staged_run import build_commit_plan, commit_staged_run, printable_commit_plan
    from bayrate.sql_adapter import SqlAdapter
    from bayrate.replay_staged_run import run_staged_replay
    from bayrate.stage_reports import (
        apply_tournament_review_decision,
        build_insert_statements,
        build_staging_payload,
        ensure_payload_run_id,
        explain_staged_run_review,
        load_host_chapter_options,
        load_staged_run,
        printable_payload,
        update_staged_run_review,
    )
except Exception:
    authorize_bayrate_admin = None
    build_commit_plan = None
    commit_staged_run = None
    printable_commit_plan = None
    SqlAdapter = None
    run_staged_replay = None
    apply_tournament_review_decision = None
    build_insert_statements = None
    build_staging_payload = None
    ensure_payload_run_id = None
    explain_staged_run_review = None
    load_host_chapter_options = None
    load_staged_run = None
    printable_payload = None
    update_staged_run_review = None

app = func.FunctionApp()
ALLOWED_SEARCH_LIMITS = {10, 25, 50, 100, 250}
DEFAULT_SEARCH_LIMIT = 25
ALLOWED_ACTIVITY_YEARS = {1, 3, 5, 10}
DEFAULT_RECENT_ACTIVITY_YEARS = 3
ALLOWED_RATING_BANDS = {item["value"] for item in explorer.RATING_FILTER_OPTIONS}
ALLOWED_PLAYER_STATUS_FILTERS = {"all", "active", "expired"}
SQL_CONNECTION_STRING = explorer.get_sql_connection_string()
if not SQL_CONNECTION_STRING:
    raise RuntimeError(
        "Missing SQL connection string. Set SQL_CONNECTION_STRING or MYSQL_SYNC_SQL_CONNECTION_STRING in Function App settings or local.settings.json."
    )


def _json_response(payload: dict) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload),
        status_code=200,
        headers=explorer.response_headers("application/json; charset=utf-8"),
    )


def _with_debug(payload: dict, **debug_fields) -> dict:
    enriched = dict(payload)
    enriched["_debug"] = {key: value for key, value in debug_fields.items() if value is not None}
    return enriched


def _public_json_default(value):
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return explorer.json_safe_value(value)


def _public_json_response(payload: dict, status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload, default=_public_json_default),
        status_code=status_code,
        headers=explorer.response_headers("application/json; charset=utf-8"),
    )


def _utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _bayrate_json_response(payload: dict, status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(payload, default=explorer.json_safe_value),
        status_code=status_code,
        headers=explorer.response_headers("application/json; charset=utf-8"),
    )


def _bayrate_preview_error(message: str, status_code: int = 400) -> func.HttpResponse:
    return _bayrate_json_response({"ok": False, "error": message}, status_code=status_code)


def _bayrate_modules_available(*names: str) -> bool:
    modules = {
        "stage": build_staging_payload is not None and printable_payload is not None,
        "load": load_staged_run is not None and printable_payload is not None,
        "write": build_insert_statements is not None and ensure_payload_run_id is not None,
        "review": (
            load_staged_run is not None
            and apply_tournament_review_decision is not None
            and update_staged_run_review is not None
        ),
        "replay": run_staged_replay is not None,
        "commit": build_commit_plan is not None and commit_staged_run is not None and printable_commit_plan is not None,
    }
    return all(modules.get(name, False) for name in names)


def _bayrate_adapter_or_error() -> tuple[object | None, func.HttpResponse | None]:
    if SqlAdapter is None:
        return None, _bayrate_preview_error("BayRate SQL adapter is not available in this deployment.", status_code=500)
    return SqlAdapter(SQL_CONNECTION_STRING), None


def _bayrate_login_redirect(req: func.HttpRequest) -> str:
    raw_url = getattr(req, "url", "") or "/api/ratings-explorer/bayrate"
    parsed = urlsplit(raw_url)
    redirect_path = parsed.path or "/api/ratings-explorer/bayrate"
    if parsed.query:
        redirect_path = f"{redirect_path}?{parsed.query}"
    return f"/.auth/login/aad?post_login_redirect_uri={quote(redirect_path, safe='')}"


def _bayrate_authorization_response(
    req: func.HttpRequest,
    adapter: object,
    *,
    html: bool = False,
) -> tuple[dict | None, func.HttpResponse | None]:
    if authorize_bayrate_admin is None:
        message = "BayRate authorization modules are not available in this deployment."
        if html:
            return None, func.HttpResponse(message, status_code=500, headers=explorer.response_headers("text/plain; charset=utf-8"))
        return None, _bayrate_preview_error(message, status_code=500)

    result = authorize_bayrate_admin(req.headers, adapter)
    if result.ok:
        return {
            "principal_name": result.principal.principal_name if result.principal else None,
            "principal_id": result.principal.principal_id if result.principal else None,
            "identity_provider": result.principal.identity_provider if result.principal else None,
        }, None

    if html and result.status_code == 401:
        headers = explorer.response_headers("text/plain; charset=utf-8")
        headers["Location"] = _bayrate_login_redirect(req)
        return None, func.HttpResponse("", status_code=302, headers=headers)

    message = result.error or "BayRate authorization failed."
    if html:
        return None, func.HttpResponse(message, status_code=result.status_code, headers=explorer.response_headers("text/plain; charset=utf-8"))
    return None, _bayrate_json_response(
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


def _bayrate_request_json(req: func.HttpRequest) -> tuple[dict | None, func.HttpResponse | None]:
    try:
        body = req.get_json()
    except ValueError:
        return None, _bayrate_preview_error("Request body must be JSON.")
    if not isinstance(body, dict):
        return None, _bayrate_preview_error("Request body must be a JSON object.")
    return body, None


def _bayrate_report_inputs_from_body(
    body: dict,
    adapter: object | None = None,
) -> tuple[list[tuple[str, str]] | None, list[dict] | None, func.HttpResponse | None]:
    reports = body.get("reports")
    if not isinstance(reports, list) or not reports:
        return None, None, _bayrate_preview_error("At least one report is required.")
    if len(reports) > 20:
        return None, None, _bayrate_preview_error("At most 20 reports can be previewed at once.")

    report_inputs = []
    report_metadata = []
    host_options_by_id = None
    for index, item in enumerate(reports, start=1):
        if not isinstance(item, dict):
            return None, None, _bayrate_preview_error(f"Report {index} must be an object.")
        source_name = str(item.get("source_name") or f"pasted-report-{index}.txt").strip()
        content = str(item.get("content") or "")
        if not content.strip():
            return None, None, _bayrate_preview_error(f"Report {index} is empty.")
        if len(content) > 500_000:
            return None, None, _bayrate_preview_error(f"Report {index} is too large.")
        metadata, metadata_error, host_options_by_id = _bayrate_report_metadata_from_item(
            item,
            adapter=adapter,
            host_options_by_id=host_options_by_id,
        )
        if metadata_error:
            return None, None, _bayrate_preview_error(f"Report {index}: {metadata_error}")
        report_inputs.append((source_name, content))
        report_metadata.append(metadata)
    return report_inputs, report_metadata, None


def _bayrate_report_metadata_from_item(
    item: dict,
    *,
    adapter: object | None,
    host_options_by_id: dict | None,
) -> tuple[dict, str | None, dict | None]:
    raw_metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    metadata = {}
    text_fields = {
        "tournament_descr": 255,
        "city": 100,
        "state_code": 20,
        "country_code": 20,
        "reward_event_key": 128,
        "reward_event_name": 255,
    }
    for key, max_length in text_fields.items():
        if key not in raw_metadata:
            continue
        value = str(raw_metadata.get(key) or "").strip()
        if value and len(value) > max_length:
            return {}, f"{key} must be {max_length} characters or fewer.", host_options_by_id
        metadata[key] = value or None

    if "reward_is_state_championship" in raw_metadata:
        metadata["reward_is_state_championship"] = bool(raw_metadata.get("reward_is_state_championship"))

    raw_host_id = raw_metadata.get("host_chapter_id")
    if raw_host_id in (None, ""):
        return metadata, None, host_options_by_id
    try:
        host_chapter_id = int(str(raw_host_id).strip())
    except (TypeError, ValueError):
        return {}, "host_chapter_id must be an integer.", host_options_by_id
    if host_chapter_id <= 0:
        return {}, "host_chapter_id must be a positive integer.", host_options_by_id
    if adapter is not None and load_host_chapter_options is not None:
        if host_options_by_id is None:
            host_options_by_id = {
                int(option.get("chapter_id")): option
                for option in load_host_chapter_options(adapter)
                if option.get("chapter_id") is not None
            }
        option = host_options_by_id.get(host_chapter_id)
        if option is None:
            return {}, f"Host chapter {host_chapter_id} was not found.", host_options_by_id
        metadata["host_chapter_id"] = host_chapter_id
        metadata["host_chapter_code"] = option.get("chapter_code")
        metadata["host_chapter_name"] = option.get("chapter_name")
    else:
        metadata["host_chapter_id"] = host_chapter_id
        metadata["host_chapter_code"] = str(raw_metadata.get("host_chapter_code") or "").strip() or None
        metadata["host_chapter_name"] = str(raw_metadata.get("host_chapter_name") or "").strip() or None
    return metadata, None, host_options_by_id


def _bayrate_payload_response(payload: dict, *, adapter: object | None = None, written: bool = False) -> dict:
    explanations = explain_staged_run_review(adapter, payload) if (adapter and explain_staged_run_review is not None) else None
    summary = printable_payload(payload, include_games=False)
    summary["written"] = written
    response = {
        "ok": True,
        "written": written,
        "summary": summary,
        "same_date_groups": _bayrate_same_date_groups(payload),
        "review_explanation": explanations,
    }
    if adapter and load_host_chapter_options is not None:
        response["host_chapter_options"] = load_host_chapter_options(adapter)
    if adapter and written and summary.get("run_id") is not None:
        response["commit_state"] = _bayrate_commit_state(adapter, summary.get("run_id"))
    return response


def _bayrate_host_chapter_from_body(adapter: object, body: dict) -> tuple[dict | None, func.HttpResponse | None]:
    if "host_chapter_id" not in body and "hostChapterId" not in body:
        return None, None
    raw_id = body.get("host_chapter_id", body.get("hostChapterId"))
    try:
        host_chapter_id = int(str(raw_id).strip())
    except (TypeError, ValueError):
        return None, _bayrate_preview_error("host_chapter_id must be an integer.")
    if host_chapter_id <= 0:
        return None, _bayrate_preview_error("host_chapter_id must be a positive integer.")
    if load_host_chapter_options is None:
        return None, _bayrate_preview_error("BayRate host chapter lookup is not available.", status_code=500)
    for option in load_host_chapter_options(adapter):
        if int(option.get("chapter_id") or 0) == host_chapter_id:
            return option, None
    return None, _bayrate_preview_error(f"Host chapter {host_chapter_id} was not found.", status_code=404)


def _bayrate_optional_bool_from_body(body: dict, *names: str) -> bool | None:
    for name in names:
        if name not in body:
            continue
        value = body.get(name)
        if isinstance(value, bool):
            return value
        text = str(value or "").strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off", ""}:
            return False
        return bool(value)
    return None


def _bayrate_commit_state(adapter: object, run_id: int | str) -> dict:
    rows = adapter.query_rows(
        """
SELECT
    (SELECT COUNT(*) FROM [ratings].[bayrate_staged_ratings] WHERE [RunID] = ?) AS [StagedRatingCount],
    (SELECT COUNT(*) FROM [ratings].[bayrate_staged_ratings] WHERE [RunID] = ? AND [Planned_Rating_Row_ID] IS NOT NULL) AS [PlannedRatingCount],
    (SELECT COUNT(*) FROM [ratings].[bayrate_staged_games] WHERE [RunID] = ?) AS [StagedGameCount],
    (SELECT COUNT(*) FROM [ratings].[bayrate_staged_games] WHERE [RunID] = ? AND [Game_ID] IS NOT NULL) AS [PlannedGameCount]
""",
        (run_id, run_id, run_id, run_id),
    )
    row = rows[0] if rows else {}
    staged_rating_count = int(row.get("StagedRatingCount") or 0)
    planned_rating_count = int(row.get("PlannedRatingCount") or 0)
    staged_game_count = int(row.get("StagedGameCount") or 0)
    planned_game_count = int(row.get("PlannedGameCount") or 0)
    partial_marker = (
        0 < planned_rating_count < staged_rating_count
        or 0 < planned_game_count < staged_game_count
        or (planned_rating_count > 0 and staged_rating_count == 0)
        or (planned_game_count > 0 and staged_game_count == 0)
    )
    committed = (
        staged_rating_count > 0
        and planned_rating_count == staged_rating_count
        and planned_game_count == staged_game_count
        and not partial_marker
    )
    if partial_marker:
        state = "partial_commit_marker"
    elif committed:
        state = "committed"
    elif staged_rating_count:
        state = "replayed_uncommitted"
    else:
        state = "needs_replay"
    return {
        "state": state,
        "committed": committed,
        "has_staged_ratings": staged_rating_count > 0,
        "staged_rating_count": staged_rating_count,
        "planned_rating_count": planned_rating_count,
        "staged_game_count": staged_game_count,
        "planned_game_count": planned_game_count,
    }


def _bayrate_replay_response(artifact: dict) -> dict:
    plan = artifact.get("plan") or {}
    result = artifact.get("bayrate_result") or {}
    staged_rating_summary = artifact.get("staged_rating_summary") or {}
    return {
        "ok": True,
        "read_only": True,
        "run_id": plan.get("run_id"),
        "output_path": artifact.get("output_path"),
        "plan": plan,
        "staged_rating_summary": staged_rating_summary,
        "result_summary": {
            "event_count": result.get("event_count"),
            "player_count": result.get("player_count"),
            "pre_event_metrics": result.get("pre_event_metrics"),
            "post_event_fit_metrics": result.get("post_event_fit_metrics"),
            "rating_result_count": staged_rating_summary.get("rating_count", 0),
            "staged_rating_count": plan.get("staged_rating_count", 0),
        },
    }


def _bayrate_same_date_groups(payload: dict) -> list[dict]:
    groups: dict[str, list[dict]] = {}
    for entry in payload.get("staged_tournaments") or []:
        row = entry.get("tournament_row") or {}
        tournament_date = row.get("Tournament_Date")
        key = tournament_date.isoformat() if hasattr(tournament_date, "isoformat") else str(tournament_date or "")
        groups.setdefault(key, []).append(
            {
                "source_report_ordinal": entry.get("source_report_ordinal"),
                "source_report_name": entry.get("source_report_name"),
                "tournament_code": row.get("Tournament_Code"),
                "title": row.get("Tournament_Descr"),
                "tournament_date": tournament_date,
                "status": entry.get("status"),
                "duplicate_candidate": entry.get("duplicate_candidate"),
                "game_count": sum(
                    1
                    for game in payload.get("staged_games") or []
                    if game.get("source_report_ordinal") == entry.get("source_report_ordinal")
                ),
            }
        )
    return [
        {
            "tournament_date": tournament_date,
            "events": events,
        }
        for tournament_date, events in sorted(groups.items())
        if tournament_date and len(events) > 1
    ]


REWARDS_PUBLIC_BALANCES_SQL = """
SELECT
    [Chapter_Code],
    [Chapter_Name],
    [As_Of_Date],
    [Latest_Snapshot_Date],
    [Is_Current],
    [Active_Member_Count],
    [Multiplier],
    [Available_Points],
    [Total_Remaining_Points],
    [Expired_Unallocated_Points],
    [Original_Points],
    [Consumed_Points],
    [Ledger_Balance],
    [Balance_Reconciliation_Delta],
    [Total_Credits],
    [Total_Debits],
    [Lot_Count],
    [Transaction_Count],
    [Expiring_30_Days],
    [Expiring_60_Days],
    [Expiring_90_Days],
    [Next_Expiration_Date],
    [Last_Transaction_Posted_At]
FROM [rewards].[v_chapter_balances]
WHERE COALESCE([Available_Points], 0) <> 0
   OR COALESCE([Ledger_Balance], 0) <> 0
   OR COALESCE([Total_Credits], 0) <> 0
   OR COALESCE([Total_Debits], 0) <> 0
ORDER BY [Available_Points] DESC, [Chapter_Code]
"""


REWARDS_PUBLIC_CHAPTER_BALANCE_SQL = """
SELECT TOP 1
    [Chapter_Code],
    [Chapter_Name],
    [As_Of_Date],
    [Latest_Snapshot_Date],
    [Is_Current],
    [Active_Member_Count],
    [Multiplier],
    [Available_Points],
    [Total_Remaining_Points],
    [Expired_Unallocated_Points],
    [Original_Points],
    [Consumed_Points],
    [Ledger_Balance],
    [Balance_Reconciliation_Delta],
    [Total_Credits],
    [Total_Debits],
    [Lot_Count],
    [Transaction_Count],
    [Expiring_30_Days],
    [Expiring_60_Days],
    [Expiring_90_Days],
    [Next_Expiration_Date],
    [Last_Transaction_Posted_At]
FROM [rewards].[v_chapter_balances]
WHERE UPPER([Chapter_Code]) = UPPER(?)
"""


REWARDS_PUBLIC_TRANSACTIONS_SQL = """
WITH [chapter_tx] AS
(
    SELECT
        [TransactionID],
        [Transaction_Type],
        [Points_Delta],
        [Base_Points],
        [Multiplier],
        [Effective_Date],
        [Earned_Date],
        [Posted_At],
        [RunID],
        [Run_Type],
        [Run_Snapshot_Date],
        [Run_Status],
        [Run_Processor],
        [Source_Type],
        [Rule_Version],
        [LotID],
        [Lot_Remaining_Points],
        [Lot_Expires_On],
        [Allocated_From_Lots],
        [Allocated_Lot_Count],
        [MetadataJson]
    FROM [rewards].[v_chapter_transaction_history]
    WHERE UPPER([Chapter_Code]) = UPPER(?)
),
[public_tx] AS
(
    SELECT
        tx.*,
        CASE
            WHEN tx.[Source_Type] IN (N'membership_event', N'legacy_membership_gap_award') THEN N'membership'
            WHEN tx.[Source_Type] = N'rated_game_participation' THEN N'rated_games'
            WHEN tx.[Source_Type] = N'tournament_host' THEN N'tournament_host'
            WHEN tx.[Source_Type] = N'state_championship' THEN N'state_championship'
            WHEN tx.[Source_Type] = N'redemption' THEN N'redemption'
            WHEN tx.[Source_Type] = N'opening_balance' THEN N'opening_balance'
            WHEN tx.[Source_Type] = N'point_expiration' THEN N'expiration'
            WHEN tx.[Source_Type] = N'legacy_dues_credit_adjustment' THEN N'adjustment'
            ELSE N'other'
        END AS [Source_Category],
        CASE
            WHEN tx.[Source_Type] IN (N'membership_event', N'legacy_membership_gap_award') THEN N'Membership awards'
            WHEN tx.[Source_Type] = N'rated_game_participation' THEN N'Rated games'
            WHEN tx.[Source_Type] = N'tournament_host' THEN N'Tournament host'
            WHEN tx.[Source_Type] = N'state_championship' THEN N'State Championship'
            WHEN tx.[Source_Type] = N'redemption' THEN N'Redemption'
            WHEN tx.[Source_Type] = N'opening_balance' THEN N'Opening balance'
            WHEN tx.[Source_Type] = N'point_expiration' THEN N'Expiration'
            WHEN tx.[Source_Type] = N'legacy_dues_credit_adjustment' THEN N'Dues credit adjustment'
            ELSE N'Other'
        END AS [Source_Label],
        CASE
        WHEN tx.[Source_Type] IN (N'membership_event', N'legacy_membership_gap_award') THEN
            CONCAT(
                CASE JSON_VALUE(tx.[MetadataJson], '$.event_type')
                    WHEN N'new_membership' THEN N'New membership'
                    WHEN N'renewal' THEN N'Renewal'
                    WHEN N'lifetime' THEN N'Lifetime membership'
                    ELSE N'Membership award'
                END,
                CASE
                    WHEN NULLIF(JSON_VALUE(tx.[MetadataJson], '$.member_count'), N'') IS NOT NULL
                        THEN CONCAT(N' x ', JSON_VALUE(tx.[MetadataJson], '$.member_count'))
                    ELSE N''
                END,
                CASE
                    WHEN COALESCE(NULLIF(JSON_VALUE(tx.[MetadataJson], '$.member_type'), N''), NULLIF(JSON_VALUE(tx.[MetadataJson], '$.event_member_type'), N'')) IS NOT NULL
                        THEN CONCAT(N' - ', COALESCE(JSON_VALUE(tx.[MetadataJson], '$.member_type'), JSON_VALUE(tx.[MetadataJson], '$.event_member_type')))
                    ELSE N''
                END
            )
        WHEN tx.[Source_Type] = N'rated_game_participation' THEN
            CONCAT(
                N'Rated game participation',
                CASE
                    WHEN NULLIF(JSON_VALUE(tx.[MetadataJson], '$.tournament_code'), N'') IS NOT NULL
                        THEN CONCAT(N' - ', JSON_VALUE(tx.[MetadataJson], '$.tournament_code'))
                    ELSE N''
                END
            )
        WHEN tx.[Source_Type] = N'tournament_host' THEN
            CONCAT(
                COALESCE(NULLIF(JSON_VALUE(tx.[MetadataJson], '$.reward_event_name'), N''), N'Tournament host award'),
                CASE
                    WHEN NULLIF(JSON_VALUE(tx.[MetadataJson], '$.rated_game_count'), N'') IS NOT NULL
                        THEN CONCAT(N' - ', JSON_VALUE(tx.[MetadataJson], '$.rated_game_count'), N' rated games')
                    ELSE N''
                END
            )
        WHEN tx.[Source_Type] = N'state_championship' THEN
            CONCAT(COALESCE(NULLIF(JSON_VALUE(tx.[MetadataJson], '$.reward_event_name'), N''), N'State Championship'), N' - State Championship award')
        WHEN tx.[Source_Type] = N'redemption' THEN
            CONCAT(
                N'Redemption - ',
                REPLACE(COALESCE(NULLIF(JSON_VALUE(tx.[MetadataJson], '$.redemption_category'), N''), N'other'), N'_', N' '),
                N', ',
                REPLACE(COALESCE(NULLIF(JSON_VALUE(tx.[MetadataJson], '$.payment_mode'), N''), N'other'), N'_', N' ')
            )
        WHEN tx.[Source_Type] = N'opening_balance' THEN N'Opening balance'
        WHEN tx.[Source_Type] = N'point_expiration' THEN N'Expired unused points'
        WHEN tx.[Source_Type] = N'legacy_dues_credit_adjustment' THEN N'Dues credit adjustment'
        ELSE REPLACE(tx.[Source_Type], N'_', N' ')
        END AS [Public_Detail]
    FROM [chapter_tx] AS tx
),
[public_entries] AS
(
    SELECT
        COUNT(*) AS [Public_Entry_Count],
        MAX([TransactionID]) AS [Sort_TransactionID],
        [Transaction_Type],
        SUM([Points_Delta]) AS [Points_Delta],
        CASE WHEN MIN([Base_Points]) = MAX([Base_Points]) THEN MAX([Base_Points]) ELSE NULL END AS [Base_Points],
        CASE WHEN MIN([Multiplier]) = MAX([Multiplier]) THEN MAX([Multiplier]) ELSE NULL END AS [Multiplier],
        [Effective_Date],
        [Earned_Date],
        MAX([Posted_At]) AS [Posted_At],
        [RunID],
        [Run_Type],
        [Run_Snapshot_Date],
        [Run_Status],
        [Run_Processor],
        [Rule_Version],
        SUM([Allocated_From_Lots]) AS [Allocated_From_Lots],
        SUM([Allocated_Lot_Count]) AS [Allocated_Lot_Count],
        [Source_Category],
        [Source_Label],
        [Public_Detail]
    FROM [public_tx]
    GROUP BY
        [Transaction_Type],
        [Effective_Date],
        [Earned_Date],
        [RunID],
        [Run_Type],
        [Run_Snapshot_Date],
        [Run_Status],
        [Run_Processor],
        [Rule_Version],
        [Source_Category],
        [Source_Label],
        [Public_Detail]
),
[scored] AS
(
    SELECT
        entries.*,
        SUM(entries.[Points_Delta]) OVER
        (
            ORDER BY entries.[Effective_Date], entries.[Posted_At], entries.[Sort_TransactionID]
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS [Running_Balance]
    FROM [public_entries] AS entries
)
SELECT TOP (?)
    [Public_Entry_Count],
    [Transaction_Type],
    [Points_Delta],
    [Base_Points],
    [Multiplier],
    [Effective_Date],
    [Earned_Date],
    [Posted_At],
    [RunID],
    [Run_Type],
    [Run_Snapshot_Date],
    [Run_Status],
    [Run_Processor],
    [Rule_Version],
    [Allocated_From_Lots],
    [Allocated_Lot_Count],
    [Running_Balance],
    [Source_Category],
    [Source_Label],
    [Public_Detail]
FROM [scored]
ORDER BY [Posted_At] DESC, [Sort_TransactionID] DESC
"""


REWARDS_PUBLIC_LOTS_SQL = """
WITH [public_lots] AS
(
    SELECT
        [Original_Points],
        [Remaining_Points],
        [Allocated_Points],
        [Allocation_Count],
        [Earned_Date],
        [Expires_On],
        [Days_Until_Expiration],
        [Aging_Status],
        [RunID],
        CASE
            WHEN [Source_Type] IN (N'membership_event', N'legacy_membership_gap_award') THEN N'membership'
            WHEN [Source_Type] = N'rated_game_participation' THEN N'rated_games'
            WHEN [Source_Type] = N'tournament_host' THEN N'tournament_host'
            WHEN [Source_Type] = N'state_championship' THEN N'state_championship'
            WHEN [Source_Type] = N'opening_balance' THEN N'opening_balance'
            WHEN [Source_Type] = N'legacy_dues_credit_adjustment' THEN N'adjustment'
            ELSE N'other'
        END AS [Source_Category],
        CASE
            WHEN [Source_Type] IN (N'membership_event', N'legacy_membership_gap_award') THEN N'Membership awards'
            WHEN [Source_Type] = N'rated_game_participation' THEN N'Rated games'
            WHEN [Source_Type] = N'tournament_host' THEN N'Tournament host'
            WHEN [Source_Type] = N'state_championship' THEN N'State Championship'
            WHEN [Source_Type] = N'opening_balance' THEN N'Opening balance'
            WHEN [Source_Type] = N'legacy_dues_credit_adjustment' THEN N'Dues credit adjustment'
            ELSE N'Other'
        END AS [Source_Label]
    FROM [rewards].[v_point_lot_aging]
    WHERE UPPER([Chapter_Code]) = UPPER(?)
)
SELECT TOP (?)
    COUNT(*) AS [Lot_Count],
    SUM([Original_Points]) AS [Original_Points],
    SUM([Remaining_Points]) AS [Remaining_Points],
    SUM([Allocated_Points]) AS [Allocated_Points],
    SUM([Allocation_Count]) AS [Allocation_Count],
    [Earned_Date],
    [Expires_On],
    MIN([Days_Until_Expiration]) AS [Days_Until_Expiration],
    [Aging_Status],
    [RunID],
    [Source_Category],
    [Source_Label]
FROM [public_lots]
GROUP BY
    [Earned_Date],
    [Expires_On],
    [Aging_Status],
    [RunID],
    [Source_Category],
    [Source_Label]
ORDER BY
    CASE WHEN SUM([Remaining_Points]) > 0 THEN 0 ELSE 1 END,
    [Expires_On],
    [Earned_Date],
    [Source_Label]
"""


REWARDS_PUBLIC_BREAKDOWN_SQL = """
WITH [public_tx] AS
(
    SELECT
        CASE
            WHEN [Source_Type] IN (N'membership_event', N'legacy_membership_gap_award') THEN N'membership'
            WHEN [Source_Type] = N'rated_game_participation' THEN N'rated_games'
            WHEN [Source_Type] = N'tournament_host' THEN N'tournament_host'
            WHEN [Source_Type] = N'state_championship' THEN N'state_championship'
            WHEN [Source_Type] = N'redemption' THEN N'redemption'
            WHEN [Source_Type] = N'opening_balance' THEN N'opening_balance'
            WHEN [Source_Type] = N'point_expiration' THEN N'expiration'
            WHEN [Source_Type] = N'legacy_dues_credit_adjustment' THEN N'adjustment'
            ELSE N'other'
        END AS [Source_Category],
        CASE
            WHEN [Source_Type] IN (N'membership_event', N'legacy_membership_gap_award') THEN N'Membership awards'
            WHEN [Source_Type] = N'rated_game_participation' THEN N'Rated games'
            WHEN [Source_Type] = N'tournament_host' THEN N'Tournament host'
            WHEN [Source_Type] = N'state_championship' THEN N'State Championship'
            WHEN [Source_Type] = N'redemption' THEN N'Redemption'
            WHEN [Source_Type] = N'opening_balance' THEN N'Opening balance'
            WHEN [Source_Type] = N'point_expiration' THEN N'Expiration'
            WHEN [Source_Type] = N'legacy_dues_credit_adjustment' THEN N'Dues credit adjustment'
            ELSE N'Other'
        END AS [Source_Label],
        [Points_Delta]
    FROM [rewards].[v_chapter_transaction_history]
    WHERE UPPER([Chapter_Code]) = UPPER(?)
)
SELECT
    [Source_Category],
    [Source_Label],
    COUNT(*) AS [Transaction_Count],
    SUM(CASE WHEN [Points_Delta] > 0 THEN [Points_Delta] ELSE 0 END) AS [Credit_Points],
    SUM(CASE WHEN [Points_Delta] < 0 THEN -[Points_Delta] ELSE 0 END) AS [Debit_Points],
    SUM([Points_Delta]) AS [Net_Points]
FROM [public_tx]
GROUP BY [Source_Category], [Source_Label]
ORDER BY [Net_Points] DESC, [Source_Label]
"""


REWARDS_PUBLIC_REDEMPTIONS_SQL = """
SELECT TOP (?)
    [Request_Date],
    [Points],
    [Amount_USD],
    [Redemption_Category],
    [Payment_Mode],
    [Status],
    [Posted_At]
FROM [rewards].[redemption_requests]
WHERE UPPER([Chapter_Code]) = UPPER(?)
  AND [Status] = N'posted'
ORDER BY [Request_Date] DESC, [Posted_At] DESC
"""


def _rewards_int(value) -> int:
    if value is None:
        return 0
    return int(value)


def _rewards_optional_int(value) -> int | None:
    if value is None:
        return None
    return int(value)


def _rewards_text(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _rewards_balance_payload(row: dict) -> dict:
    available_points = _rewards_int(row.get("Available_Points"))
    return {
        "chapter_code": _rewards_text(row.get("Chapter_Code")),
        "chapter_name": _rewards_text(row.get("Chapter_Name")),
        "as_of_date": explorer.json_safe_value(row.get("As_Of_Date")),
        "latest_snapshot_date": explorer.json_safe_value(row.get("Latest_Snapshot_Date")),
        "is_current": bool(row.get("Is_Current")) if row.get("Is_Current") is not None else None,
        "active_member_count": _rewards_int(row.get("Active_Member_Count")),
        "multiplier": _rewards_int(row.get("Multiplier")),
        "available_points": available_points,
        "available_value_usd": round(available_points / 1000, 3),
        "total_remaining_points": _rewards_int(row.get("Total_Remaining_Points")),
        "expired_unallocated_points": _rewards_int(row.get("Expired_Unallocated_Points")),
        "original_points": _rewards_int(row.get("Original_Points")),
        "consumed_points": _rewards_int(row.get("Consumed_Points")),
        "ledger_balance": _rewards_int(row.get("Ledger_Balance")),
        "balance_reconciliation_delta": _rewards_int(row.get("Balance_Reconciliation_Delta")),
        "total_credits": _rewards_int(row.get("Total_Credits")),
        "total_debits": _rewards_int(row.get("Total_Debits")),
        "lot_count": _rewards_int(row.get("Lot_Count")),
        "transaction_count": _rewards_int(row.get("Transaction_Count")),
        "expiring_30_days": _rewards_int(row.get("Expiring_30_Days")),
        "expiring_60_days": _rewards_int(row.get("Expiring_60_Days")),
        "expiring_90_days": _rewards_int(row.get("Expiring_90_Days")),
        "next_expiration_date": explorer.json_safe_value(row.get("Next_Expiration_Date")),
        "last_transaction_posted_at": explorer.json_safe_value(row.get("Last_Transaction_Posted_At")),
    }


def _rewards_transaction_payload(row: dict) -> dict:
    points = _rewards_int(row.get("Points_Delta"))
    return {
        "entry_count": _rewards_int(row.get("Public_Entry_Count")) or 1,
        "transaction_type": _rewards_text(row.get("Transaction_Type")),
        "points_delta": points,
        "value_delta_usd": round(points / 1000, 3),
        "base_points": _rewards_optional_int(row.get("Base_Points")),
        "multiplier": _rewards_optional_int(row.get("Multiplier")),
        "effective_date": explorer.json_safe_value(row.get("Effective_Date")),
        "earned_date": explorer.json_safe_value(row.get("Earned_Date")),
        "posted_at": explorer.json_safe_value(row.get("Posted_At")),
        "run_id": _rewards_optional_int(row.get("RunID")),
        "run_type": _rewards_text(row.get("Run_Type")),
        "run_snapshot_date": explorer.json_safe_value(row.get("Run_Snapshot_Date")),
        "run_status": _rewards_text(row.get("Run_Status")),
        "run_processor": _rewards_text(row.get("Run_Processor")),
        "rule_version": _rewards_text(row.get("Rule_Version")),
        "allocated_from_lots": _rewards_int(row.get("Allocated_From_Lots")),
        "allocated_lot_count": _rewards_int(row.get("Allocated_Lot_Count")),
        "running_balance": _rewards_int(row.get("Running_Balance")),
        "source_category": _rewards_text(row.get("Source_Category")) or "other",
        "source_label": _rewards_text(row.get("Source_Label")) or "Other",
        "public_detail": _rewards_text(row.get("Public_Detail")) or "Rewards activity",
    }


def _rewards_lot_payload(row: dict) -> dict:
    return {
        "lot_count": _rewards_int(row.get("Lot_Count")) or 1,
        "original_points": _rewards_int(row.get("Original_Points")),
        "remaining_points": _rewards_int(row.get("Remaining_Points")),
        "allocated_points": _rewards_int(row.get("Allocated_Points")),
        "allocation_count": _rewards_int(row.get("Allocation_Count")),
        "earned_date": explorer.json_safe_value(row.get("Earned_Date")),
        "expires_on": explorer.json_safe_value(row.get("Expires_On")),
        "days_until_expiration": _rewards_optional_int(row.get("Days_Until_Expiration")),
        "aging_status": _rewards_text(row.get("Aging_Status")),
        "run_id": _rewards_optional_int(row.get("RunID")),
        "source_category": _rewards_text(row.get("Source_Category")) or "other",
        "source_label": _rewards_text(row.get("Source_Label")) or "Other",
    }


def _rewards_breakdown_payload(row: dict) -> dict:
    return {
        "source_category": _rewards_text(row.get("Source_Category")) or "other",
        "source_label": _rewards_text(row.get("Source_Label")) or "Other",
        "transaction_count": _rewards_int(row.get("Transaction_Count")),
        "credit_points": _rewards_int(row.get("Credit_Points")),
        "debit_points": _rewards_int(row.get("Debit_Points")),
        "net_points": _rewards_int(row.get("Net_Points")),
    }


def _rewards_redemption_payload(row: dict) -> dict:
    points = _rewards_int(row.get("Points"))
    amount = row.get("Amount_USD")
    return {
        "request_date": explorer.json_safe_value(row.get("Request_Date")),
        "points": points,
        "amount_usd": float(amount) if amount is not None else round(points / 1000, 3),
        "redemption_category": _rewards_text(row.get("Redemption_Category")),
        "payment_mode": _rewards_text(row.get("Payment_Mode")),
        "status": _rewards_text(row.get("Status")),
        "posted_at": explorer.json_safe_value(row.get("Posted_At")),
    }


def _rewards_summary_payload(chapters: list[dict]) -> dict:
    available_points = sum(chapter["available_points"] for chapter in chapters)
    ledger_balance = sum(chapter["ledger_balance"] for chapter in chapters)
    total_credits = sum(chapter["total_credits"] for chapter in chapters)
    total_debits = sum(chapter["total_debits"] for chapter in chapters)
    return {
        "chapter_count": len(chapters),
        "available_points": available_points,
        "available_value_usd": round(available_points / 1000, 3),
        "ledger_balance": ledger_balance,
        "total_credits": total_credits,
        "total_debits": total_debits,
        "expiring_30_days": sum(chapter["expiring_30_days"] for chapter in chapters),
        "expiring_60_days": sum(chapter["expiring_60_days"] for chapter in chapters),
        "expiring_90_days": sum(chapter["expiring_90_days"] for chapter in chapters),
        "reconciliation_delta": sum(chapter["balance_reconciliation_delta"] for chapter in chapters),
    }


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


def _load_snapshot_or_error() -> tuple[dict | None, func.HttpResponse | None]:
    if (os.environ.get("RATINGS_EXPLORER_DISABLE_SNAPSHOT") or "").strip().lower() in {"1", "true", "yes", "on"}:
        return None, None
    snapshot = explorer.load_snapshot()
    if snapshot:
        return snapshot, None
    return None, None


def _get_conn_str_or_error() -> tuple[str | None, func.HttpResponse | None]:
    return SQL_CONNECTION_STRING, None


def _player_detail_has_recent_game_handicap(payload: dict | None) -> bool:
    if not payload:
        return False
    recent_games = payload.get("recent_games") or []
    if not recent_games:
        return True
    return all("handicap" in game for game in recent_games)


def _player_detail_has_recent_game_sgf_metadata(payload: dict | None) -> bool:
    if not payload:
        return False
    recent_games = payload.get("recent_games") or []
    if not recent_games:
        return True
    return all("game_id" in game and "has_sgf" in game for game in recent_games)


def _player_detail_has_recent_game_rank_metadata(payload: dict | None) -> bool:
    if not payload:
        return False
    recent_games = payload.get("recent_games") or []
    if not recent_games:
        return True
    sgf_games = [game for game in recent_games if game.get("has_sgf")]
    if not sgf_games:
        return True
    return all("player_rank" in game and "opponent_rank" in game for game in sgf_games)


def _tournament_detail_has_game_sgf_metadata(payload: dict | None) -> bool:
    if not payload:
        return False
    games = payload.get("games") or []
    if not games:
        return True
    return all("has_sgf" in game for game in games)


def _history_payload_from_points(history: list[tuple[datetime, float, float]]) -> list[dict]:
    return explorer.serialize_rating_history(history)


def _parse_search_limit(req: func.HttpRequest) -> tuple[int | None, func.HttpResponse | None]:
    limit_text = (req.params.get("limit") or "").strip()
    if not limit_text:
        return DEFAULT_SEARCH_LIMIT, None
    if not limit_text.isdigit():
        return None, func.HttpResponse("Query parameter 'limit' must be one of 10, 25, 50, 100, or 250.", status_code=400)
    limit = int(limit_text)
    if limit not in ALLOWED_SEARCH_LIMITS:
        return None, func.HttpResponse("Query parameter 'limit' must be one of 10, 25, 50, 100, or 250.", status_code=400)
    return limit, None


def _parse_nonnegative_int_param(req: func.HttpRequest, name: str, default: int = 0) -> tuple[int | None, func.HttpResponse | None]:
    raw_text = (req.params.get(name) or "").strip()
    if not raw_text:
        return default, None
    if not raw_text.isdigit():
        return None, func.HttpResponse(f"Query parameter '{name}' must be a non-negative integer.", status_code=400)
    return int(raw_text), None


def _years_ago_iso(years: int, today: date) -> str:
    try:
        return today.replace(year=today.year - years).isoformat()
    except ValueError:
        return today.replace(month=2, day=28, year=today.year - years).isoformat()


def _parse_recent_activity_cutoff(req: func.HttpRequest) -> tuple[str | None, func.HttpResponse | None]:
    raw_years_text = req.params.get("recent_activity_years")
    years_text = (raw_years_text or "").strip()
    if years_text.lower() in {"none", "all", "no_limit", "nolimit"}:
        return None, None
    if not years_text:
        return _years_ago_iso(DEFAULT_RECENT_ACTIVITY_YEARS, datetime.now(timezone.utc).date()), None
    if not years_text.isdigit():
        return None, func.HttpResponse(
            "Query parameter 'recent_activity_years' must be one of 1, 3, 5, or 10.",
            status_code=400,
        )
    years = int(years_text)
    if years not in ALLOWED_ACTIVITY_YEARS:
        return None, func.HttpResponse(
            "Query parameter 'recent_activity_years' must be one of 1, 3, 5, or 10.",
            status_code=400,
        )
    return _years_ago_iso(years, datetime.now(timezone.utc).date()), None


def _parse_rating_bands(req: func.HttpRequest) -> tuple[list[str] | None, func.HttpResponse | None]:
    rating_bands_text = (req.params.get("rating_bands") or req.params.get("rating_band") or "").strip()
    if not rating_bands_text:
        return None, None
    rating_bands = [value.strip() for value in rating_bands_text.split(",") if value.strip()]
    invalid = [value for value in rating_bands if value not in ALLOWED_RATING_BANDS]
    if invalid:
        return None, func.HttpResponse(
            "Query parameter 'rating_bands' is invalid.",
            status_code=400,
        )
    return rating_bands, None


def _parse_player_status(req: func.HttpRequest) -> tuple[str, func.HttpResponse | None]:
    status_filter = (req.params.get("status") or "").strip().lower() or "all"
    if status_filter not in ALLOWED_PLAYER_STATUS_FILTERS:
        return "all", func.HttpResponse(
            "Query parameter 'status' must be one of All, Active, or Expired.",
            status_code=400,
        )
    return status_filter, None


def _parse_csv_values(req: func.HttpRequest, key: str, legacy_key: str | None = None) -> list[str] | None:
    raw = (req.params.get(key) or req.params.get(legacy_key or "") or "").strip()
    if not raw:
        return None
    values = [value.strip() for value in raw.split(",") if value.strip()]
    return values or None


def _is_default_player_startup_search(
    agaid: int | None,
    first_name: str | None,
    last_name: str | None,
    chapters: list[str] | None,
    states: list[str] | None,
    member_types: list[str] | None,
    status_filter: str | None,
    recent_activity_cutoff: str | None,
    rating_bands: list[str] | None,
) -> bool:
    return (
        agaid is None
        and not (first_name or "").strip()
        and not (last_name or "").strip()
        and not chapters
        and not states
        and not member_types
        and (status_filter or "all").strip().lower() == "all"
        and recent_activity_cutoff is None
        and not rating_bands
    )


@app.function_name(name="RewardsPublicReportPage")
@app.route(route="ratings-explorer/rewards", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_public_report_page(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        explorer.load_ratings_explorer_html("", "rewards_report.html"),
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="RewardsPublicBalances")
@app.route(route="ratings-explorer/rewards/balances", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_public_balances(req: func.HttpRequest) -> func.HttpResponse:
    started = perf_counter()
    try:
        rows = explorer.query_rows(SQL_CONNECTION_STRING, REWARDS_PUBLIC_BALANCES_SQL, [])
        chapters = [_rewards_balance_payload(row) for row in rows]
        return _public_json_response(
            _with_debug(
                {
                    "ok": True,
                    "generated_at": _utc_now_text(),
                    "summary": _rewards_summary_payload(chapters),
                    "chapters": chapters,
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return func.HttpResponse(f"Rewards balance report failed: {exc}", status_code=500)


@app.function_name(name="RewardsPublicChapter")
@app.route(route="ratings-explorer/rewards/chapter", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def rewards_public_chapter(req: func.HttpRequest) -> func.HttpResponse:
    started = perf_counter()
    chapter_code, error = _parse_rewards_chapter_code(req)
    if error:
        return error
    limit, error = _parse_rewards_limit(req)
    if error:
        return error
    try:
        balance_rows = explorer.query_rows(SQL_CONNECTION_STRING, REWARDS_PUBLIC_CHAPTER_BALANCE_SQL, [chapter_code])
        if not balance_rows:
            return func.HttpResponse(f"No rewards balance found for chapter '{chapter_code}'.", status_code=404)
        transactions = [
            _rewards_transaction_payload(row)
            for row in explorer.query_rows(SQL_CONNECTION_STRING, REWARDS_PUBLIC_TRANSACTIONS_SQL, [chapter_code, limit])
        ]
        lots = [
            _rewards_lot_payload(row)
            for row in explorer.query_rows(SQL_CONNECTION_STRING, REWARDS_PUBLIC_LOTS_SQL, [chapter_code, limit])
        ]
        breakdown = [
            _rewards_breakdown_payload(row)
            for row in explorer.query_rows(SQL_CONNECTION_STRING, REWARDS_PUBLIC_BREAKDOWN_SQL, [chapter_code])
        ]
        redemptions = [
            _rewards_redemption_payload(row)
            for row in explorer.query_rows(SQL_CONNECTION_STRING, REWARDS_PUBLIC_REDEMPTIONS_SQL, [limit, chapter_code])
        ]
        return _public_json_response(
            _with_debug(
                {
                    "ok": True,
                    "generated_at": _utc_now_text(),
                    "chapter": _rewards_balance_payload(balance_rows[0]),
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


@app.function_name(name="RatingsExplorerPage")
@app.route(route="ratings-explorer", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_page(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        explorer.load_ratings_explorer_html(""),
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="RatingsExplorerMobilePage")
@app.route(route="ratings-explorer/mobile", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_mobile_page(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        explorer.load_ratings_explorer_html("", "ratings_explorer_mobile.html"),
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="BayRateStagingPage")
@app.route(route="ratings-explorer/bayrate", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_page(req: func.HttpRequest) -> func.HttpResponse:
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter, html=True)
    if auth_error:
        return auth_error
    return func.HttpResponse(
        explorer.load_ratings_explorer_html("", "bayrate_staging.html"),
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="BayRateStagingPreview")
@app.route(route="ratings-explorer/bayrate/preview", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_preview(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("stage"):
        return _bayrate_preview_error("BayRate staging modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, error = _bayrate_request_json(req)
    if error:
        return error
    report_inputs, report_metadata, error = _bayrate_report_inputs_from_body(body, adapter)
    if error:
        return error

    duplicate_check = bool(body.get("duplicate_check", True))
    try:
        payload = build_staging_payload(
            report_inputs,
            adapter=adapter,
            duplicate_check=duplicate_check,
            report_metadata=report_metadata,
        )
    except Exception as exc:
        return _bayrate_preview_error(str(exc))

    response_adapter = adapter if duplicate_check else None
    return _bayrate_json_response(
        {
            **_bayrate_payload_response(payload, adapter=response_adapter, written=False),
            "duplicate_check": duplicate_check and bool(adapter),
        }
    )


@app.function_name(name="BayRateMetadataOptions")
@app.route(route="ratings-explorer/bayrate/metadata-options", methods=["GET", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_metadata_options(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    if load_host_chapter_options is None:
        return _bayrate_preview_error("BayRate metadata option lookup is not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    try:
        host_chapter_options = load_host_chapter_options(adapter)
    except Exception as exc:
        return _bayrate_preview_error(str(exc), status_code=500)
    return _bayrate_json_response({"ok": True, "host_chapter_options": host_chapter_options})


@app.function_name(name="BayRateStagingWrite")
@app.route(route="ratings-explorer/bayrate/stage", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_write(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("stage", "write"):
        return _bayrate_preview_error("BayRate staging write modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, error = _bayrate_request_json(req)
    if error:
        return error
    if body.get("confirm_stage") is not True:
        return _bayrate_preview_error("confirm_stage=true is required.")
    report_inputs, report_metadata, error = _bayrate_report_inputs_from_body(body, adapter)
    if error:
        return error
    try:
        payload = build_staging_payload(
            report_inputs,
            adapter=adapter,
            duplicate_check=True,
            report_metadata=report_metadata,
        )
        ensure_payload_run_id(payload, adapter)
        adapter.execute_statements(build_insert_statements(payload))
        payload["written"] = True
        payload["dry_run"] = False
    except Exception as exc:
        return _bayrate_preview_error(str(exc), status_code=500)
    return _bayrate_json_response(_bayrate_payload_response(payload, adapter=adapter, written=True))


@app.function_name(name="BayRateStagingReview")
@app.route(route="ratings-explorer/bayrate/review", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_review(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("stage", "review"):
        return _bayrate_preview_error("BayRate review modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, error = _bayrate_request_json(req)
    if error:
        return error
    if body.get("confirm_review") is not True:
        return _bayrate_preview_error("confirm_review=true is required.")
    run_id = str(body.get("run_id") or "").strip()
    if not run_id:
        return _bayrate_preview_error("run_id is required.")
    source_report_ordinal = body.get("source_report_ordinal")
    if not isinstance(source_report_ordinal, int):
        return _bayrate_preview_error("source_report_ordinal must be an integer.")
    host_chapter, error = _bayrate_host_chapter_from_body(adapter, body)
    if error:
        return error
    try:
        payload = load_staged_run(adapter, run_id)
        apply_tournament_review_decision(
            payload,
            source_report_ordinal,
            use_duplicate_code=bool(body.get("use_duplicate_code", False)),
            mark_ready=bool(body.get("mark_ready", False)),
            operator_note=str(body.get("operator_note") or "").strip() or None,
            host_chapter_id=host_chapter.get("chapter_id") if host_chapter else None,
            host_chapter_code=host_chapter.get("chapter_code") if host_chapter else None,
            host_chapter_name=host_chapter.get("chapter_name") if host_chapter else None,
            reward_event_key=str(body.get("reward_event_key") or "").strip() or None
            if "reward_event_key" in body
            else None,
            reward_event_name=str(body.get("reward_event_name") or "").strip() or None
            if "reward_event_name" in body
            else None,
            reward_is_state_championship=_bayrate_optional_bool_from_body(
                body,
                "reward_is_state_championship",
                "rewardIsStateChampionship",
            ),
        )
        update_staged_run_review(adapter, payload)
    except Exception as exc:
        return _bayrate_preview_error(str(exc), status_code=500)
    return _bayrate_json_response(_bayrate_payload_response(payload, adapter=adapter, written=True))


@app.function_name(name="BayRateStagingRun")
@app.route(route="ratings-explorer/bayrate/run", methods=["GET", "POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_run(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("load", "review"):
        return _bayrate_preview_error("BayRate staged-run modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error

    if req.method == "GET":
        run_id = str(req.params.get("run_id") or "").strip()
    else:
        body, error = _bayrate_request_json(req)
        if error:
            return error
        run_id = str(body.get("run_id") or "").strip()
    if not run_id:
        return _bayrate_preview_error("run_id is required.")

    try:
        payload = load_staged_run(adapter, run_id)
    except Exception as exc:
        status_code = 404 if "was not found" in str(exc) else 500
        return _bayrate_preview_error(str(exc), status_code=status_code)
    return _bayrate_json_response(_bayrate_payload_response(payload, adapter=adapter, written=True))


@app.function_name(name="BayRateStagingReplay")
@app.route(route="ratings-explorer/bayrate/replay", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_replay(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("replay"):
        return _bayrate_preview_error("BayRate replay modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, error = _bayrate_request_json(req)
    if error:
        return error
    run_id = str(body.get("run_id") or "").strip()
    if not run_id:
        return _bayrate_preview_error("run_id is required.")
    try:
        artifact = run_staged_replay(
            adapter,
            run_id=run_id,
            allow_needs_review=bool(body.get("allow_needs_review", True)),
            write_artifact=False,
            persist_staged_ratings=bool(body.get("persist_staged_ratings", True)),
        )
    except Exception as exc:
        return _bayrate_preview_error(str(exc), status_code=500)
    response = _bayrate_replay_response(artifact)
    response["commit_state"] = _bayrate_commit_state(adapter, run_id)
    return _bayrate_json_response(response)


@app.function_name(name="BayRateStagingCommitPreview")
@app.route(route="ratings-explorer/bayrate/commit-preview", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_commit_preview(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("commit"):
        return _bayrate_preview_error("BayRate commit modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    _, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, error = _bayrate_request_json(req)
    if error:
        return error
    run_id = str(body.get("run_id") or "").strip()
    if not run_id:
        return _bayrate_preview_error("run_id is required.")
    try:
        plan = build_commit_plan(adapter, run_id)
    except Exception as exc:
        return _bayrate_preview_error(str(exc), status_code=500)
    return _bayrate_json_response(
        {
            "ok": True,
            "commit_plan": printable_commit_plan(plan),
            "commit_state": _bayrate_commit_state(adapter, run_id),
        }
    )


@app.function_name(name="BayRateStagingCommit")
@app.route(route="ratings-explorer/bayrate/commit", methods=["POST", "OPTIONS"], auth_level=func.AuthLevel.ANONYMOUS)
def bayrate_staging_commit(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return func.HttpResponse("", status_code=204, headers=explorer.response_headers("application/json; charset=utf-8"))
    if not _bayrate_modules_available("commit"):
        return _bayrate_preview_error("BayRate commit modules are not available in this deployment.", status_code=500)
    adapter, error = _bayrate_adapter_or_error()
    if error:
        return error
    authorization, auth_error = _bayrate_authorization_response(req, adapter)
    if auth_error:
        return auth_error
    body, error = _bayrate_request_json(req)
    if error:
        return error
    run_id = str(body.get("run_id") or "").strip()
    if not run_id:
        return _bayrate_preview_error("run_id is required.")
    if body.get("confirm_production_commit") is not True:
        return _bayrate_preview_error("confirm_production_commit=true is required.")
    expected_confirmation = f"COMMIT RUN {run_id}"
    if str(body.get("confirmation_text") or "").strip() != expected_confirmation:
        return _bayrate_preview_error(f'confirmation_text must be "{expected_confirmation}".')
    commit_plan_hash = str(body.get("commit_plan_hash") or "").strip()
    if not commit_plan_hash:
        return _bayrate_preview_error("commit_plan_hash is required. Preview the production commit before committing.")

    try:
        plan = commit_staged_run(
            adapter,
            run_id,
            confirm_production_commit=True,
            expected_plan_hash=commit_plan_hash,
            confirm_sgf_replacement=body.get("confirm_sgf_replacement") is True,
            operator_principal_name=(authorization or {}).get("principal_name"),
            operator_principal_id=(authorization or {}).get("principal_id"),
        )
    except ValueError as exc:
        status_code = 409 if "changed since preview" in str(exc) else 400
        return _bayrate_preview_error(str(exc), status_code=status_code)
    except Exception as exc:
        return _bayrate_preview_error(str(exc), status_code=500)

    return _bayrate_json_response(
        {
            "ok": True,
            "commit_plan": printable_commit_plan(plan),
            "commit_state": _bayrate_commit_state(adapter, run_id),
        }
    )


@app.function_name(name="RatingsExplorerPlayers")
@app.route(route="ratings-explorer/players", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_players(req: func.HttpRequest) -> func.HttpResponse:
    started = perf_counter()
    snapshot = explorer.load_player_search_snapshot()
    limit, error = _parse_search_limit(req)
    if error:
        return error
    recent_activity_cutoff, error = _parse_recent_activity_cutoff(req)
    if error:
        return error
    rating_bands, error = _parse_rating_bands(req)
    if error:
        return error
    status_filter, error = _parse_player_status(req)
    if error:
        return error
    agaid_text = (req.params.get("agaid") or "").strip()
    chapters = _parse_csv_values(req, "chapters", "chapter")
    states = _parse_csv_values(req, "states", "state")
    member_types = _parse_csv_values(req, "member_types", "member_type")
    first_name = (req.params.get("first_name") or "").strip() or None
    last_name = (req.params.get("last_name") or "").strip() or None
    agaid = None
    if agaid_text:
        if not agaid_text.isdigit():
            return func.HttpResponse("Query parameter 'agaid' must be numeric.", status_code=400)
        agaid = int(agaid_text)
    try:
        can_use_snapshot = bool(
            snapshot and (
                explorer.snapshot_supports_player_member_type(snapshot)
                or _is_default_player_startup_search(
                    agaid,
                    first_name,
                    last_name,
                    chapters,
                    states,
                    member_types,
                    status_filter,
                    recent_activity_cutoff,
                    rating_bands,
                )
            )
        )
        if can_use_snapshot:
            return _json_response(
                _with_debug(
                    {
                        "results": explorer.search_players_from_snapshot(
                            snapshot,
                            agaid,
                            first_name,
                            last_name,
                            chapters,
                            states,
                            member_types,
                            status_filter,
                            recent_activity_cutoff,
                            rating_bands,
                            limit,
                        )
                    },
                    data_source="player_search_snapshot",
                    elapsed_ms=round((perf_counter() - started) * 1000, 1),
                )
            )
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        return _json_response(
            _with_debug(
                {
                    "results": explorer.search_players(
                        conn_str,
                        agaid,
                        first_name,
                        last_name,
                        chapters,
                        states,
                        member_types,
                        status_filter,
                        recent_activity_cutoff,
                        rating_bands,
                        limit,
                    )
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return func.HttpResponse(f"Player search failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerPlayersStartup")
@app.route(route="ratings-explorer/players-startup", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_players_startup(req: func.HttpRequest) -> func.HttpResponse:
    started = perf_counter()
    limit, error = _parse_search_limit(req)
    if error:
        return error
    try:
        startup_payload = explorer.load_startup_players()
        if startup_payload:
            results = list(startup_payload.get("results") or [])[:limit]
            return _json_response(
                _with_debug(
                    {
                        "results": results,
                        "meta": startup_payload.get("meta") or {},
                    },
                    data_source="startup_snapshot",
                    elapsed_ms=round((perf_counter() - started) * 1000, 1),
                )
            )
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        return _json_response(
            _with_debug(
                {
                    "results": explorer.search_players(
                        conn_str,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        "all",
                        None,
                        None,
                        limit,
                    )
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return func.HttpResponse(f"Startup player load failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerTournaments")
@app.route(route="ratings-explorer/tournaments", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_tournaments(req: func.HttpRequest) -> func.HttpResponse:
    started = perf_counter()
    snapshot = explorer.load_tournament_search_snapshot()
    limit, error = _parse_search_limit(req)
    if error:
        return error
    page, error = _parse_nonnegative_int_param(req, "page")
    if error:
        return error
    try:
        tournament_code = (req.params.get("tournament_code") or "").strip() or None
        cities = _parse_csv_values(req, "cities", "city")
        states = _parse_csv_values(req, "states", "state")
        if snapshot:
            search_payload = explorer.search_tournaments_from_snapshot(
                snapshot,
                (req.params.get("description") or "").strip() or None,
                tournament_code,
                cities,
                states,
                (req.params.get("date_from") or "").strip() or None,
                (req.params.get("date_before") or "").strip() or None,
                limit,
                page=page,
            )
            return _json_response(
                _with_debug(
                    search_payload,
                    data_source="tournament_search_snapshot",
                    elapsed_ms=round((perf_counter() - started) * 1000, 1),
                )
            )
        legacy_snapshot, error = _load_snapshot_or_error()
        if error:
            return error
        if legacy_snapshot:
            search_payload = explorer.search_tournaments_from_snapshot(
                legacy_snapshot,
                (req.params.get("description") or "").strip() or None,
                tournament_code,
                cities,
                states,
                (req.params.get("date_from") or "").strip() or None,
                (req.params.get("date_before") or "").strip() or None,
                limit,
                page=page,
            )
            return _json_response(
                _with_debug(
                    search_payload,
                    data_source="main_snapshot_fallback",
                    elapsed_ms=round((perf_counter() - started) * 1000, 1),
                )
            )
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        search_payload = explorer.search_tournaments(
            conn_str,
            (req.params.get("description") or "").strip() or None,
            tournament_code,
            cities,
            states,
            (req.params.get("date_from") or "").strip() or None,
            (req.params.get("date_before") or "").strip() or None,
            limit,
            page=page,
        )
        return _json_response(
            _with_debug(
                search_payload,
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return func.HttpResponse(f"Tournament search failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerFilterOptions")
@app.route(route="ratings-explorer/filter-options", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_filter_options(req: func.HttpRequest) -> func.HttpResponse:
    started = perf_counter()
    try:
        filter_options = explorer.load_filter_options_snapshot()
        if filter_options:
            return _json_response(_with_debug(filter_options, data_source="filter_options_snapshot", elapsed_ms=round((perf_counter() - started) * 1000, 1)))
        snapshot, error = _load_snapshot_or_error()
        if error:
            return error
        if snapshot:
            return _json_response(_with_debug(explorer.build_filter_options_from_snapshot(snapshot), data_source="main_snapshot_fallback", elapsed_ms=round((perf_counter() - started) * 1000, 1)))
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        return _json_response(_with_debug(explorer.build_filter_options(conn_str), data_source="sql_live", elapsed_ms=round((perf_counter() - started) * 1000, 1)))
    except Exception as exc:
        return func.HttpResponse(f"Filter options failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerPlayer")
@app.route(route="ratings-explorer/player", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_player(req: func.HttpRequest) -> func.HttpResponse:
    started = perf_counter()
    agaid_text = (req.params.get("agaid") or "").strip()
    recent_games_sgf_only = (req.params.get("recent_games_sgf_only") or "").strip().lower() in {"1", "true", "yes", "on"}
    recent_tournaments_page_text = (req.params.get("recent_tournaments_page") or "0").strip()
    recent_games_page_text = (req.params.get("recent_games_page") or "0").strip()
    opponents_page_text = (req.params.get("opponents_page") or "0").strip()
    opponents_sort = (req.params.get("opponents_sort") or "games").strip().lower()
    if not agaid_text.isdigit():
        return func.HttpResponse("Query parameter 'agaid' must be numeric.", status_code=400)
    if not recent_tournaments_page_text.isdigit() or not recent_games_page_text.isdigit() or not opponents_page_text.isdigit():
        return func.HttpResponse("Paging parameters must be non-negative integers.", status_code=400)
    if opponents_sort not in {"games", "latest"}:
        return func.HttpResponse("Query parameter 'opponents_sort' must be 'games' or 'latest'.", status_code=400)
    try:
        data_source = None
        recent_tournaments_page = int(recent_tournaments_page_text)
        recent_games_page = int(recent_games_page_text)
        opponents_page = int(opponents_page_text)
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        payload = explorer.get_player_detail(
            conn_str,
            int(agaid_text),
            recent_games_sgf_only=recent_games_sgf_only,
            recent_tournaments_page=recent_tournaments_page,
            recent_games_page=recent_games_page,
            opponents_page=opponents_page,
            opponents_sort=opponents_sort,
            include_context=False,
        )
        history_points = explorer.load_sql_rating_history(int(agaid_text))
        if payload:
            data_source = "sql_live"
        if not payload:
            return func.HttpResponse(f"No player found for AGAID {agaid_text}.", status_code=404)
        payload = dict(payload)
        payload["rating_history"] = payload.get("rating_history") or _history_payload_from_points(history_points or [])
        payload["news_articles"] = []
        payload["review_videos"] = []
        payload["data_source"] = data_source or "unknown"
        return _json_response(_with_debug(payload, data_source=data_source or "unknown", elapsed_ms=round((perf_counter() - started) * 1000, 1)))
    except Exception as exc:
        return func.HttpResponse(f"Player detail failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerPlayerContext")
@app.route(route="ratings-explorer/player-context", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_player_context(req: func.HttpRequest) -> func.HttpResponse:
    started = perf_counter()
    agaid_text = (req.params.get("agaid") or "").strip()
    if not agaid_text.isdigit():
        return func.HttpResponse("Query parameter 'agaid' must be numeric.", status_code=400)
    try:
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        agaid = int(agaid_text)
        return _json_response(
            _with_debug(
                {
                    "news_articles": explorer.load_player_articles(conn_str, agaid),
                    "review_videos": explorer.load_player_review_videos(conn_str, agaid),
                },
                data_source="sql_live",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    except Exception as exc:
        return func.HttpResponse(f"Player context failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerTournament")
@app.route(route="ratings-explorer/tournament", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_tournament(req: func.HttpRequest) -> func.HttpResponse:
    started = perf_counter()
    tournament_code = (req.params.get("tournament_code") or "").strip()
    if not tournament_code:
        return func.HttpResponse("Query parameter 'tournament_code' is required.", status_code=400)
    try:
        data_source = None
        payload = explorer.load_tournament_detail_snapshot(tournament_code)
        if payload and not _tournament_detail_has_game_sgf_metadata(payload):
            payload = None
        if payload:
            data_source = "tournament_detail_snapshot"
        conn_str, error = _get_conn_str_or_error()
        if not payload and not error:
            payload = explorer.get_tournament_detail(conn_str, tournament_code)
            if payload:
                data_source = "sql_live"
        if not payload:
            if error:
                return error
            return func.HttpResponse(f"No tournament found for code '{tournament_code}'.", status_code=404)
        payload = dict(payload)
        payload["data_source"] = data_source or "unknown"
        return _json_response(_with_debug(payload, data_source=data_source or "unknown", elapsed_ms=round((perf_counter() - started) * 1000, 1)))
    except Exception as exc:
        return func.HttpResponse(f"Tournament detail failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerGameSgf")
@app.route(route="ratings-explorer/game-sgf", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_game_sgf(req: func.HttpRequest) -> func.HttpResponse:
    game_id_text = (req.params.get("game_id") or "").strip()
    if not game_id_text.isdigit():
        return func.HttpResponse("Query parameter 'game_id' must be numeric.", status_code=400)
    try:
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        sgf_code, sgf_text = explorer.load_game_sgf_text(conn_str, int(game_id_text))
        if not sgf_code:
            return func.HttpResponse(f"No SGF is linked for game {game_id_text}.", status_code=404)
        if sgf_text is None:
            return func.HttpResponse(f"SGF blob '{sgf_code}' was not found.", status_code=404)
        return func.HttpResponse(
            sgf_text,
            status_code=200,
            headers=explorer.response_headers("application/x-go-sgf; charset=utf-8"),
        )
    except Exception as exc:
        return func.HttpResponse(f"SGF lookup failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerGameSgfViewer")
@app.route(route="ratings-explorer/game-sgf-viewer", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_game_sgf_viewer(req: func.HttpRequest) -> func.HttpResponse:
    game_id_text = (req.params.get("game_id") or "").strip()
    if not game_id_text.isdigit():
        return func.HttpResponse("Query parameter 'game_id' must be numeric.", status_code=400)
    game_id = int(game_id_text)
    mobile = (req.params.get("mobile") or "").strip().lower() in {"1", "true", "yes", "on"}
    sgf_url = f"/api/ratings-explorer/game-sgf?game_id={game_id}"
    page = explorer.render_game_sgf_viewer_html(game_id, sgf_url, mobile=mobile)
    return func.HttpResponse(
        page,
        status_code=200,
        headers=explorer.response_headers("text/html; charset=utf-8"),
    )


@app.function_name(name="RatingsExplorerAsset")
@app.route(route="ratings-explorer/assets/{*asset_path}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_asset(req: func.HttpRequest) -> func.HttpResponse:
    asset_path = (req.route_params.get("asset_path") or "").strip()
    if not asset_path:
        return func.HttpResponse("Asset path is required.", status_code=400)
    payload, content_type = explorer.get_asset_bytes(asset_path)
    if payload is None or content_type is None:
        return func.HttpResponse(f"Asset '{asset_path}' was not found.", status_code=404)
    return func.HttpResponse(
        body=payload,
        status_code=200,
        headers=explorer.response_headers(content_type),
    )


@app.function_name(name="RatingsExplorerPlayerHistorySvg")
@app.route(route="ratings-explorer/player-history.svg", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_player_history_svg(req: func.HttpRequest) -> func.HttpResponse:
    agaid_text = (req.params.get("agaid") or "").strip()
    if not agaid_text.isdigit():
        return func.HttpResponse("Query parameter 'agaid' must be numeric.", status_code=400)
    try:
        conn_str, error = _get_conn_str_or_error()
        if error:
            return error
        history = explorer.load_sql_rating_history(int(agaid_text))
        member_name = explorer.load_member_name(conn_str, int(agaid_text))
        svg = explorer.render_single_history_svg(
            int(agaid_text),
            history,
            member_name,
        )
        return func.HttpResponse(
            svg,
            status_code=200,
            headers=explorer.response_headers("image/svg+xml; charset=utf-8"),
        )
    except ValueError as exc:
        return func.HttpResponse(str(exc), status_code=404)
    except Exception as exc:
        return func.HttpResponse(f"History chart generation failed: {exc}", status_code=500)


@app.function_name(name="RatingsExplorerSnapshotStatus")
@app.route(route="ratings-explorer/snapshot-status", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_snapshot_status(req: func.HttpRequest) -> func.HttpResponse:
    started = perf_counter()
    status = explorer.load_snapshot_status()
    snapshot_meta = (status or {}).get("snapshot_meta")
    if snapshot_meta:
        return _json_response(
            _with_debug(
                {
                    "snapshot_available": True,
                    "meta": snapshot_meta,
                    "job_status": status,
                },
                data_source="snapshot_status",
                elapsed_ms=round((perf_counter() - started) * 1000, 1),
            )
        )
    return _json_response(_with_debug({"snapshot_available": False, "meta": None, "job_status": status}, data_source="none", elapsed_ms=round((perf_counter() - started) * 1000, 1)))


@app.function_name(name="RatingsExplorerSnapshotWarm")
@app.route(route="ratings-explorer/snapshot-warm", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def ratings_explorer_snapshot_warm(req: func.HttpRequest) -> func.HttpResponse:
    started = perf_counter()
    startup = explorer.load_startup_players()
    player_search = explorer.load_player_search_snapshot()
    tournament_search = explorer.load_tournament_search_snapshot()
    filter_options = explorer.load_filter_options_snapshot()
    ok = bool(startup and player_search and tournament_search and filter_options)
    return _json_response(
        _with_debug(
            {
                "ok": ok,
                "snapshot_available": ok,
            },
            data_source="small_artifacts" if ok else "none",
            elapsed_ms=round((perf_counter() - started) * 1000, 1),
        )
    )


@app.function_name(name="RatingsExplorerSnapshotRefresh")
@app.route(route="ratings-explorer/snapshot-refresh", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def ratings_explorer_snapshot_refresh(req: func.HttpRequest) -> func.HttpResponse:
    requested_at = datetime.now(timezone.utc).isoformat()
    explorer.request_snapshot_refresh("http", requested_at)
    explorer.update_snapshot_status(
        "queued",
        source="http",
        detail="Snapshot refresh requested and waiting for timer pickup.",
        error=None,
    )
    return _json_response({"ok": True, "queued": True, "requested_at": requested_at})


@app.function_name(name="RatingsExplorerNightlySnapshot")
@app.schedule(schedule="0 15 6 * * *", arg_name="timer", run_on_startup=False, use_monitor=True)
def ratings_explorer_nightly_snapshot(timer: func.TimerRequest) -> None:
    conn_str = SQL_CONNECTION_STRING
    explorer.update_snapshot_status("running", source="timer", detail="Nightly snapshot refresh started.", error=None)
    try:
        snapshot = explorer.refresh_snapshot(conn_str)
        explorer.update_snapshot_status(
            "completed",
            source="timer",
            detail="Nightly snapshot refresh completed.",
            error=None,
            snapshot_meta=snapshot.get("meta", {}),
        )
    except Exception as exc:
        explorer.update_snapshot_status(
            "failed",
            source="timer",
            detail="Nightly snapshot refresh failed.",
            error=str(exc),
        )
        raise


@app.function_name(name="RatingsExplorerPendingSnapshotRefresh")
@app.schedule(schedule="0 */5 * * * *", arg_name="timer", run_on_startup=False, use_monitor=True)
def ratings_explorer_pending_snapshot_refresh(timer: func.TimerRequest) -> None:
    request = explorer.load_snapshot_request()
    if not request:
        return
    conn_str = SQL_CONNECTION_STRING
    try:
        explorer.update_snapshot_status(
            "running",
            source="manual-timer",
            detail=f"Pending snapshot refresh started from {request.get('source')}.",
            error=None,
        )
        snapshot = explorer.refresh_snapshot(conn_str)
        explorer.clear_snapshot_request()
        explorer.update_snapshot_status(
            "completed",
            source="manual-timer",
            detail="Pending snapshot refresh completed.",
            error=None,
            snapshot_meta=snapshot.get("meta", {}),
        )
    except Exception as exc:
        explorer.update_snapshot_status(
            "failed",
            source="manual-timer",
            detail="Pending snapshot refresh failed.",
            error=str(exc),
        )
        raise
