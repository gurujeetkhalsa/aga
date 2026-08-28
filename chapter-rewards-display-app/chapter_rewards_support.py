# Copyright 2026, American Go Association, All rights reserved

import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import certifi
import pytds
try:
    import pyodbc
except Exception:
    pyodbc = None


def _app_root() -> Path:
    """Execute the app root routine."""
    return Path(__file__).resolve().parent


def get_sql_connection_string() -> str | None:
    """Return sql connection string."""
    conn = os.environ.get("SQL_CONNECTION_STRING") or os.environ.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
    if conn:
        return conn
    for settings_path in (_app_root() / "local.settings.json", _app_root().parent / "local.settings.json"):
        if settings_path.exists():
            try:
                values = json.loads(settings_path.read_text(encoding="utf-8")).get("Values", {})
            except (OSError, json.JSONDecodeError):
                continue
            conn = values.get("SQL_CONNECTION_STRING") or values.get("MYSQL_SYNC_SQL_CONNECTION_STRING")
            if conn:
                return conn
    return None


def response_headers(content_type: str) -> dict[str, str]:
    """Execute the response headers routine."""
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, X-Functions-Key, x-functions-key",
        "Cache-Control": "no-store",
        "Content-Type": content_type,
    }


def json_safe_value(value: Any) -> Any:
    """Execute the json safe value routine."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


def _parse_sql_connection_string(connection_string: str) -> dict[str, object]:
    """Parse sql connection string."""
    parts: dict[str, str] = {}
    for item in connection_string.split(";"):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        parts[key.strip().lower()] = value.strip().strip("{}")
    server = parts["server"].replace("tcp:", "")
    host, port_text = (server.split(",", 1) + ["1433"])[:2]
    return {
        "server": host,
        "port": int(port_text),
        "database": parts["database"],
        "user": parts["uid"],
        "password": parts["pwd"],
    }


def _tds_connect(conn_str: str):
    """Execute the tds connect routine."""
    sql = _parse_sql_connection_string(conn_str)
    return pytds.connect(
        server=sql["server"],
        port=sql["port"],
        database=sql["database"],
        user=sql["user"],
        password=sql["password"],
        cafile=certifi.where(),
        validate_host=True,
        enc_login_only=False,
        autocommit=True,
        timeout=60,
        as_dict=True,
    )


def query_rows(conn_str: str, query: str, params: list[Any] | tuple[Any, ...]) -> list[dict[str, Any]]:
    """Query rows."""
    if pyodbc is not None:
        try:
            return _query_rows_via_odbc(conn_str, query, params)
        except Exception:
            pass
    return _query_rows_via_tds(conn_str, query, params)


def _query_rows_via_odbc(conn_str: str, query: str, params: list[Any] | tuple[Any, ...]) -> list[dict[str, Any]]:
    """Query rows via odbc."""
    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(query, list(params))
            columns = [column[0] for column in cursor.description]
            rows = [dict(zip(columns, record)) for record in cursor.fetchall()]
            conn.commit()
            return rows
        finally:
            cursor.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _sql_literal(value: Any) -> str:
    """Execute the sql literal routine."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return "'" + value.isoformat() + "'"
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _query_rows_via_tds(conn_str: str, query: str, params: list[Any] | tuple[Any, ...]) -> list[dict[str, Any]]:
    """Query rows via tds."""
    conn = _tds_connect(conn_str)
    try:
        cursor = conn.cursor()
        rendered_query = query
        for value in params:
            rendered_query = rendered_query.replace("?", _sql_literal(value), 1)
        cursor.execute(rendered_query)
        return list(cursor.fetchall())
    finally:
        conn.close()

REWARDS_PUBLIC_BALANCES_SQL = """
SELECT
    balance.[Chapter_Code],
    balance.[Chapter_Name],
    balance.[As_Of_Date],
    balance.[Latest_Snapshot_Date],
    balance.[Is_Current],
    balance.[Active_Member_Count],
    balance.[Multiplier],
    balance.[Available_Points],
    balance.[Total_Remaining_Points],
    balance.[Expired_Unallocated_Points],
    balance.[Original_Points],
    balance.[Consumed_Points],
    balance.[Ledger_Balance],
    balance.[Balance_Reconciliation_Delta],
    balance.[Total_Credits],
    balance.[Total_Debits],
    COALESCE(recent.[Earned_30_Days], 0) AS [Earned_30_Days],
    COALESCE(recent.[Used_30_Days], 0) AS [Used_30_Days],
    balance.[Lot_Count],
    balance.[Transaction_Count],
    balance.[Expiring_30_Days],
    balance.[Expiring_60_Days],
    balance.[Expiring_90_Days],
    balance.[Next_Expiration_Date],
    balance.[Last_Transaction_Posted_At]
FROM [rewards].[v_chapter_balances] AS balance
LEFT JOIN [membership].[chapters] AS chapter
    ON chapter.[ChapterID] = balance.[ChapterID]
OUTER APPLY
(
    SELECT
        SUM(CASE WHEN tx.[Points_Delta] > 0 THEN tx.[Points_Delta] ELSE 0 END) AS [Earned_30_Days],
        SUM(CASE WHEN tx.[Points_Delta] < 0 THEN -tx.[Points_Delta] ELSE 0 END) AS [Used_30_Days]
    FROM [rewards].[transactions] AS tx
    WHERE tx.[ChapterID] = balance.[ChapterID]
      AND tx.[Posted_At] >= DATEADD(day, -30, SYSUTCDATETIME())
) AS recent
WHERE (
       COALESCE(balance.[Available_Points], 0) <> 0
    OR COALESCE(balance.[Ledger_Balance], 0) <> 0
    OR COALESCE(balance.[Total_Credits], 0) <> 0
    OR COALESCE(balance.[Total_Debits], 0) <> 0
)
  AND COALESCE(NULLIF(UPPER(LTRIM(RTRIM(chapter.[Status]))), N''), N'ACTIVE') <> N'DROPPED'
ORDER BY balance.[Available_Points] DESC, balance.[Chapter_Code]
"""


REWARDS_PUBLIC_CHAPTER_BALANCE_SQL = """
SELECT TOP 1
    balance.[Chapter_Code],
    balance.[Chapter_Name],
    balance.[As_Of_Date],
    balance.[Latest_Snapshot_Date],
    balance.[Is_Current],
    balance.[Active_Member_Count],
    balance.[Multiplier],
    balance.[Available_Points],
    balance.[Total_Remaining_Points],
    balance.[Expired_Unallocated_Points],
    balance.[Original_Points],
    balance.[Consumed_Points],
    balance.[Ledger_Balance],
    balance.[Balance_Reconciliation_Delta],
    balance.[Total_Credits],
    balance.[Total_Debits],
    COALESCE(recent.[Earned_30_Days], 0) AS [Earned_30_Days],
    COALESCE(recent.[Used_30_Days], 0) AS [Used_30_Days],
    balance.[Lot_Count],
    balance.[Transaction_Count],
    balance.[Expiring_30_Days],
    balance.[Expiring_60_Days],
    balance.[Expiring_90_Days],
    balance.[Next_Expiration_Date],
    balance.[Last_Transaction_Posted_At]
FROM [rewards].[v_chapter_balances] AS balance
LEFT JOIN [membership].[chapters] AS chapter
    ON chapter.[ChapterID] = balance.[ChapterID]
OUTER APPLY
(
    SELECT
        SUM(CASE WHEN tx.[Points_Delta] > 0 THEN tx.[Points_Delta] ELSE 0 END) AS [Earned_30_Days],
        SUM(CASE WHEN tx.[Points_Delta] < 0 THEN -tx.[Points_Delta] ELSE 0 END) AS [Used_30_Days]
    FROM [rewards].[transactions] AS tx
    WHERE tx.[ChapterID] = balance.[ChapterID]
      AND tx.[Posted_At] >= DATEADD(day, -30, SYSUTCDATETIME())
) AS recent
WHERE UPPER(balance.[Chapter_Code]) = UPPER(?)
  AND COALESCE(NULLIF(UPPER(LTRIM(RTRIM(chapter.[Status]))), N''), N'ACTIVE') <> N'DROPPED'
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
            WHEN tx.[Source_Type] = N'bayrate_rerun_reconciliation' THEN N'adjustment'
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
            WHEN tx.[Source_Type] = N'bayrate_rerun_reconciliation' THEN N'Bayrate rerun reconciliation'
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
                REPLACE(COALESCE(NULLIF(JSON_VALUE(tx.[MetadataJson], '$.payment_mode'), N''), N'other'), N'_', N' '),
                CASE
                    WHEN NULLIF(JSON_VALUE(tx.[MetadataJson], '$.description'), N'') IS NOT NULL
                        THEN CONCAT(N' - ', JSON_VALUE(tx.[MetadataJson], '$.description'))
                    ELSE N''
                END
            )
        WHEN tx.[Source_Type] = N'opening_balance' THEN N'Opening balance'
        WHEN tx.[Source_Type] = N'point_expiration' THEN N'Expired unused points'
        WHEN tx.[Source_Type] = N'legacy_dues_credit_adjustment' THEN N'Dues credit adjustment'
        WHEN tx.[Source_Type] = N'bayrate_rerun_reconciliation' THEN
            CONCAT(N'Bayrate Run ', JSON_VALUE(tx.[MetadataJson], '$.bayrate_run_id'), N' reconciliation')
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
),
[selected] AS
(
    SELECT TOP (?)
        [Public_Entry_Count],
        [Sort_TransactionID],
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
    ORDER BY [Effective_Date] DESC, [Posted_At] DESC, [Sort_TransactionID] DESC
)
SELECT
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
FROM [selected]
ORDER BY [Effective_Date] DESC, [Posted_At] DESC, [Sort_TransactionID] DESC
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
            WHEN [Source_Type] = N'bayrate_rerun_reconciliation' THEN N'adjustment'
            ELSE N'other'
        END AS [Source_Category],
        CASE
            WHEN [Source_Type] IN (N'membership_event', N'legacy_membership_gap_award') THEN N'Membership awards'
            WHEN [Source_Type] = N'rated_game_participation' THEN N'Rated games'
            WHEN [Source_Type] = N'tournament_host' THEN N'Tournament host'
            WHEN [Source_Type] = N'state_championship' THEN N'State Championship'
            WHEN [Source_Type] = N'opening_balance' THEN N'Opening balance'
            WHEN [Source_Type] = N'legacy_dues_credit_adjustment' THEN N'Dues credit adjustment'
            WHEN [Source_Type] = N'bayrate_rerun_reconciliation' THEN N'Bayrate rerun reconciliation'
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
            WHEN [Source_Type] = N'bayrate_rerun_reconciliation' THEN N'adjustment'
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
            WHEN [Source_Type] = N'bayrate_rerun_reconciliation' THEN N'Bayrate rerun reconciliation'
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
    request.[RedemptionID],
    request.[Posted_TransactionID],
    request.[Request_Date],
    request.[Points],
    request.[Amount_USD],
    request.[Redemption_Category],
    request.[Payment_Mode],
    request.[Description],
    request.[Receipt_Reference],
    request.[Status],
    request.[Posted_At],
    COALESCE(receipts.[Active_Receipt_Count], 0) AS [Active_Receipt_Count]
FROM [rewards].[redemption_requests] AS request
OUTER APPLY
(
    SELECT COUNT(*) AS [Active_Receipt_Count]
    FROM [rewards].[redemption_receipts] AS receipt
    WHERE receipt.[RedemptionID] = request.[RedemptionID]
      AND receipt.[Is_Deleted] = 0
) AS receipts
WHERE UPPER(request.[Chapter_Code]) = UPPER(?)
  AND request.[Status] = N'posted'
ORDER BY request.[Request_Date] DESC, request.[Posted_At] DESC
"""

def _rewards_int(value) -> int:
    """Execute the rewards int routine."""
    if value is None:
        return 0
    return int(value)


def _rewards_optional_int(value) -> int | None:
    """Execute the rewards optional int routine."""
    if value is None:
        return None
    return int(value)


def _rewards_text(value) -> str | None:
    """Execute the rewards text routine."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None

def _rewards_balance_payload(row: dict) -> dict:
    """Execute the rewards balance payload routine."""
    available_points = _rewards_int(row.get("Available_Points"))
    return {
        "chapter_code": _rewards_text(row.get("Chapter_Code")),
        "chapter_name": _rewards_text(row.get("Chapter_Name")),
        "as_of_date": json_safe_value(row.get("As_Of_Date")),
        "latest_snapshot_date": json_safe_value(row.get("Latest_Snapshot_Date")),
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
        "earned_30_days": _rewards_int(row.get("Earned_30_Days")),
        "used_30_days": _rewards_int(row.get("Used_30_Days")),
        "lot_count": _rewards_int(row.get("Lot_Count")),
        "transaction_count": _rewards_int(row.get("Transaction_Count")),
        "expiring_30_days": _rewards_int(row.get("Expiring_30_Days")),
        "expiring_60_days": _rewards_int(row.get("Expiring_60_Days")),
        "expiring_90_days": _rewards_int(row.get("Expiring_90_Days")),
        "next_expiration_date": json_safe_value(row.get("Next_Expiration_Date")),
        "last_transaction_posted_at": json_safe_value(row.get("Last_Transaction_Posted_At")),
    }


def _rewards_transaction_payload(row: dict) -> dict:
    """Execute the rewards transaction payload routine."""
    points = _rewards_int(row.get("Points_Delta"))
    return {
        "entry_count": _rewards_int(row.get("Public_Entry_Count")) or 1,
        "transaction_type": _rewards_text(row.get("Transaction_Type")),
        "points_delta": points,
        "value_delta_usd": round(points / 1000, 3),
        "base_points": _rewards_optional_int(row.get("Base_Points")),
        "multiplier": _rewards_optional_int(row.get("Multiplier")),
        "effective_date": json_safe_value(row.get("Effective_Date")),
        "earned_date": json_safe_value(row.get("Earned_Date")),
        "posted_at": json_safe_value(row.get("Posted_At")),
        "run_id": _rewards_optional_int(row.get("RunID")),
        "run_type": _rewards_text(row.get("Run_Type")),
        "run_snapshot_date": json_safe_value(row.get("Run_Snapshot_Date")),
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
    """Execute the rewards lot payload routine."""
    return {
        "lot_count": _rewards_int(row.get("Lot_Count")) or 1,
        "original_points": _rewards_int(row.get("Original_Points")),
        "remaining_points": _rewards_int(row.get("Remaining_Points")),
        "allocated_points": _rewards_int(row.get("Allocated_Points")),
        "allocation_count": _rewards_int(row.get("Allocation_Count")),
        "earned_date": json_safe_value(row.get("Earned_Date")),
        "expires_on": json_safe_value(row.get("Expires_On")),
        "days_until_expiration": _rewards_optional_int(row.get("Days_Until_Expiration")),
        "aging_status": _rewards_text(row.get("Aging_Status")),
        "run_id": _rewards_optional_int(row.get("RunID")),
        "source_category": _rewards_text(row.get("Source_Category")) or "other",
        "source_label": _rewards_text(row.get("Source_Label")) or "Other",
    }


def _rewards_breakdown_payload(row: dict) -> dict:
    """Execute the rewards breakdown payload routine."""
    return {
        "source_category": _rewards_text(row.get("Source_Category")) or "other",
        "source_label": _rewards_text(row.get("Source_Label")) or "Other",
        "transaction_count": _rewards_int(row.get("Transaction_Count")),
        "credit_points": _rewards_int(row.get("Credit_Points")),
        "debit_points": _rewards_int(row.get("Debit_Points")),
        "net_points": _rewards_int(row.get("Net_Points")),
    }


def _rewards_redemption_payload(row: dict) -> dict:
    """Execute the rewards redemption payload routine."""
    points = _rewards_int(row.get("Points"))
    amount = row.get("Amount_USD")
    return {
        "redemption_id": _rewards_optional_int(row.get("RedemptionID")),
        "transaction_id": _rewards_optional_int(row.get("Posted_TransactionID")),
        "request_date": json_safe_value(row.get("Request_Date")),
        "points": points,
        "amount_usd": float(amount) if amount is not None else round(points / 1000, 3),
        "redemption_category": _rewards_text(row.get("Redemption_Category")),
        "payment_mode": _rewards_text(row.get("Payment_Mode")),
        "description": _rewards_text(row.get("Description")),
        "receipt_reference": _rewards_text(row.get("Receipt_Reference")),
        "active_receipt_count": _rewards_int(row.get("Active_Receipt_Count")),
        "status": _rewards_text(row.get("Status")),
        "posted_at": json_safe_value(row.get("Posted_At")),
    }

def _rewards_summary_payload(chapters: list[dict]) -> dict:
    """Execute the rewards summary payload routine."""
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
