import base64
import json
import mimetypes
import os
import re
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import azure.functions as func


def _app_root() -> Path:
    """Execute the app root routine."""
    return Path(__file__).resolve().parent


def response_headers(content_type: str) -> dict[str, str]:
    """Execute the response headers routine."""
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
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


def _json_response(payload: dict, status_code: int = 200) -> func.HttpResponse:
    """Execute the json response routine."""
    return func.HttpResponse(
        json.dumps(payload, default=json_safe_value),
        status_code=status_code,
        headers=response_headers("application/json; charset=utf-8"),
    )


def _rewards_manual_debit_error(message: str, status_code: int = 400) -> func.HttpResponse:
    """Execute the rewards manual debit error routine."""
    return _json_response({"ok": False, "error": message}, status_code=status_code)

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


REWARDS_ADMIN_CHAPTER_OPTIONS_SQL = """
SELECT
    balance.[ChapterID],
    balance.[Chapter_Code],
    balance.[Chapter_Name],
    balance.[Available_Points],
    balance.[Latest_Snapshot_Date],
    balance.[Active_Member_Count],
    balance.[Multiplier]
FROM [rewards].[v_chapter_balances] AS balance
LEFT JOIN [membership].[chapters] AS chapter
    ON chapter.[ChapterID] = balance.[ChapterID]
WHERE balance.[Chapter_Code] IS NOT NULL
  AND COALESCE(NULLIF(UPPER(LTRIM(RTRIM(chapter.[Status]))), N''), N'ACTIVE') <> N'DROPPED'
ORDER BY balance.[Chapter_Code]
"""

REWARDS_MANUAL_DEBIT_SQL = """
EXEC [rewards].[sp_post_manual_redemption]
    @ChapterID = ?,
    @ChapterCode = ?,
    @RequestDate = ?,
    @Points = ?,
    @RedemptionCategory = ?,
    @PaymentMode = ?,
    @Description = ?,
    @ReceiptReference = ?,
    @ExternalRequestID = ?,
    @Notes = ?,
    @SourcePayloadJson = ?,
    @DryRun = ?,
    @RunType = N'manual',
    @PostedByPrincipalName = ?,
    @PostedByPrincipalId = ?
"""


REWARDS_MANUAL_DEBIT_LOOKUP_SQL = """
SELECT TOP 1
    request.[RedemptionID],
    request.[External_Request_ID],
    request.[ChapterID],
    request.[Chapter_Code],
    request.[Chapter_Name],
    request.[Request_Date],
    request.[Points],
    request.[Amount_USD],
    request.[Redemption_Category],
    request.[Payment_Mode],
    request.[Description],
    request.[Receipt_Reference],
    request.[Status],
    request.[Posted_TransactionID],
    request.[Posted_At],
    request.[Posted_By_Principal_Name],
    balance.[Available_Points],
    balance.[Latest_Snapshot_Date]
FROM [rewards].[redemption_requests] AS request
LEFT JOIN [rewards].[v_chapter_balances] AS balance
    ON balance.[ChapterID] = request.[ChapterID]
WHERE request.[External_Request_ID] = ?
ORDER BY request.[RedemptionID] DESC
"""


REWARDS_REDEMPTION_CATEGORIES = {"chapter_renewal", "go_promotion", "other"}
REWARDS_PAYMENT_MODES = {"dues_credit", "reimbursement", "other"}
REWARDS_MAX_MANUAL_DEBIT_POINTS = 100_000_000
REWARDS_RECEIPT_CONTAINER = (os.environ.get("REWARDS_RECEIPT_CONTAINER") or "").strip() or "chapter-rewards-receipts"
REWARDS_MAX_RECEIPT_BYTES = int(os.environ.get("REWARDS_MAX_RECEIPT_BYTES") or str(8 * 1024 * 1024))
REWARDS_MAX_RECEIPT_FILES = int(os.environ.get("REWARDS_MAX_RECEIPT_FILES") or "5")
REWARDS_ALLOWED_RECEIPT_CONTENT_TYPES = {
    "image/jpeg",
    "image/jpg",
    "image/png",
    "image/gif",
    "image/webp",
    "application/pdf",
}


REWARDS_RECEIPTS_SQL = """
SELECT
    receipt.[ReceiptID],
    receipt.[RedemptionID],
    receipt.[Posted_TransactionID],
    receipt.[ChapterID],
    receipt.[Chapter_Code],
    receipt.[Blob_Container],
    receipt.[Blob_Name],
    receipt.[Original_File_Name],
    receipt.[Content_Type],
    receipt.[Content_Length],
    receipt.[Uploaded_At],
    receipt.[Uploaded_By_Principal_Name],
    receipt.[Is_Deleted]
FROM [rewards].[redemption_receipts] AS receipt
WHERE receipt.[RedemptionID] = ?
  AND receipt.[Is_Deleted] = 0
ORDER BY receipt.[Uploaded_At] DESC, receipt.[ReceiptID] DESC
"""


REWARDS_RECEIPT_LOOKUP_SQL = """
SELECT TOP 1
    receipt.[ReceiptID],
    receipt.[RedemptionID],
    receipt.[Posted_TransactionID],
    receipt.[ChapterID],
    receipt.[Chapter_Code],
    receipt.[Blob_Container],
    receipt.[Blob_Name],
    receipt.[Original_File_Name],
    receipt.[Content_Type],
    receipt.[Content_Length],
    receipt.[Uploaded_At],
    receipt.[Uploaded_By_Principal_Name],
    receipt.[Is_Deleted]
FROM [rewards].[redemption_receipts] AS receipt
WHERE receipt.[ReceiptID] = ?
  AND receipt.[Is_Deleted] = 0
"""


REWARDS_ADMIN_REDEMPTION_SQL = """
SELECT TOP 1
    request.[RedemptionID],
    request.[External_Request_ID],
    request.[ChapterID],
    request.[Chapter_Code],
    request.[Chapter_Name],
    request.[Request_Date],
    request.[Points],
    request.[Amount_USD],
    request.[Redemption_Category],
    request.[Payment_Mode],
    request.[Description],
    request.[Receipt_Reference],
    request.[Status],
    request.[Posted_TransactionID],
    request.[Posted_At],
    request.[Posted_By_Principal_Name],
    request.[Notes],
    request.[Notes_Updated_At],
    request.[Notes_Updated_By_Principal_Name]
FROM [rewards].[redemption_requests] AS request
WHERE request.[RedemptionID] = ?
"""


REWARDS_ADD_RECEIPT_SQL = """
EXEC [rewards].[sp_add_redemption_receipt]
    @RedemptionID = ?,
    @BlobContainer = ?,
    @BlobName = ?,
    @OriginalFileName = ?,
    @ContentType = ?,
    @ContentLength = ?,
    @Sha256Hex = ?,
    @UploadedByPrincipalName = ?,
    @UploadedByPrincipalId = ?
"""


REWARDS_DELETE_RECEIPT_SQL = """
EXEC [rewards].[sp_delete_redemption_receipt]
    @ReceiptID = ?,
    @DeletedByPrincipalName = ?,
    @DeletedByPrincipalId = ?
"""


REWARDS_UPDATE_NOTES_SQL = """
EXEC [rewards].[sp_update_redemption_notes]
    @RedemptionID = ?,
    @Notes = ?,
    @UpdatedByPrincipalName = ?,
    @UpdatedByPrincipalId = ?
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


def _rewards_receipt_payload(row: dict) -> dict:
    """Execute the rewards receipt payload routine."""
    receipt_id = _rewards_optional_int(row.get("ReceiptID"))
    return {
        "receipt_id": receipt_id,
        "redemption_id": _rewards_optional_int(row.get("RedemptionID")),
        "transaction_id": _rewards_optional_int(row.get("Posted_TransactionID")),
        "chapter_id": _rewards_optional_int(row.get("ChapterID")),
        "chapter_code": _rewards_text(row.get("Chapter_Code")),
        "original_file_name": _rewards_text(row.get("Original_File_Name")),
        "content_type": _rewards_text(row.get("Content_Type")),
        "content_length": _rewards_int(row.get("Content_Length")),
        "uploaded_at": json_safe_value(row.get("Uploaded_At")),
        "uploaded_by_principal_name": _rewards_text(row.get("Uploaded_By_Principal_Name")),
        "is_deleted": bool(row.get("Is_Deleted")),
        "url": f"{api_route_root()}/admin/receipts/{receipt_id}" if receipt_id else None,
    }


def api_route_root() -> str:
    """Execute the api route root routine."""
    return "/api/chapter-rewards"


def _rewards_admin_redemption_payload(row: dict, receipts: list[dict] | None = None) -> dict:
    """Execute the rewards admin redemption payload routine."""
    payload = _rewards_redemption_payload(row)
    payload.update(
        {
            "external_request_id": _rewards_text(row.get("External_Request_ID")),
            "chapter_id": _rewards_optional_int(row.get("ChapterID")),
            "chapter_code": _rewards_text(row.get("Chapter_Code")),
            "chapter_name": _rewards_text(row.get("Chapter_Name")),
            "posted_by_principal_name": _rewards_text(row.get("Posted_By_Principal_Name")),
            "notes": _rewards_text(row.get("Notes")),
            "notes_updated_at": json_safe_value(row.get("Notes_Updated_At")),
            "notes_updated_by_principal_name": _rewards_text(row.get("Notes_Updated_By_Principal_Name")),
            "receipts": receipts or [],
        }
    )
    return payload


def _rewards_admin_chapter_payload(row: dict) -> dict:
    """Execute the rewards admin chapter payload routine."""
    return {
        "chapter_id": _rewards_optional_int(row.get("ChapterID")),
        "chapter_code": _rewards_text(row.get("Chapter_Code")),
        "chapter_name": _rewards_text(row.get("Chapter_Name")),
        "available_points": _rewards_int(row.get("Available_Points")),
        "latest_snapshot_date": json_safe_value(row.get("Latest_Snapshot_Date")),
        "active_member_count": _rewards_int(row.get("Active_Member_Count")),
        "multiplier": _rewards_int(row.get("Multiplier")),
    }


def _rewards_manual_debit_payload(row: dict) -> dict:
    """Execute the rewards manual debit payload routine."""
    points = _rewards_int(row.get("Points"))
    available = _rewards_int(row.get("Available_Points"))
    amount = row.get("Amount_USD")
    return {
        "run_id": _rewards_optional_int(row.get("RunID")),
        "dry_run": bool(row.get("DryRun")),
        "redemption_id": _rewards_optional_int(row.get("RedemptionID") or row.get("Existing_RedemptionID")),
        "transaction_id": _rewards_optional_int(row.get("Posted_TransactionID") or row.get("Existing_TransactionID")),
        "external_request_id": _rewards_text(row.get("External_Request_ID")),
        "chapter_id": _rewards_optional_int(row.get("ChapterID")),
        "chapter_code": _rewards_text(row.get("Chapter_Code")),
        "chapter_name": _rewards_text(row.get("Chapter_Name")),
        "request_date": json_safe_value(row.get("Request_Date")),
        "points": points,
        "amount_usd": float(amount) if amount is not None else round(points / 1000, 3),
        "redemption_category": _rewards_text(row.get("Redemption_Category")),
        "payment_mode": _rewards_text(row.get("Payment_Mode")),
        "description": _rewards_text(row.get("Description")),
        "receipt_reference": _rewards_text(row.get("Receipt_Reference")),
        "status": _rewards_text(row.get("Status")),
        "posted_at": json_safe_value(row.get("Posted_At")),
        "posted_by_principal_name": _rewards_text(row.get("Posted_By_Principal_Name")),
        "available_points": available,
        "available_after_points": _rewards_int(row.get("Available_After_Points")),
        "available_lot_count": _rewards_int(row.get("Available_Lot_Count")),
        "insufficient_balance_count": _rewards_int(row.get("InsufficientBalanceCount")),
        "shortfall_points": _rewards_int(row.get("Shortfall_Points")),
        "already_posted_count": _rewards_int(row.get("AlreadyPostedCount")),
        "new_post_count": _rewards_int(row.get("NewPostCount")),
        "latest_snapshot_date": json_safe_value(row.get("Latest_Snapshot_Date")),
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

def _clean_rewards_body_text(body: dict, key: str, *, max_length: int, required: bool = False) -> tuple[str | None, str | None]:
    """Clean rewards body text."""
    text = str(body.get(key) or "").strip()
    if not text:
        if required:
            return None, f"{key} is required."
        return None, None
    if len(text) > max_length:
        return None, f"{key} must be {max_length} characters or fewer."
    return text, None


def _rewards_manual_debit_request_from_body(
    body: dict,
    authorization: dict,
    *,
    generate_request_id: bool,
) -> tuple[dict | None, func.HttpResponse | None]:
    """Execute the rewards manual debit request from body routine."""
    raw_chapter_id = body.get("chapter_id", body.get("chapterId"))
    chapter_id = None
    if raw_chapter_id not in (None, ""):
        try:
            chapter_id = int(str(raw_chapter_id).strip())
        except ValueError:
            return None, _rewards_manual_debit_error("chapter_id must be an integer.")
        if chapter_id <= 0:
            return None, _rewards_manual_debit_error("chapter_id must be positive.")

    chapter_code, error_text = _clean_rewards_body_text(body, "chapter_code", max_length=64)
    if error_text:
        return None, _rewards_manual_debit_error(error_text)
    if chapter_id is None and not chapter_code:
        return None, _rewards_manual_debit_error("Select a chapter before previewing or posting a debit.")

    raw_request_date = str(body.get("request_date") or body.get("requestDate") or date.today().isoformat()).strip()
    try:
        request_date = date.fromisoformat(raw_request_date[:10])
    except ValueError:
        return None, _rewards_manual_debit_error("request_date must use YYYY-MM-DD format.")

    try:
        points = int(str(body.get("points") or "").replace(",", "").strip())
    except ValueError:
        return None, _rewards_manual_debit_error("points must be a positive integer.")
    if points <= 0:
        return None, _rewards_manual_debit_error("points must be a positive integer.")
    if points > REWARDS_MAX_MANUAL_DEBIT_POINTS:
        return None, _rewards_manual_debit_error(f"points must be {REWARDS_MAX_MANUAL_DEBIT_POINTS:,} or less.")

    category = str(body.get("redemption_category") or body.get("category") or "").strip().lower()
    if category not in REWARDS_REDEMPTION_CATEGORIES:
        return None, _rewards_manual_debit_error("Choose a supported redemption category.")

    payment_mode = str(body.get("payment_mode") or body.get("paymentMode") or "").strip().lower()
    if payment_mode not in REWARDS_PAYMENT_MODES:
        return None, _rewards_manual_debit_error("Choose a supported payment mode.")

    description, error_text = _clean_rewards_body_text(body, "description", max_length=512, required=True)
    if error_text:
        return None, _rewards_manual_debit_error(error_text)

    receipt_reference, error_text = _clean_rewards_body_text(body, "receipt_reference", max_length=256)
    if error_text:
        return None, _rewards_manual_debit_error(error_text)

    notes, error_text = _clean_rewards_body_text(body, "notes", max_length=4000)
    if error_text:
        return None, _rewards_manual_debit_error(error_text)

    external_request_id, error_text = _clean_rewards_body_text(body, "external_request_id", max_length=64)
    if error_text:
        return None, _rewards_manual_debit_error(error_text)
    if external_request_id is None and generate_request_id:
        external_request_id = f"manual-{datetime.now(timezone.utc):%Y%m%d}-{uuid.uuid4().hex[:12]}"

    source_payload = {
        "entry_source": "rewards_admin_form",
        "chapter_id": chapter_id,
        "chapter_code": chapter_code,
        "request_date": request_date.isoformat(),
        "points": points,
        "redemption_category": category,
        "payment_mode": payment_mode,
        "description": description,
        "receipt_reference": receipt_reference,
        "notes": notes,
        "entered_by_principal_name": authorization.get("principal_name"),
        "entered_by_principal_id": authorization.get("principal_id"),
    }

    return {
        "chapter_id": chapter_id,
        "chapter_code": chapter_code,
        "request_date": request_date,
        "points": points,
        "redemption_category": category,
        "payment_mode": payment_mode,
        "description": description,
        "receipt_reference": receipt_reference,
        "external_request_id": external_request_id,
        "notes": notes,
        "source_payload_json": json.dumps(source_payload, sort_keys=True),
        "principal_name": authorization.get("principal_name"),
        "principal_id": authorization.get("principal_id"),
    }, None


def _rewards_manual_debit_params(request: dict, *, dry_run: bool) -> tuple:
    """Execute the rewards manual debit params routine."""
    return (
        request["chapter_id"],
        request["chapter_code"],
        request["request_date"],
        request["points"],
        request["redemption_category"],
        request["payment_mode"],
        request["description"],
        request["receipt_reference"],
        request["external_request_id"],
        request["notes"],
        request["source_payload_json"],
        bool(dry_run),
        request["principal_name"],
        request["principal_id"],
    )


def _safe_rewards_blob_component(value: str, fallback: str = "receipt") -> str:
    """Execute the safe rewards blob component routine."""
    text = str(value or "").strip()
    if not text:
        text = fallback
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("._-")
    return text[:180] or fallback


def _rewards_receipt_container_client():
    """Execute the rewards receipt container client routine."""
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError as exc:
        raise RuntimeError("azure-storage-blob is not installed for receipt uploads.") from exc

    connection_string = os.environ.get("AzureWebJobsStorage")
    blob_service_uri = os.environ.get("AzureWebJobsStorage__blobServiceUri")
    if connection_string:
        client = BlobServiceClient.from_connection_string(connection_string)
    elif blob_service_uri:
        try:
            from azure.identity import ManagedIdentityCredential
        except ImportError as exc:
            raise RuntimeError("azure-identity is required for managed identity receipt uploads.") from exc
        client_id = os.environ.get("AzureWebJobsStorage__clientId")
        credential = ManagedIdentityCredential(client_id=client_id) if client_id else ManagedIdentityCredential()
        client = BlobServiceClient(account_url=blob_service_uri, credential=credential)
    else:
        raise RuntimeError("No Azure Blob Storage configuration is available for receipt uploads.")

    container = client.get_container_client(REWARDS_RECEIPT_CONTAINER)
    try:
        container.create_container()
    except Exception:
        pass
    return container


def _decode_receipt_upload(item: dict) -> tuple[str, str, bytes]:
    """Decode receipt upload."""
    filename = _safe_rewards_blob_component(str(item.get("name") or "receipt"), fallback="receipt")
    content_type = str(item.get("content_type") or item.get("type") or "").strip().lower()
    if not content_type or content_type == "application/octet-stream":
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    if content_type in {"image/jpg", "image/pjpeg"}:
        content_type = "image/jpeg"
    if content_type not in REWARDS_ALLOWED_RECEIPT_CONTENT_TYPES:
        raise ValueError(f"{filename} has unsupported file type {content_type}.")

    data_url = str(item.get("data_url") or item.get("dataUrl") or "").strip()
    if "," in data_url and data_url.lower().startswith("data:"):
        encoded = data_url.split(",", 1)[1]
    else:
        encoded = str(item.get("base64") or "").strip()
    if not encoded:
        raise ValueError(f"{filename} is missing file content.")
    try:
        content = base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise ValueError(f"{filename} is not valid base64 content.") from exc
    if not content:
        raise ValueError(f"{filename} is empty.")
    if len(content) > REWARDS_MAX_RECEIPT_BYTES:
        raise ValueError(f"{filename} is larger than the {REWARDS_MAX_RECEIPT_BYTES // (1024 * 1024)} MB receipt limit.")
    return filename, content_type, content


def _rewards_receipt_blob_name(redemption_id: int, filename: str) -> str:
    """Execute the rewards receipt blob name routine."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"redemptions/{redemption_id}/{stamp}-{uuid.uuid4().hex[:12]}-{filename}"


def _positive_body_int(body: dict, key: str) -> tuple[int | None, func.HttpResponse | None]:
    """Execute the positive body int routine."""
    try:
        value = int(str(body.get(key) or "").strip())
    except ValueError:
        return None, _rewards_manual_debit_error(f"{key} must be a positive integer.")
    if value <= 0:
        return None, _rewards_manual_debit_error(f"{key} must be a positive integer.")
    return value, None
