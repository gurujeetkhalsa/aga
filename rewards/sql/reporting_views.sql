SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'rewards') IS NULL
    EXEC(N'CREATE SCHEMA [rewards]');
GO

CREATE OR ALTER VIEW [rewards].[v_chapter_balances]
AS
WITH [as_of] AS
(
    SELECT CAST(SYSUTCDATETIME() AS date) AS [As_Of_Date]
),
[latest_snapshot] AS
(
    SELECT MAX([Snapshot_Date]) AS [Snapshot_Date]
    FROM [rewards].[chapter_daily_snapshot]
),
[latest_chapters] AS
(
    SELECT
        c.[ChapterID],
        c.[Chapter_Code],
        c.[Chapter_Name],
        c.[Snapshot_Date] AS [Latest_Snapshot_Date],
        c.[Is_Current],
        c.[Active_Member_Count],
        c.[Multiplier]
    FROM [rewards].[chapter_daily_snapshot] AS c
    INNER JOIN [latest_snapshot] AS ls
        ON ls.[Snapshot_Date] = c.[Snapshot_Date]
),
[lot_summary] AS
(
    SELECT
        l.[ChapterID],
        MAX(l.[Chapter_Code]) AS [Chapter_Code],
        COUNT_BIG(*) AS [Lot_Count],
        SUM(l.[Original_Points]) AS [Original_Points],
        SUM(l.[Original_Points] - l.[Remaining_Points]) AS [Consumed_Points],
        SUM(l.[Remaining_Points]) AS [Total_Remaining_Points],
        SUM(CASE WHEN l.[Remaining_Points] > 0 AND l.[Expires_On] >= a.[As_Of_Date] THEN l.[Remaining_Points] ELSE 0 END) AS [Available_Points],
        SUM(CASE WHEN l.[Remaining_Points] > 0 AND l.[Expires_On] < a.[As_Of_Date] THEN l.[Remaining_Points] ELSE 0 END) AS [Expired_Unallocated_Points],
        SUM(CASE WHEN l.[Remaining_Points] > 0 AND l.[Expires_On] BETWEEN a.[As_Of_Date] AND DATEADD(day, 30, a.[As_Of_Date]) THEN l.[Remaining_Points] ELSE 0 END) AS [Expiring_30_Days],
        SUM(CASE WHEN l.[Remaining_Points] > 0 AND l.[Expires_On] BETWEEN a.[As_Of_Date] AND DATEADD(day, 60, a.[As_Of_Date]) THEN l.[Remaining_Points] ELSE 0 END) AS [Expiring_60_Days],
        SUM(CASE WHEN l.[Remaining_Points] > 0 AND l.[Expires_On] BETWEEN a.[As_Of_Date] AND DATEADD(day, 90, a.[As_Of_Date]) THEN l.[Remaining_Points] ELSE 0 END) AS [Expiring_90_Days],
        MIN(CASE WHEN l.[Remaining_Points] > 0 THEN l.[Expires_On] END) AS [Next_Expiration_Date],
        MAX(l.[Created_At]) AS [Last_Lot_Created_At]
    FROM [rewards].[point_lots] AS l
    CROSS JOIN [as_of] AS a
    GROUP BY l.[ChapterID]
),
[transaction_summary] AS
(
    SELECT
        t.[ChapterID],
        MAX(t.[Chapter_Code]) AS [Chapter_Code],
        COUNT_BIG(*) AS [Transaction_Count],
        SUM(t.[Points_Delta]) AS [Ledger_Balance],
        SUM(CASE WHEN t.[Points_Delta] > 0 THEN t.[Points_Delta] ELSE 0 END) AS [Total_Credits],
        SUM(CASE WHEN t.[Points_Delta] < 0 THEN -t.[Points_Delta] ELSE 0 END) AS [Total_Debits],
        MAX(t.[Posted_At]) AS [Last_Transaction_Posted_At]
    FROM [rewards].[transactions] AS t
    GROUP BY t.[ChapterID]
),
[chapter_keys] AS
(
    SELECT [ChapterID] FROM [latest_chapters]
    UNION
    SELECT [ChapterID] FROM [lot_summary]
    UNION
    SELECT [ChapterID] FROM [transaction_summary]
)
SELECT
    k.[ChapterID],
    COALESCE(c.[Chapter_Code], l.[Chapter_Code], t.[Chapter_Code]) AS [Chapter_Code],
    c.[Chapter_Name],
    a.[As_Of_Date],
    c.[Latest_Snapshot_Date],
    c.[Is_Current],
    c.[Active_Member_Count],
    c.[Multiplier],
    COALESCE(l.[Available_Points], 0) AS [Available_Points],
    COALESCE(l.[Total_Remaining_Points], 0) AS [Total_Remaining_Points],
    COALESCE(l.[Expired_Unallocated_Points], 0) AS [Expired_Unallocated_Points],
    COALESCE(l.[Original_Points], 0) AS [Original_Points],
    COALESCE(l.[Consumed_Points], 0) AS [Consumed_Points],
    COALESCE(t.[Ledger_Balance], 0) AS [Ledger_Balance],
    COALESCE(t.[Ledger_Balance], 0) - COALESCE(l.[Total_Remaining_Points], 0) AS [Balance_Reconciliation_Delta],
    COALESCE(t.[Total_Credits], 0) AS [Total_Credits],
    COALESCE(t.[Total_Debits], 0) AS [Total_Debits],
    COALESCE(l.[Lot_Count], 0) AS [Lot_Count],
    COALESCE(t.[Transaction_Count], 0) AS [Transaction_Count],
    COALESCE(l.[Expiring_30_Days], 0) AS [Expiring_30_Days],
    COALESCE(l.[Expiring_60_Days], 0) AS [Expiring_60_Days],
    COALESCE(l.[Expiring_90_Days], 0) AS [Expiring_90_Days],
    l.[Next_Expiration_Date],
    t.[Last_Transaction_Posted_At],
    l.[Last_Lot_Created_At]
FROM [chapter_keys] AS k
CROSS JOIN [as_of] AS a
LEFT JOIN [latest_chapters] AS c
    ON c.[ChapterID] = k.[ChapterID]
LEFT JOIN [lot_summary] AS l
    ON l.[ChapterID] = k.[ChapterID]
LEFT JOIN [transaction_summary] AS t
    ON t.[ChapterID] = k.[ChapterID];
GO

CREATE OR ALTER VIEW [rewards].[v_chapter_transaction_history]
AS
WITH [debit_allocations] AS
(
    SELECT
        a.[Debit_TransactionID] AS [TransactionID],
        SUM(a.[Points_Allocated]) AS [Allocated_From_Lots],
        COUNT_BIG(*) AS [Allocated_Lot_Count]
    FROM [rewards].[lot_allocations] AS a
    GROUP BY a.[Debit_TransactionID]
)
SELECT
    t.[TransactionID],
    t.[ChapterID],
    t.[Chapter_Code],
    t.[Transaction_Type],
    t.[Points_Delta],
    t.[Base_Points],
    t.[Multiplier],
    t.[Chapter_Active_Member_Count],
    t.[Effective_Date],
    t.[Earned_Date],
    t.[Valuation_Date],
    t.[Posted_At],
    t.[RunID],
    r.[Run_Type],
    r.[Snapshot_Date] AS [Run_Snapshot_Date],
    r.[Status] AS [Run_Status],
    JSON_VALUE(r.[SummaryJson], '$.processor') AS [Run_Processor],
    t.[Source_Type],
    t.[Source_Key],
    t.[Rule_Version],
    l.[LotID],
    l.[Original_Points] AS [Lot_Original_Points],
    l.[Remaining_Points] AS [Lot_Remaining_Points],
    l.[Expires_On] AS [Lot_Expires_On],
    COALESCE(da.[Allocated_From_Lots], 0) AS [Allocated_From_Lots],
    COALESCE(da.[Allocated_Lot_Count], 0) AS [Allocated_Lot_Count],
    t.[MetadataJson],
    t.[Created_By]
FROM [rewards].[transactions] AS t
LEFT JOIN [rewards].[reward_runs] AS r
    ON r.[RunID] = t.[RunID]
LEFT JOIN [rewards].[point_lots] AS l
    ON l.[Earn_TransactionID] = t.[TransactionID]
LEFT JOIN [debit_allocations] AS da
    ON da.[TransactionID] = t.[TransactionID];
GO

CREATE OR ALTER VIEW [rewards].[v_point_lot_aging]
AS
WITH [as_of] AS
(
    SELECT CAST(SYSUTCDATETIME() AS date) AS [As_Of_Date]
),
[allocation_summary] AS
(
    SELECT
        a.[LotID],
        SUM(a.[Points_Allocated]) AS [Allocated_Points],
        COUNT_BIG(*) AS [Allocation_Count],
        MAX(a.[Allocated_At]) AS [Last_Allocated_At]
    FROM [rewards].[lot_allocations] AS a
    GROUP BY a.[LotID]
)
SELECT
    l.[LotID],
    l.[Earn_TransactionID],
    l.[ChapterID],
    l.[Chapter_Code],
    l.[Original_Points],
    l.[Remaining_Points],
    COALESCE(a.[Allocated_Points], 0) AS [Allocated_Points],
    COALESCE(a.[Allocation_Count], 0) AS [Allocation_Count],
    l.[Earned_Date],
    l.[Expires_On],
    DATEDIFF(day, x.[As_Of_Date], l.[Expires_On]) AS [Days_Until_Expiration],
    CASE
        WHEN l.[Remaining_Points] = 0 THEN N'exhausted'
        WHEN l.[Expires_On] < x.[As_Of_Date] THEN N'expired_unallocated'
        WHEN l.[Expires_On] <= DATEADD(day, 30, x.[As_Of_Date]) THEN N'expiring_30_days'
        WHEN l.[Expires_On] <= DATEADD(day, 60, x.[As_Of_Date]) THEN N'expiring_60_days'
        WHEN l.[Expires_On] <= DATEADD(day, 90, x.[As_Of_Date]) THEN N'expiring_90_days'
        ELSE N'active'
    END AS [Aging_Status],
    l.[Source_Type],
    l.[Source_Key],
    t.[Transaction_Type],
    t.[Base_Points],
    t.[Multiplier],
    t.[Chapter_Active_Member_Count],
    t.[Posted_At] AS [Earn_Posted_At],
    t.[RunID],
    JSON_VALUE(r.[SummaryJson], '$.processor') AS [Run_Processor],
    l.[Created_At],
    a.[Last_Allocated_At]
FROM [rewards].[point_lots] AS l
CROSS JOIN [as_of] AS x
INNER JOIN [rewards].[transactions] AS t
    ON t.[TransactionID] = l.[Earn_TransactionID]
LEFT JOIN [rewards].[reward_runs] AS r
    ON r.[RunID] = t.[RunID]
LEFT JOIN [allocation_summary] AS a
    ON a.[LotID] = l.[LotID];
GO

CREATE OR ALTER VIEW [rewards].[v_reward_run_history]
AS
WITH [transaction_summary] AS
(
    SELECT
        t.[RunID],
        COUNT_BIG(*) AS [Transaction_Count],
        SUM(t.[Points_Delta]) AS [Net_Points],
        SUM(CASE WHEN t.[Points_Delta] > 0 THEN t.[Points_Delta] ELSE 0 END) AS [Credit_Points],
        SUM(CASE WHEN t.[Points_Delta] < 0 THEN -t.[Points_Delta] ELSE 0 END) AS [Debit_Points],
        MIN(t.[Posted_At]) AS [First_Transaction_Posted_At],
        MAX(t.[Posted_At]) AS [Last_Transaction_Posted_At]
    FROM [rewards].[transactions] AS t
    WHERE t.[RunID] IS NOT NULL
    GROUP BY t.[RunID]
)
SELECT
    r.[RunID],
    r.[Run_Type],
    r.[Snapshot_Date],
    r.[Started_At],
    r.[Completed_At],
    CASE
        WHEN r.[Completed_At] IS NULL THEN NULL
        ELSE DATEDIFF(second, r.[Started_At], r.[Completed_At])
    END AS [Duration_Seconds],
    r.[Status],
    JSON_VALUE(r.[SummaryJson], '$.processor') AS [Processor],
    COALESCE(t.[Transaction_Count], 0) AS [Transaction_Count],
    COALESCE(t.[Net_Points], 0) AS [Net_Points],
    COALESCE(t.[Credit_Points], 0) AS [Credit_Points],
    COALESCE(t.[Debit_Points], 0) AS [Debit_Points],
    t.[First_Transaction_Posted_At],
    t.[Last_Transaction_Posted_At],
    r.[SummaryJson],
    r.[Error_Message]
FROM [rewards].[reward_runs] AS r
LEFT JOIN [transaction_summary] AS t
    ON t.[RunID] = r.[RunID];
GO

CREATE OR ALTER VIEW [rewards].[v_membership_event_audit]
AS
SELECT
    e.[Membership_Event_ID],
    e.[Message_ID],
    e.[AGAID],
    e.[Event_Type],
    e.[Event_Date],
    e.[Received_At],
    e.[Member_Type],
    e.[Base_Points],
    e.[Term_Years],
    e.[Credit_Deadline],
    e.[Status],
    e.[Credited_TransactionID],
    t.[ChapterID] AS [Credited_ChapterID],
    t.[Chapter_Code] AS [Credited_Chapter_Code],
    t.[Points_Delta] AS [Credited_Points],
    t.[Multiplier] AS [Credited_Multiplier],
    t.[Chapter_Active_Member_Count] AS [Credited_Chapter_Active_Member_Count],
    t.[Effective_Date] AS [Credited_Effective_Date],
    t.[Posted_At] AS [Credited_Posted_At],
    t.[RunID] AS [Credited_RunID],
    e.[Expired_At],
    e.[Created_At],
    e.[Updated_At],
    e.[Source_Payload_Json]
FROM [rewards].[membership_events] AS e
LEFT JOIN [rewards].[transactions] AS t
    ON t.[TransactionID] = e.[Credited_TransactionID];
GO
