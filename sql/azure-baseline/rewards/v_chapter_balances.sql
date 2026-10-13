-- Live Azure SQL metadata; database aga-membership; VIEW
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   VIEW [rewards].[v_chapter_balances]
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
