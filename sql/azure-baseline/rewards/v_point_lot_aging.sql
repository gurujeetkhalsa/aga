-- Live Azure SQL metadata; database aga-membership; VIEW
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   VIEW [rewards].[v_point_lot_aging]
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
