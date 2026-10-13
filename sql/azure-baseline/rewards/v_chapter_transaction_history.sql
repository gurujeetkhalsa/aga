-- Live Azure SQL metadata; database aga-membership; VIEW
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   VIEW [rewards].[v_chapter_transaction_history]
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
