-- Live Azure SQL metadata; database aga-membership; VIEW
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   VIEW [rewards].[v_reward_run_history]
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
