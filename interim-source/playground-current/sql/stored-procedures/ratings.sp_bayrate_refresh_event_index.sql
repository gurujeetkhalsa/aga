-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[sp_bayrate_refresh_event_index].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_refresh_event_index]
    @ModelLabel NVARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;
    WITH event_rows AS
    (
        SELECT
            CASE WHEN NULLIF(LTRIM(RTRIM(g.[Tournament_Code])), N'') IS NOT NULL THEN CONCAT(N'code:', LTRIM(RTRIM(g.[Tournament_Code])))
                 ELSE CONCAT(N'date:', CONVERT(nvarchar(10), CAST(g.[Game_Date] AS date), 23)) END AS [EventKey],
            CAST(g.[Game_Date] AS date) AS [EventDate],
            NULLIF(LTRIM(RTRIM(g.[Tournament_Code])), N'') AS [Tournament_Code],
            HASHBYTES('SHA2_256', STRING_AGG(CONCAT_WS(N'|', CONVERT(nvarchar(30), g.[Game_ID]), CONVERT(nvarchar(10), CAST(g.[Game_Date] AS date), 23), COALESCE(g.[Tournament_Code], N''), COALESCE(CONVERT(nvarchar(30), g.[Round]), N''), CONVERT(nvarchar(30), g.[Pin_Player_1]), COALESCE(g.[Color_1], N''), COALESCE(g.[Rank_1], N''), CONVERT(nvarchar(30), g.[Pin_Player_2]), COALESCE(g.[Color_2], N''), COALESCE(g.[Rank_2], N''), COALESCE(CONVERT(nvarchar(30), g.[Handicap]), N''), COALESCE(CONVERT(nvarchar(50), g.[Komi]), N''), COALESCE(g.[Result], N''), COALESCE(CONVERT(nvarchar(10), g.[Online]), N''), COALESCE(CONVERT(nvarchar(10), g.[Exclude]), N''), COALESCE(CONVERT(nvarchar(10), g.[Rated]), N'')), N';') WITHIN GROUP (ORDER BY g.[Game_Date], g.[Tournament_Code], g.[Round], g.[Game_ID])) AS [SourceHash]
        FROM [ratings].[vw_bayrate_games_source] AS g
        GROUP BY CASE WHEN NULLIF(LTRIM(RTRIM(g.[Tournament_Code])), N'') IS NOT NULL THEN CONCAT(N'code:', LTRIM(RTRIM(g.[Tournament_Code]))) ELSE CONCAT(N'date:', CONVERT(nvarchar(10), CAST(g.[Game_Date] AS date), 23)) END,
                 CAST(g.[Game_Date] AS date), NULLIF(LTRIM(RTRIM(g.[Tournament_Code])), N'')
    ),
    numbered AS
    (
        SELECT @ModelLabel AS [ModelLabel], ROW_NUMBER() OVER (ORDER BY [EventDate], COALESCE([Tournament_Code], N''), [EventKey]) AS [EventOrdinal], [EventKey], [EventDate], [Tournament_Code], [SourceHash]
        FROM event_rows
    )
    MERGE [ratings].[bayrate_event_index] AS tgt
    USING numbered AS src
      ON tgt.[ModelLabel] = src.[ModelLabel]
     AND tgt.[EventKey] = src.[EventKey]
    WHEN MATCHED THEN
        UPDATE SET
            tgt.[EventOrdinal] = src.[EventOrdinal],
            tgt.[EventDate] = src.[EventDate],
            tgt.[Tournament_Code] = src.[Tournament_Code],
            tgt.[SourceHash] = src.[SourceHash],
            tgt.[Dirty] = CASE WHEN tgt.[SourceHash] <> src.[SourceHash] THEN 1 ELSE tgt.[Dirty] END,
            tgt.[DirtyReason] = CASE WHEN tgt.[SourceHash] <> src.[SourceHash] THEN N'source_hash_changed' ELSE tgt.[DirtyReason] END,
            tgt.[LastUpdated] = SYSDATETIME()
    WHEN NOT MATCHED THEN
        INSERT ([ModelLabel], [EventOrdinal], [EventKey], [EventDate], [Tournament_Code], [SourceHash], [Dirty], [DirtyReason])
        VALUES (src.[ModelLabel], src.[EventOrdinal], src.[EventKey], src.[EventDate], src.[Tournament_Code], src.[SourceHash], 1, N'new_event');
END;
GO
