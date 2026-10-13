-- Copyright 2026, American Go Association, All rights reserved

-- Live Azure SQL stored procedure export.
-- Source object: [rewards].[sp_import_legacy_gap_tournament_awards].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_import_legacy_gap_tournament_awards]
    @TournamentAwardsJson nvarchar(max),
    @DryRun bit = 0,
    @RunType nvarchar(32) = N'import',
    @SourceAsOfDate date = '2026-02-08',
    @LedgerStartDate date = '2026-05-02',
    @MinGames int = 15,
    @MaxGames int = 700,
    @MaxSupport int = 1000,
    @Exponent float = 0.93,
    @StateChampionshipPoints int = 200000,
    @HostSourceType nvarchar(64) = N'tournament_host',
    @StateSourceType nvarchar(64) = N'state_championship',
    @RuleVersion nvarchar(32) = N'2026-05-03'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'rewards.reward_runs', N'U') IS NULL
       OR OBJECT_ID(N'rewards.transactions', N'U') IS NULL
       OR OBJECT_ID(N'rewards.point_lots', N'U') IS NULL
    BEGIN
        THROW 53100, N'Rewards tournament award tables do not exist. Apply rewards/sql/chapter_rewards_schema.sql first.', 1;
    END;

    IF @LedgerStartDate < @SourceAsOfDate
        THROW 53101, N'LedgerStartDate must be on or after SourceAsOfDate.', 1;

    IF @RunType NOT IN (N'daily', N'manual', N'import', N'backfill')
        THROW 53102, N'Unsupported rewards tournament award run type.', 1;

    IF @MinGames < 0 OR @MaxGames <= @MinGames OR @MaxSupport < 0 OR @Exponent <= 0 OR @StateChampionshipPoints < 0
        THROW 53103, N'Invalid tournament award formula parameters.', 1;

    IF ISJSON(@TournamentAwardsJson) <> 1
        THROW 53104, N'TournamentAwardsJson must be a JSON array.', 1;

    DECLARE @Rows table
    (
        [Source_Row_Number] int NOT NULL,
        [ChapterID] int NOT NULL,
        [Input_Chapter_Code] nvarchar(64) NOT NULL,
        [Input_Chapter_Name] nvarchar(256) NULL,
        [Tournament_Date] date NOT NULL,
        [Tournament_Code] nvarchar(128) NOT NULL,
        [Rated_Game_Count] int NOT NULL,
        [Is_State_Championship] bit NOT NULL
    );

    INSERT INTO @Rows
    (
        [Source_Row_Number],
        [ChapterID],
        [Input_Chapter_Code],
        [Input_Chapter_Name],
        [Tournament_Date],
        [Tournament_Code],
        [Rated_Game_Count],
        [Is_State_Championship]
    )
    SELECT
        COALESCE(raw.[Source_Row_Number], CONVERT(int, parsed.[key]) + 1),
        raw.[ChapterID],
        UPPER(NULLIF(LTRIM(RTRIM(raw.[Chapter_Code])), N'')),
        NULLIF(LTRIM(RTRIM(raw.[Chapter_Name])), N''),
        raw.[Tournament_Date],
        NULLIF(LTRIM(RTRIM(raw.[Tournament_Code])), N''),
        raw.[Rated_Game_Count],
        raw.[Is_State_Championship]
    FROM OPENJSON(@TournamentAwardsJson) AS parsed
    CROSS APPLY OPENJSON(parsed.[value])
    WITH
    (
        [Source_Row_Number] int '$.source_row_number',
        [ChapterID] int '$.chapter_id',
        [Chapter_Code] nvarchar(64) '$.chapter_code',
        [Chapter_Name] nvarchar(256) '$.chapter_name',
        [Tournament_Date] date '$.tournament_date',
        [Tournament_Code] nvarchar(128) '$.tournament_code',
        [Rated_Game_Count] int '$.rated_game_count',
        [Is_State_Championship] bit '$.is_state_championship'
    ) AS raw;

    IF EXISTS
    (
        SELECT 1
        FROM @Rows
        WHERE [ChapterID] IS NULL
           OR [Input_Chapter_Code] IS NULL
           OR [Input_Chapter_Code] = N''
           OR [Tournament_Date] IS NULL
           OR [Tournament_Code] IS NULL
           OR [Tournament_Code] = N''
           OR [Rated_Game_Count] IS NULL
           OR [Is_State_Championship] IS NULL
    )
    BEGIN
        THROW 53105, N'Legacy tournament award import contains incomplete rows.', 1;
    END;

    IF EXISTS (SELECT 1 FROM @Rows WHERE [Rated_Game_Count] < 0)
        THROW 53106, N'Legacy tournament award import contains negative rated game counts.', 1;

    IF EXISTS
    (
        SELECT 1
        FROM @Rows
        WHERE [Tournament_Date] <= @SourceAsOfDate
           OR [Tournament_Date] > @LedgerStartDate
    )
    BEGIN
        THROW 53107, N'Legacy tournament award dates must be after the source balance date and on or before the ledger start date.', 1;
    END;

    IF EXISTS
    (
        SELECT 1
        FROM @Rows
        GROUP BY [ChapterID], [Tournament_Code]
        HAVING COUNT(*) > 1
    )
    BEGIN
        THROW 53108, N'Legacy tournament award import contains duplicate chapter/tournament rows.', 1;
    END;

    IF OBJECT_ID(N'tempdb..#LegacyTournamentAwards', N'U') IS NOT NULL
        DROP TABLE #LegacyTournamentAwards;

    ;WITH [scored] AS
    (
        SELECT
            rows.[Source_Row_Number],
            rows.[ChapterID],
            rows.[Input_Chapter_Code] AS [Chapter_Code],
            rows.[Input_Chapter_Name] AS [Chapter_Name],
            rows.[Tournament_Date],
            rows.[Tournament_Code],
            rows.[Rated_Game_Count],
            rows.[Is_State_Championship],
            CAST(
                CASE
                    WHEN rows.[Rated_Game_Count] <= @MinGames THEN 0
                    WHEN rows.[Rated_Game_Count] >= @MaxGames THEN @MaxSupport * 1000
                    ELSE ROUND(
                        @MaxSupport
                        * POWER((rows.[Rated_Game_Count] - @MinGames) * 1.0 / NULLIF(@MaxGames - @MinGames, 0), @Exponent)
                        * 1000,
                        0
                    )
                END
                AS int
            ) AS [Host_Award_Points],
            CAST(CASE WHEN rows.[Is_State_Championship] = 1 THEN @StateChampionshipPoints ELSE 0 END AS int) AS [State_Championship_Points]
        FROM @Rows AS rows
    )
    SELECT
        scored.[Source_Row_Number],
        scored.[ChapterID],
        scored.[Chapter_Code],
        scored.[Chapter_Name],
        scored.[Tournament_Date],
        scored.[Tournament_Code],
        scored.[Rated_Game_Count],
        scored.[Is_State_Championship],
        scored.[Host_Award_Points],
        scored.[State_Championship_Points],
        CONCAT(CONVERT(nvarchar(32), scored.[ChapterID]), N':', scored.[Tournament_Code]) AS [Source_Base_Key],
        CONCAT(CONVERT(nvarchar(32), scored.[ChapterID]), N':', scored.[Tournament_Code], N':points:', CONVERT(nvarchar(32), scored.[Host_Award_Points])) AS [Host_Source_Key],
        CONCAT(CONVERT(nvarchar(32), scored.[ChapterID]), N':', scored.[Tournament_Code], N':points:', CONVERT(nvarchar(32), scored.[State_Championship_Points])) AS [State_Source_Key],
        COALESCE(host_existing.[Existing_Points], 0) AS [Host_Existing_Points],
        host_current.[TransactionID] AS [Host_Current_TransactionID],
        CASE
            WHEN scored.[Host_Award_Points] > COALESCE(host_existing.[Existing_Points], 0)
             AND host_current.[TransactionID] IS NULL
                THEN scored.[Host_Award_Points] - COALESCE(host_existing.[Existing_Points], 0)
            ELSE 0
        END AS [Host_New_Points],
        COALESCE(state_existing.[Existing_Points], 0) AS [State_Existing_Points],
        state_current.[TransactionID] AS [State_Current_TransactionID],
        CASE
            WHEN scored.[State_Championship_Points] > COALESCE(state_existing.[Existing_Points], 0)
             AND state_current.[TransactionID] IS NULL
                THEN scored.[State_Championship_Points] - COALESCE(state_existing.[Existing_Points], 0)
            ELSE 0
        END AS [State_New_Points]
    INTO #LegacyTournamentAwards
    FROM [scored] AS scored
    OUTER APPLY
    (
        SELECT SUM(t.[Points_Delta]) AS [Existing_Points]
        FROM [rewards].[transactions] AS t
        WHERE t.[Source_Type] = @HostSourceType
          AND t.[Transaction_Type] = N'earn'
          AND t.[ChapterID] = scored.[ChapterID]
          AND LEFT(t.[Source_Key], LEN(CONCAT(CONVERT(nvarchar(32), scored.[ChapterID]), N':', scored.[Tournament_Code], N':points:')))
              = CONCAT(CONVERT(nvarchar(32), scored.[ChapterID]), N':', scored.[Tournament_Code], N':points:')
    ) AS host_existing
    OUTER APPLY
    (
        SELECT TOP 1 t.[TransactionID]
        FROM [rewards].[transactions] AS t
        WHERE t.[Source_Type] = @HostSourceType
          AND t.[Source_Key] = CONCAT(CONVERT(nvarchar(32), scored.[ChapterID]), N':', scored.[Tournament_Code], N':points:', CONVERT(nvarchar(32), scored.[Host_Award_Points]))
          AND t.[Transaction_Type] = N'earn'
          AND t.[ChapterID] = scored.[ChapterID]
        ORDER BY t.[TransactionID]
    ) AS host_current
    OUTER APPLY
    (
        SELECT SUM(t.[Points_Delta]) AS [Existing_Points]
        FROM [rewards].[transactions] AS t
        WHERE t.[Source_Type] = @StateSourceType
          AND t.[Transaction_Type] = N'earn'
          AND t.[ChapterID] = scored.[ChapterID]
          AND LEFT(t.[Source_Key], LEN(CONCAT(CONVERT(nvarchar(32), scored.[ChapterID]), N':', scored.[Tournament_Code], N':points:')))
              = CONCAT(CONVERT(nvarchar(32), scored.[ChapterID]), N':', scored.[Tournament_Code], N':points:')
    ) AS state_existing
    OUTER APPLY
    (
        SELECT TOP 1 t.[TransactionID]
        FROM [rewards].[transactions] AS t
        WHERE t.[Source_Type] = @StateSourceType
          AND t.[Source_Key] = CONCAT(CONVERT(nvarchar(32), scored.[ChapterID]), N':', scored.[Tournament_Code], N':points:', CONVERT(nvarchar(32), scored.[State_Championship_Points]))
          AND t.[Transaction_Type] = N'earn'
          AND t.[ChapterID] = scored.[ChapterID]
        ORDER BY t.[TransactionID]
    ) AS state_current;

    DECLARE @InputRowCount int = (SELECT COUNT(*) FROM #LegacyTournamentAwards);
    DECLARE @ChapterCount int = (SELECT COUNT(DISTINCT [ChapterID]) FROM #LegacyTournamentAwards);
    DECLARE @TournamentCount int = (SELECT COUNT(DISTINCT CONCAT(CONVERT(nvarchar(32), [ChapterID]), N':', [Tournament_Code])) FROM #LegacyTournamentAwards);
    DECLARE @RatedGameCount int = COALESCE((SELECT SUM([Rated_Game_Count]) FROM #LegacyTournamentAwards), 0);
    DECLARE @HostEligibleAwardCount int = (SELECT COUNT(*) FROM #LegacyTournamentAwards WHERE [Host_Award_Points] > 0);
    DECLARE @HostAlreadyAwardedCount int = (SELECT COUNT(*) FROM #LegacyTournamentAwards WHERE [Host_Award_Points] > 0 AND [Host_Existing_Points] >= [Host_Award_Points]);
    DECLARE @HostNewAwardCount int = (SELECT COUNT(*) FROM #LegacyTournamentAwards WHERE [Host_New_Points] > 0);
    DECLARE @HostPointTotal int = COALESCE((SELECT SUM([Host_New_Points]) FROM #LegacyTournamentAwards), 0);
    DECLARE @StateChampionshipGroupCount int = (SELECT COUNT(*) FROM #LegacyTournamentAwards WHERE [State_Championship_Points] > 0);
    DECLARE @StateAlreadyAwardedCount int = (SELECT COUNT(*) FROM #LegacyTournamentAwards WHERE [State_Championship_Points] > 0 AND [State_Existing_Points] >= [State_Championship_Points]);
    DECLARE @StateNewAwardCount int = (SELECT COUNT(*) FROM #LegacyTournamentAwards WHERE [State_New_Points] > 0);
    DECLARE @StateChampionshipPointTotal int = COALESCE((SELECT SUM([State_New_Points]) FROM #LegacyTournamentAwards), 0);
    DECLARE @NewAwardCount int = @HostNewAwardCount + @StateNewAwardCount;
    DECLARE @PointTotal int = @HostPointTotal + @StateChampionshipPointTotal;

    IF @DryRun = 1
    BEGIN
        SELECT
            CAST(NULL AS int) AS [RunID],
            CAST(1 AS bit) AS [DryRun],
            @SourceAsOfDate AS [SourceAsOfDate],
            @LedgerStartDate AS [LedgerStartDate],
            @InputRowCount AS [InputRowCount],
            @ChapterCount AS [ChapterCount],
            @TournamentCount AS [TournamentCount],
            @RatedGameCount AS [RatedGameCount],
            @HostEligibleAwardCount AS [HostEligibleAwardCount],
            @HostAlreadyAwardedCount AS [HostAlreadyAwardedCount],
            @HostNewAwardCount AS [HostNewAwardCount],
            @HostPointTotal AS [HostPointTotal],
            @StateChampionshipGroupCount AS [StateChampionshipGroupCount],
            @StateAlreadyAwardedCount AS [StateAlreadyAwardedCount],
            @StateNewAwardCount AS [StateNewAwardCount],
            @StateChampionshipPointTotal AS [StateChampionshipPointTotal],
            @NewAwardCount AS [NewAwardCount],
            @PointTotal AS [PointTotal];

        RETURN;
    END;

    DECLARE @InsertedRun table ([RunID] int NOT NULL);

    INSERT INTO [rewards].[reward_runs]
    (
        [Run_Type],
        [Snapshot_Date],
        [Started_At],
        [Status],
        [SummaryJson]
    )
    OUTPUT INSERTED.[RunID] INTO @InsertedRun ([RunID])
    VALUES
    (
        @RunType,
        @LedgerStartDate,
        SYSUTCDATETIME(),
        N'running',
        NULL
    );

    DECLARE @RunID int = (SELECT TOP 1 [RunID] FROM @InsertedRun);

    DECLARE @InsertedTransactions table
    (
        [TransactionID] bigint NOT NULL,
        [ChapterID] int NOT NULL,
        [Chapter_Code] nvarchar(64) NOT NULL,
        [Points_Delta] int NOT NULL,
        [Earned_Date] date NOT NULL,
        [Source_Type] nvarchar(64) NOT NULL,
        [Source_Key] nvarchar(256) NOT NULL
    );

    INSERT INTO [rewards].[transactions]
    (
        [ChapterID],
        [Chapter_Code],
        [Transaction_Type],
        [Points_Delta],
        [Base_Points],
        [Multiplier],
        [Chapter_Active_Member_Count],
        [Effective_Date],
        [Earned_Date],
        [Valuation_Date],
        [Posted_At],
        [RunID],
        [Source_Type],
        [Source_Key],
        [Rule_Version],
        [MetadataJson]
    )
    OUTPUT
        INSERTED.[TransactionID],
        INSERTED.[ChapterID],
        INSERTED.[Chapter_Code],
        INSERTED.[Points_Delta],
        INSERTED.[Earned_Date],
        INSERTED.[Source_Type],
        INSERTED.[Source_Key]
    INTO @InsertedTransactions
    (
        [TransactionID],
        [ChapterID],
        [Chapter_Code],
        [Points_Delta],
        [Earned_Date],
        [Source_Type],
        [Source_Key]
    )
    SELECT
        awards.[ChapterID],
        awards.[Chapter_Code],
        N'earn',
        awards.[New_Points],
        awards.[Desired_Points],
        NULL,
        NULL,
        awards.[Tournament_Date],
        awards.[Tournament_Date],
        awards.[Tournament_Date],
        SYSUTCDATETIME(),
        @RunID,
        awards.[Source_Type],
        awards.[Source_Key],
        @RuleVersion,
        (
            SELECT
                CAST(1 AS bit) AS [legacy_gap],
                @SourceAsOfDate AS [source_balance_as_of_date],
                @LedgerStartDate AS [ledger_start_date],
                awards.[Award_Type] AS [award_type],
                awards.[Source_Row_Number] AS [source_row_number],
                awards.[ChapterID] AS [chapter_id],
                awards.[Chapter_Code] AS [chapter_code],
                awards.[Chapter_Name] AS [chapter_name],
                awards.[Tournament_Date] AS [tournament_date],
                awards.[Tournament_Code] AS [tournament_code],
                awards.[Rated_Game_Count] AS [rated_game_count],
                awards.[Is_State_Championship] AS [is_state_championship],
                awards.[Desired_Points] AS [desired_points],
                awards.[Existing_Points] AS [existing_points],
                awards.[New_Points] AS [new_points],
                @MinGames AS [min_games],
                @MaxGames AS [max_games],
                @MaxSupport AS [max_support],
                @Exponent AS [exponent],
                @StateChampionshipPoints AS [state_championship_points]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM
    (
        SELECT
            N'tournament_host' AS [Award_Type],
            @HostSourceType AS [Source_Type],
            [Host_Source_Key] AS [Source_Key],
            [Source_Row_Number],
            [ChapterID],
            [Chapter_Code],
            [Chapter_Name],
            [Tournament_Date],
            [Tournament_Code],
            [Rated_Game_Count],
            [Is_State_Championship],
            [Host_Award_Points] AS [Desired_Points],
            [Host_Existing_Points] AS [Existing_Points],
            [Host_New_Points] AS [New_Points]
        FROM #LegacyTournamentAwards
        WHERE [Host_New_Points] > 0
        UNION ALL
        SELECT
            N'state_championship' AS [Award_Type],
            @StateSourceType AS [Source_Type],
            [State_Source_Key] AS [Source_Key],
            [Source_Row_Number],
            [ChapterID],
            [Chapter_Code],
            [Chapter_Name],
            [Tournament_Date],
            [Tournament_Code],
            [Rated_Game_Count],
            [Is_State_Championship],
            [State_Championship_Points] AS [Desired_Points],
            [State_Existing_Points] AS [Existing_Points],
            [State_New_Points] AS [New_Points]
        FROM #LegacyTournamentAwards
        WHERE [State_New_Points] > 0
    ) AS awards;

    DECLARE @InsertedAwardCount int = @@ROWCOUNT;

    INSERT INTO [rewards].[point_lots]
    (
        [Earn_TransactionID],
        [ChapterID],
        [Chapter_Code],
        [Original_Points],
        [Remaining_Points],
        [Earned_Date],
        [Expires_On],
        [Source_Type],
        [Source_Key]
    )
    SELECT
        tx.[TransactionID],
        tx.[ChapterID],
        tx.[Chapter_Code],
        tx.[Points_Delta],
        tx.[Points_Delta],
        tx.[Earned_Date],
        DATEADD(year, 2, tx.[Earned_Date]),
        tx.[Source_Type],
        tx.[Source_Key]
    FROM @InsertedTransactions AS tx;

    UPDATE [rewards].[reward_runs]
    SET
        [Completed_At] = SYSUTCDATETIME(),
        [Status] = N'succeeded',
        [SummaryJson] =
        (
            SELECT
                N'legacy_tournament_gap_award' AS [processor],
                CAST(1 AS bit) AS [legacy_gap],
                @SourceAsOfDate AS [source_balance_as_of_date],
                @LedgerStartDate AS [ledger_start_date],
                @InputRowCount AS [input_row_count],
                @ChapterCount AS [chapter_count],
                @TournamentCount AS [tournament_count],
                @RatedGameCount AS [rated_game_count],
                @HostEligibleAwardCount AS [host_eligible_award_count],
                @HostAlreadyAwardedCount AS [host_already_awarded_count],
                @HostNewAwardCount AS [host_new_award_count],
                @HostPointTotal AS [host_point_total],
                @StateChampionshipGroupCount AS [state_championship_group_count],
                @StateAlreadyAwardedCount AS [state_already_awarded_count],
                @StateNewAwardCount AS [state_new_award_count],
                @StateChampionshipPointTotal AS [state_championship_point_total],
                @InsertedAwardCount AS [new_award_count],
                @PointTotal AS [point_total],
                @MinGames AS [min_games],
                @MaxGames AS [max_games],
                @MaxSupport AS [max_support],
                @Exponent AS [exponent],
                @StateChampionshipPoints AS [state_championship_points]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    WHERE [RunID] = @RunID;

    SELECT
        @RunID AS [RunID],
        CAST(0 AS bit) AS [DryRun],
        @SourceAsOfDate AS [SourceAsOfDate],
        @LedgerStartDate AS [LedgerStartDate],
        @InputRowCount AS [InputRowCount],
        @ChapterCount AS [ChapterCount],
        @TournamentCount AS [TournamentCount],
        @RatedGameCount AS [RatedGameCount],
        @HostEligibleAwardCount AS [HostEligibleAwardCount],
        @HostAlreadyAwardedCount AS [HostAlreadyAwardedCount],
        @HostNewAwardCount AS [HostNewAwardCount],
        @HostPointTotal AS [HostPointTotal],
        @StateChampionshipGroupCount AS [StateChampionshipGroupCount],
        @StateAlreadyAwardedCount AS [StateAlreadyAwardedCount],
        @StateNewAwardCount AS [StateNewAwardCount],
        @StateChampionshipPointTotal AS [StateChampionshipPointTotal],
        @InsertedAwardCount AS [NewAwardCount],
        @PointTotal AS [PointTotal];
END;
GO
