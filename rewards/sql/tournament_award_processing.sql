SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'rewards') IS NULL
    EXEC(N'CREATE SCHEMA [rewards]');
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_process_tournament_awards]
    @TournamentDateFrom date = NULL,
    @TournamentDateTo date = NULL,
    @RunType nvarchar(32) = N'daily',
    @DryRun bit = 0,
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

    IF OBJECT_ID(N'rewards.transactions', N'U') IS NULL
       OR OBJECT_ID(N'rewards.point_lots', N'U') IS NULL
       OR OBJECT_ID(N'rewards.reward_runs', N'U') IS NULL
    BEGIN
        THROW 52700, N'Rewards tournament award tables do not exist. Apply rewards/sql/chapter_rewards_schema.sql first.', 1;
    END;

    IF OBJECT_ID(N'ratings.tournaments', N'U') IS NULL
       OR OBJECT_ID(N'ratings.games', N'U') IS NULL
    BEGIN
        THROW 52701, N'Ratings tournament/game tables do not exist.', 1;
    END;

    IF COL_LENGTH(N'ratings.tournaments', N'Host_ChapterID') IS NULL
       OR COL_LENGTH(N'ratings.tournaments', N'Host_ChapterCode') IS NULL
       OR COL_LENGTH(N'ratings.tournaments', N'Host_ChapterName') IS NULL
       OR COL_LENGTH(N'ratings.tournaments', N'Reward_Event_Key') IS NULL
       OR COL_LENGTH(N'ratings.tournaments', N'Reward_Event_Name') IS NULL
       OR COL_LENGTH(N'ratings.tournaments', N'Reward_Is_State_Championship') IS NULL
    BEGIN
        THROW 52702, N'ratings.tournaments is missing BayRate reward metadata columns. Apply bayrate/sql/bayrate_staging_schema.sql first.', 1;
    END;

    SET @TournamentDateTo = COALESCE(@TournamentDateTo, CAST(SYSUTCDATETIME() AS date));

    IF @TournamentDateFrom IS NOT NULL AND @TournamentDateTo < @TournamentDateFrom
    BEGIN
        THROW 52703, N'TournamentDateTo must be on or after TournamentDateFrom.', 1;
    END;

    IF @RunType NOT IN (N'daily', N'manual', N'import', N'backfill')
    BEGIN
        THROW 52704, N'Unsupported rewards tournament award run type.', 1;
    END;

    IF @MinGames < 0 OR @MaxGames <= @MinGames OR @MaxSupport < 0 OR @Exponent <= 0 OR @StateChampionshipPoints < 0
    BEGIN
        THROW 52705, N'Invalid tournament award formula parameters.', 1;
    END;

    IF OBJECT_ID(N'tempdb..#TournamentAwardGroups', N'U') IS NOT NULL
    BEGIN
        DROP TABLE #TournamentAwardGroups;
    END;

    ;WITH [sections] AS
    (
        SELECT
            t.[Tournament_Code],
            CAST(t.[Tournament_Date] AS date) AS [Tournament_Date],
            t.[Host_ChapterID],
            NULLIF(LTRIM(RTRIM(t.[Host_ChapterCode])), N'') AS [Host_ChapterCode],
            NULLIF(LTRIM(RTRIM(t.[Host_ChapterName])), N'') AS [Host_ChapterName],
            COALESCE(NULLIF(LTRIM(RTRIM(t.[Reward_Event_Key])), N''), NULLIF(LTRIM(RTRIM(t.[Tournament_Code])), N'')) AS [Reward_Event_Key],
            COALESCE(NULLIF(LTRIM(RTRIM(t.[Reward_Event_Name])), N''), NULLIF(LTRIM(RTRIM(t.[Tournament_Descr])), N'')) AS [Reward_Event_Name],
            COALESCE(t.[Reward_Is_State_Championship], 0) AS [Reward_Is_State_Championship]
        FROM [ratings].[tournaments] AS t
        WHERE t.[Tournament_Code] IS NOT NULL
          AND t.[Tournament_Date] IS NOT NULL
    ),
    [valid_sections] AS
    (
        SELECT *
        FROM [sections]
        WHERE [Host_ChapterID] IS NOT NULL
          AND [Host_ChapterCode] IS NOT NULL
          AND [Reward_Event_Key] IS NOT NULL
    ),
    [groups] AS
    (
        SELECT
            [Host_ChapterID],
            MAX([Host_ChapterCode]) AS [Host_ChapterCode],
            MAX([Host_ChapterName]) AS [Host_ChapterName],
            [Reward_Event_Key],
            MAX([Reward_Event_Name]) AS [Reward_Event_Name],
            MIN([Tournament_Date]) AS [Event_Date],
            MAX([Tournament_Date]) AS [Last_Tournament_Date],
            COUNT(*) AS [Tournament_Section_Count],
            STRING_AGG(CONVERT(nvarchar(max), [Tournament_Code]), N',') WITHIN GROUP (ORDER BY [Tournament_Code]) AS [Tournament_Codes],
            MAX(CASE WHEN [Reward_Is_State_Championship] = 1 THEN 1 ELSE 0 END) AS [Is_State_Championship]
        FROM [valid_sections]
        GROUP BY [Host_ChapterID], [Reward_Event_Key]
    ),
    [game_counts] AS
    (
        SELECT
            s.[Host_ChapterID],
            s.[Reward_Event_Key],
            COUNT(g.[Game_ID]) AS [Rated_Game_Count]
        FROM [valid_sections] AS s
        LEFT JOIN [ratings].[games] AS g
            ON g.[Tournament_Code] = s.[Tournament_Code]
           AND COALESCE(g.[Rated], 1) = 1
           AND COALESCE(g.[Exclude], 0) = 0
        GROUP BY s.[Host_ChapterID], s.[Reward_Event_Key]
    ),
    [scored] AS
    (
        SELECT
            g.[Host_ChapterID],
            g.[Host_ChapterCode],
            g.[Host_ChapterName],
            g.[Reward_Event_Key],
            g.[Reward_Event_Name],
            g.[Event_Date],
            g.[Last_Tournament_Date],
            g.[Tournament_Section_Count],
            g.[Tournament_Codes],
            CAST(COALESCE(gc.[Rated_Game_Count], 0) AS int) AS [Rated_Game_Count],
            CAST(g.[Is_State_Championship] AS bit) AS [Is_State_Championship],
            CAST(
                CASE
                    WHEN COALESCE(gc.[Rated_Game_Count], 0) <= @MinGames THEN 0
                    WHEN COALESCE(gc.[Rated_Game_Count], 0) >= @MaxGames THEN @MaxSupport * 1000
                    ELSE ROUND(
                        @MaxSupport
                        * POWER((COALESCE(gc.[Rated_Game_Count], 0) - @MinGames) * 1.0 / NULLIF(@MaxGames - @MinGames, 0), @Exponent)
                        * 1000,
                        0
                    )
                END
                AS int
            ) AS [Host_Award_Points],
            CAST(CASE WHEN g.[Is_State_Championship] = 1 THEN @StateChampionshipPoints ELSE 0 END AS int) AS [State_Championship_Points]
        FROM [groups] AS g
        LEFT JOIN [game_counts] AS gc
            ON gc.[Host_ChapterID] = g.[Host_ChapterID]
           AND gc.[Reward_Event_Key] = g.[Reward_Event_Key]
        WHERE (@TournamentDateFrom IS NULL OR g.[Event_Date] >= @TournamentDateFrom)
          AND g.[Event_Date] <= @TournamentDateTo
    )
    SELECT
        scored.[Host_ChapterID],
        scored.[Host_ChapterCode],
        scored.[Host_ChapterName],
        scored.[Reward_Event_Key],
        scored.[Reward_Event_Name],
        scored.[Event_Date],
        scored.[Last_Tournament_Date],
        scored.[Tournament_Section_Count],
        scored.[Tournament_Codes],
        scored.[Rated_Game_Count],
        scored.[Is_State_Championship],
        scored.[Host_Award_Points],
        scored.[State_Championship_Points],
        CONCAT(CONVERT(nvarchar(32), scored.[Host_ChapterID]), N':', scored.[Reward_Event_Key]) AS [Source_Base_Key],
        CONCAT(CONVERT(nvarchar(32), scored.[Host_ChapterID]), N':', scored.[Reward_Event_Key], N':points:', CONVERT(nvarchar(32), scored.[Host_Award_Points])) AS [Host_Source_Key],
        CONCAT(CONVERT(nvarchar(32), scored.[Host_ChapterID]), N':', scored.[Reward_Event_Key], N':points:', CONVERT(nvarchar(32), scored.[State_Championship_Points])) AS [State_Source_Key],
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
    INTO #TournamentAwardGroups
    FROM [scored] AS scored
    OUTER APPLY
    (
        SELECT SUM(t.[Points_Delta]) AS [Existing_Points]
        FROM [rewards].[transactions] AS t
        WHERE t.[Source_Type] = @HostSourceType
          AND t.[Transaction_Type] = N'earn'
          AND t.[ChapterID] = scored.[Host_ChapterID]
          AND LEFT(t.[Source_Key], LEN(CONCAT(CONVERT(nvarchar(32), scored.[Host_ChapterID]), N':', scored.[Reward_Event_Key], N':points:')))
              = CONCAT(CONVERT(nvarchar(32), scored.[Host_ChapterID]), N':', scored.[Reward_Event_Key], N':points:')
    ) AS host_existing
    OUTER APPLY
    (
        SELECT TOP 1 t.[TransactionID]
        FROM [rewards].[transactions] AS t
        WHERE t.[Source_Type] = @HostSourceType
          AND t.[Source_Key] = CONCAT(CONVERT(nvarchar(32), scored.[Host_ChapterID]), N':', scored.[Reward_Event_Key], N':points:', CONVERT(nvarchar(32), scored.[Host_Award_Points]))
          AND t.[Transaction_Type] = N'earn'
          AND t.[ChapterID] = scored.[Host_ChapterID]
        ORDER BY t.[TransactionID]
    ) AS host_current
    OUTER APPLY
    (
        SELECT SUM(t.[Points_Delta]) AS [Existing_Points]
        FROM [rewards].[transactions] AS t
        WHERE t.[Source_Type] = @StateSourceType
          AND t.[Transaction_Type] = N'earn'
          AND t.[ChapterID] = scored.[Host_ChapterID]
          AND LEFT(t.[Source_Key], LEN(CONCAT(CONVERT(nvarchar(32), scored.[Host_ChapterID]), N':', scored.[Reward_Event_Key], N':points:')))
              = CONCAT(CONVERT(nvarchar(32), scored.[Host_ChapterID]), N':', scored.[Reward_Event_Key], N':points:')
    ) AS state_existing
    OUTER APPLY
    (
        SELECT TOP 1 t.[TransactionID]
        FROM [rewards].[transactions] AS t
        WHERE t.[Source_Type] = @StateSourceType
          AND t.[Source_Key] = CONCAT(CONVERT(nvarchar(32), scored.[Host_ChapterID]), N':', scored.[Reward_Event_Key], N':points:', CONVERT(nvarchar(32), scored.[State_Championship_Points]))
          AND t.[Transaction_Type] = N'earn'
          AND t.[ChapterID] = scored.[Host_ChapterID]
        ORDER BY t.[TransactionID]
    ) AS state_current;

    DECLARE @MissingHostChapterCount int =
    (
        SELECT COUNT(*)
        FROM [ratings].[tournaments] AS t
        WHERE t.[Tournament_Date] IS NOT NULL
          AND (@TournamentDateFrom IS NULL OR CAST(t.[Tournament_Date] AS date) >= @TournamentDateFrom)
          AND CAST(t.[Tournament_Date] AS date) <= @TournamentDateTo
          AND (t.[Host_ChapterID] IS NULL OR NULLIF(LTRIM(RTRIM(t.[Host_ChapterCode])), N'') IS NULL)
    );

    DECLARE @MissingRewardEventKeyCount int =
    (
        SELECT COUNT(*)
        FROM [ratings].[tournaments] AS t
        WHERE t.[Tournament_Date] IS NOT NULL
          AND (@TournamentDateFrom IS NULL OR CAST(t.[Tournament_Date] AS date) >= @TournamentDateFrom)
          AND CAST(t.[Tournament_Date] AS date) <= @TournamentDateTo
          AND COALESCE(NULLIF(LTRIM(RTRIM(t.[Reward_Event_Key])), N''), NULLIF(LTRIM(RTRIM(t.[Tournament_Code])), N'')) IS NULL
    );

    DECLARE @EventGroupCount int = (SELECT COUNT(*) FROM #TournamentAwardGroups);
    DECLARE @TournamentSectionCount int = COALESCE((SELECT SUM([Tournament_Section_Count]) FROM #TournamentAwardGroups), 0);
    DECLARE @RatedGameCount int = COALESCE((SELECT SUM([Rated_Game_Count]) FROM #TournamentAwardGroups), 0);
    DECLARE @HostEligibleAwardCount int = (SELECT COUNT(*) FROM #TournamentAwardGroups WHERE [Host_Award_Points] > 0);
    DECLARE @HostAlreadyAwardedCount int = (SELECT COUNT(*) FROM #TournamentAwardGroups WHERE [Host_Award_Points] > 0 AND [Host_Existing_Points] >= [Host_Award_Points]);
    DECLARE @HostNewAwardCount int = (SELECT COUNT(*) FROM #TournamentAwardGroups WHERE [Host_New_Points] > 0);
    DECLARE @HostPointTotal int = COALESCE((SELECT SUM([Host_New_Points]) FROM #TournamentAwardGroups), 0);
    DECLARE @StateChampionshipGroupCount int = (SELECT COUNT(*) FROM #TournamentAwardGroups WHERE [State_Championship_Points] > 0);
    DECLARE @StateAlreadyAwardedCount int = (SELECT COUNT(*) FROM #TournamentAwardGroups WHERE [State_Championship_Points] > 0 AND [State_Existing_Points] >= [State_Championship_Points]);
    DECLARE @StateNewAwardCount int = (SELECT COUNT(*) FROM #TournamentAwardGroups WHERE [State_New_Points] > 0);
    DECLARE @StateChampionshipPointTotal int = COALESCE((SELECT SUM([State_New_Points]) FROM #TournamentAwardGroups), 0);
    DECLARE @NewAwardCount int = @HostNewAwardCount + @StateNewAwardCount;
    DECLARE @PointTotal int = @HostPointTotal + @StateChampionshipPointTotal;

    IF @DryRun = 1
    BEGIN
        SELECT
            CAST(NULL AS int) AS [RunID],
            @TournamentDateFrom AS [TournamentDateFrom],
            @TournamentDateTo AS [TournamentDateTo],
            CAST(1 AS bit) AS [DryRun],
            @EventGroupCount AS [EventGroupCount],
            @TournamentSectionCount AS [TournamentSectionCount],
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
            @PointTotal AS [PointTotal],
            @MissingHostChapterCount AS [MissingHostChapterCount],
            @MissingRewardEventKeyCount AS [MissingRewardEventKeyCount];

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
        @TournamentDateTo,
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
        awards.[Host_ChapterID],
        awards.[Host_ChapterCode],
        N'earn',
        awards.[New_Points],
        awards.[Desired_Points],
        NULL,
        NULL,
        awards.[Event_Date],
        awards.[Event_Date],
        awards.[Event_Date],
        SYSUTCDATETIME(),
        @RunID,
        awards.[Source_Type],
        awards.[Source_Key],
        @RuleVersion,
        (
            SELECT
                awards.[Award_Type] AS [award_type],
                awards.[Reward_Event_Key] AS [reward_event_key],
                awards.[Reward_Event_Name] AS [reward_event_name],
                awards.[Host_ChapterID] AS [host_chapter_id],
                awards.[Host_ChapterCode] AS [host_chapter_code],
                awards.[Host_ChapterName] AS [host_chapter_name],
                awards.[Tournament_Codes] AS [tournament_codes],
                awards.[Tournament_Section_Count] AS [tournament_section_count],
                awards.[Rated_Game_Count] AS [rated_game_count],
                awards.[Desired_Points] AS [desired_points],
                awards.[Existing_Points] AS [existing_points],
                awards.[New_Points] AS [new_points],
                awards.[Is_State_Championship] AS [is_state_championship],
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
            [Host_ChapterID],
            [Host_ChapterCode],
            [Host_ChapterName],
            [Reward_Event_Key],
            [Reward_Event_Name],
            [Event_Date],
            [Tournament_Codes],
            [Tournament_Section_Count],
            [Rated_Game_Count],
            [Is_State_Championship],
            [Host_Award_Points] AS [Desired_Points],
            [Host_Existing_Points] AS [Existing_Points],
            [Host_New_Points] AS [New_Points]
        FROM #TournamentAwardGroups
        WHERE [Host_New_Points] > 0
        UNION ALL
        SELECT
            N'state_championship' AS [Award_Type],
            @StateSourceType AS [Source_Type],
            [State_Source_Key] AS [Source_Key],
            [Host_ChapterID],
            [Host_ChapterCode],
            [Host_ChapterName],
            [Reward_Event_Key],
            [Reward_Event_Name],
            [Event_Date],
            [Tournament_Codes],
            [Tournament_Section_Count],
            [Rated_Game_Count],
            [Is_State_Championship],
            [State_Championship_Points] AS [Desired_Points],
            [State_Existing_Points] AS [Existing_Points],
            [State_New_Points] AS [New_Points]
        FROM #TournamentAwardGroups
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
                N'tournament_awards' AS [processor],
                @EventGroupCount AS [event_group_count],
                @TournamentSectionCount AS [tournament_section_count],
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
                @MissingHostChapterCount AS [missing_host_chapter_count],
                @MissingRewardEventKeyCount AS [missing_reward_event_key_count],
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
        @TournamentDateFrom AS [TournamentDateFrom],
        @TournamentDateTo AS [TournamentDateTo],
        CAST(0 AS bit) AS [DryRun],
        @EventGroupCount AS [EventGroupCount],
        @TournamentSectionCount AS [TournamentSectionCount],
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
        @PointTotal AS [PointTotal],
        @MissingHostChapterCount AS [MissingHostChapterCount],
        @MissingRewardEventKeyCount AS [MissingRewardEventKeyCount];
END;
GO
