-- Copyright 2026, American Go Association, All rights reserved

-- Live Azure SQL stored procedure export.
-- Source object: [rewards].[sp_process_legacy_gap_rated_game_awards].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_process_legacy_gap_rated_game_awards]
    @GameIDFrom int,
    @GameIDTo int,
    @SourceAsOfDate date = '2026-02-08',
    @LedgerStartDate date = '2026-05-02',
    @ValuationSnapshotDate date = '2026-05-02',
    @RunType nvarchar(32) = N'import',
    @DryRun bit = 0,
    @MaxMemberAGAID int = 50000,
    @BasePoints int = 500,
    @SourceType nvarchar(64) = N'rated_game_participation',
    @RuleVersion nvarchar(32) = N'2026-05-02'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'rewards.member_daily_snapshot', N'U') IS NULL
       OR OBJECT_ID(N'rewards.chapter_daily_snapshot', N'U') IS NULL
       OR OBJECT_ID(N'rewards.transactions', N'U') IS NULL
       OR OBJECT_ID(N'rewards.point_lots', N'U') IS NULL
    BEGIN
        THROW 52900, N'Rewards rated-game award tables do not exist. Apply rewards/sql/chapter_rewards_schema.sql first.', 1;
    END;

    IF @GameIDFrom IS NULL OR @GameIDTo IS NULL
        THROW 52901, N'GameIDFrom and GameIDTo are required.', 1;

    IF @GameIDTo < @GameIDFrom
        THROW 52902, N'GameIDTo must be on or after GameIDFrom.', 1;

    IF @LedgerStartDate < @SourceAsOfDate
        THROW 52903, N'LedgerStartDate must be on or after SourceAsOfDate.', 1;

    IF @RunType NOT IN (N'daily', N'manual', N'import', N'backfill')
        THROW 52904, N'Unsupported rewards rated-game award run type.', 1;

    IF NOT EXISTS (SELECT 1 FROM [rewards].[member_daily_snapshot] WHERE [Snapshot_Date] = @ValuationSnapshotDate)
        THROW 52905, N'Member valuation snapshot does not exist.', 1;

    IF NOT EXISTS (SELECT 1 FROM [rewards].[chapter_daily_snapshot] WHERE [Snapshot_Date] = @ValuationSnapshotDate)
        THROW 52906, N'Chapter valuation snapshot does not exist.', 1;

    IF OBJECT_ID(N'tempdb..#LegacyRatedGameAwards', N'U') IS NOT NULL
        DROP TABLE #LegacyRatedGameAwards;

    ;WITH [game_participants] AS
    (
        SELECT
            g.[Game_ID],
            g.[Tournament_Code],
            CAST(g.[Game_Date] AS date) AS [Game_Date],
            g.[Pin_Player_1] AS [AGAID],
            g.[Pin_Player_2] AS [Opponent_AGAID],
            g.[Color_1] AS [Color],
            1 AS [Player_Slot]
        FROM [ratings].[games] AS g
        WHERE g.[Game_ID] IS NOT NULL
          AND g.[Game_ID] >= @GameIDFrom
          AND g.[Game_ID] <= @GameIDTo
          AND g.[Game_Date] IS NOT NULL
          AND CAST(g.[Game_Date] AS date) > @SourceAsOfDate
          AND CAST(g.[Game_Date] AS date) <= @LedgerStartDate
          AND COALESCE(g.[Rated], 1) = 1
          AND COALESCE(g.[Online], 0) = 0
          AND COALESCE(g.[Exclude], 0) = 0
          AND g.[Pin_Player_1] IS NOT NULL
          AND g.[Pin_Player_1] < @MaxMemberAGAID
        UNION ALL
        SELECT
            g.[Game_ID],
            g.[Tournament_Code],
            CAST(g.[Game_Date] AS date) AS [Game_Date],
            g.[Pin_Player_2] AS [AGAID],
            g.[Pin_Player_1] AS [Opponent_AGAID],
            g.[Color_2] AS [Color],
            2 AS [Player_Slot]
        FROM [ratings].[games] AS g
        WHERE g.[Game_ID] IS NOT NULL
          AND g.[Game_ID] >= @GameIDFrom
          AND g.[Game_ID] <= @GameIDTo
          AND g.[Game_Date] IS NOT NULL
          AND CAST(g.[Game_Date] AS date) > @SourceAsOfDate
          AND CAST(g.[Game_Date] AS date) <= @LedgerStartDate
          AND COALESCE(g.[Rated], 1) = 1
          AND COALESCE(g.[Online], 0) = 0
          AND COALESCE(g.[Exclude], 0) = 0
          AND g.[Pin_Player_2] IS NOT NULL
          AND g.[Pin_Player_2] < @MaxMemberAGAID
    ),
    [award_candidates] AS
    (
        SELECT
            gp.[Game_ID],
            gp.[Tournament_Code],
            gp.[Game_Date],
            gp.[AGAID],
            gp.[Opponent_AGAID],
            gp.[Color],
            gp.[Player_Slot],
            ms.[Member_Type],
            ms.[ChapterID],
            ms.[Chapter_Code],
            ms.[Is_Active],
            ms.[Is_Tournament_Pass],
            cs.[Is_Current],
            cs.[Active_Member_Count],
            cs.[Multiplier],
            CASE
                WHEN ms.[AGAID] IS NULL THEN N'missing_member_snapshot'
                WHEN ms.[Is_Active] <> 1 THEN N'inactive_player'
                WHEN ms.[ChapterID] IS NULL THEN N'no_chapter'
                WHEN cs.[ChapterID] IS NULL THEN N'missing_chapter_snapshot'
                WHEN cs.[Is_Current] <> 1 THEN N'chapter_not_current'
                ELSE N'eligible'
            END AS [Eligibility_Status],
            CONCAT(@SourceType, N':', CONVERT(nvarchar(32), gp.[Game_ID]), N':', CONVERT(nvarchar(32), gp.[AGAID])) AS [Source_Key]
        FROM [game_participants] AS gp
        LEFT JOIN [rewards].[member_daily_snapshot] AS ms
            ON ms.[Snapshot_Date] = @ValuationSnapshotDate
           AND ms.[AGAID] = gp.[AGAID]
        LEFT JOIN [rewards].[chapter_daily_snapshot] AS cs
            ON cs.[Snapshot_Date] = @ValuationSnapshotDate
           AND cs.[ChapterID] = ms.[ChapterID]
    )
    SELECT
        candidates.[Game_ID],
        candidates.[Tournament_Code],
        candidates.[Game_Date],
        candidates.[AGAID],
        candidates.[Opponent_AGAID],
        candidates.[Color],
        candidates.[Player_Slot],
        candidates.[Member_Type],
        candidates.[ChapterID],
        candidates.[Chapter_Code],
        candidates.[Is_Active],
        candidates.[Is_Tournament_Pass],
        candidates.[Is_Current],
        candidates.[Active_Member_Count],
        candidates.[Multiplier],
        candidates.[Eligibility_Status],
        candidates.[Source_Key],
        existing.[TransactionID] AS [Already_TransactionID],
        CASE
            WHEN candidates.[Eligibility_Status] = N'eligible'
                THEN @BasePoints * candidates.[Multiplier]
            ELSE 0
        END AS [Points]
    INTO #LegacyRatedGameAwards
    FROM [award_candidates] AS candidates
    OUTER APPLY
    (
        SELECT TOP 1
            t.[TransactionID]
        FROM [rewards].[transactions] AS t
        WHERE t.[Source_Type] = @SourceType
          AND t.[Source_Key] = candidates.[Source_Key]
          AND t.[Transaction_Type] = N'earn'
        ORDER BY t.[TransactionID]
    ) AS existing;

    DECLARE @GameCount int = (SELECT COUNT(DISTINCT [Game_ID]) FROM #LegacyRatedGameAwards);
    DECLARE @ParticipantCount int = (SELECT COUNT(*) FROM #LegacyRatedGameAwards);
    DECLARE @EligibleAwardCount int = (SELECT COUNT(*) FROM #LegacyRatedGameAwards WHERE [Eligibility_Status] = N'eligible');
    DECLARE @AlreadyAwardedCount int = (SELECT COUNT(*) FROM #LegacyRatedGameAwards WHERE [Eligibility_Status] = N'eligible' AND [Already_TransactionID] IS NOT NULL);
    DECLARE @NewAwardCount int = (SELECT COUNT(*) FROM #LegacyRatedGameAwards WHERE [Eligibility_Status] = N'eligible' AND [Already_TransactionID] IS NULL);
    DECLARE @PointTotal int = COALESCE((SELECT SUM([Points]) FROM #LegacyRatedGameAwards WHERE [Eligibility_Status] = N'eligible' AND [Already_TransactionID] IS NULL), 0);
    DECLARE @MissingMemberSnapshotCount int = (SELECT COUNT(*) FROM #LegacyRatedGameAwards WHERE [Eligibility_Status] = N'missing_member_snapshot');
    DECLARE @MissingChapterSnapshotCount int = (SELECT COUNT(*) FROM #LegacyRatedGameAwards WHERE [Eligibility_Status] = N'missing_chapter_snapshot');
    DECLARE @InactivePlayerCount int = (SELECT COUNT(*) FROM #LegacyRatedGameAwards WHERE [Eligibility_Status] = N'inactive_player');
    DECLARE @NoChapterCount int = (SELECT COUNT(*) FROM #LegacyRatedGameAwards WHERE [Eligibility_Status] = N'no_chapter');
    DECLARE @ChapterNotCurrentCount int = (SELECT COUNT(*) FROM #LegacyRatedGameAwards WHERE [Eligibility_Status] = N'chapter_not_current');

    IF @DryRun = 1
    BEGIN
        SELECT
            CAST(NULL AS int) AS [RunID],
            CAST(1 AS bit) AS [DryRun],
            @GameIDFrom AS [GameIDFrom],
            @GameIDTo AS [GameIDTo],
            @SourceAsOfDate AS [SourceAsOfDate],
            @LedgerStartDate AS [LedgerStartDate],
            @ValuationSnapshotDate AS [ValuationSnapshotDate],
            @GameCount AS [GameCount],
            @ParticipantCount AS [ParticipantCount],
            @EligibleAwardCount AS [EligibleAwardCount],
            @AlreadyAwardedCount AS [AlreadyAwardedCount],
            @NewAwardCount AS [NewAwardCount],
            @PointTotal AS [PointTotal],
            @MissingMemberSnapshotCount AS [MissingMemberSnapshotCount],
            @MissingChapterSnapshotCount AS [MissingChapterSnapshotCount],
            @InactivePlayerCount AS [InactivePlayerCount],
            @NoChapterCount AS [NoChapterCount],
            @ChapterNotCurrentCount AS [ChapterNotCurrentCount];

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
        awards.[Points],
        @BasePoints,
        awards.[Multiplier],
        awards.[Active_Member_Count],
        awards.[Game_Date],
        awards.[Game_Date],
        @ValuationSnapshotDate,
        SYSUTCDATETIME(),
        @RunID,
        @SourceType,
        awards.[Source_Key],
        @RuleVersion,
        (
            SELECT
                CAST(1 AS bit) AS [legacy_gap],
                @SourceAsOfDate AS [source_balance_as_of_date],
                @LedgerStartDate AS [ledger_start_date],
                @ValuationSnapshotDate AS [valuation_snapshot_date],
                awards.[Game_ID] AS [game_id],
                awards.[Tournament_Code] AS [tournament_code],
                awards.[Game_Date] AS [game_date],
                awards.[AGAID] AS [agaid],
                awards.[Opponent_AGAID] AS [opponent_agaid],
                awards.[Color] AS [color],
                awards.[Player_Slot] AS [player_slot],
                awards.[Member_Type] AS [member_type],
                awards.[ChapterID] AS [chapter_id],
                awards.[Chapter_Code] AS [chapter_code],
                awards.[Active_Member_Count] AS [chapter_active_member_count],
                awards.[Multiplier] AS [multiplier],
                @BasePoints AS [base_points],
                awards.[Points] AS [points]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM #LegacyRatedGameAwards AS awards
    WHERE awards.[Eligibility_Status] = N'eligible'
      AND awards.[Already_TransactionID] IS NULL;

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
                @SourceType AS [processor],
                CAST(1 AS bit) AS [legacy_gap],
                @GameIDFrom AS [game_id_from],
                @GameIDTo AS [game_id_to],
                @SourceAsOfDate AS [source_balance_as_of_date],
                @LedgerStartDate AS [ledger_start_date],
                @ValuationSnapshotDate AS [valuation_snapshot_date],
                @GameCount AS [game_count],
                @ParticipantCount AS [participant_count],
                @EligibleAwardCount AS [eligible_award_count],
                @AlreadyAwardedCount AS [already_awarded_count],
                @InsertedAwardCount AS [new_award_count],
                @PointTotal AS [point_total],
                @MissingMemberSnapshotCount AS [missing_member_snapshot_count],
                @MissingChapterSnapshotCount AS [missing_chapter_snapshot_count],
                @InactivePlayerCount AS [inactive_player_count],
                @NoChapterCount AS [no_chapter_count],
                @ChapterNotCurrentCount AS [chapter_not_current_count]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    WHERE [RunID] = @RunID;

    SELECT
        @RunID AS [RunID],
        CAST(0 AS bit) AS [DryRun],
        @GameIDFrom AS [GameIDFrom],
        @GameIDTo AS [GameIDTo],
        @SourceAsOfDate AS [SourceAsOfDate],
        @LedgerStartDate AS [LedgerStartDate],
        @ValuationSnapshotDate AS [ValuationSnapshotDate],
        @GameCount AS [GameCount],
        @ParticipantCount AS [ParticipantCount],
        @EligibleAwardCount AS [EligibleAwardCount],
        @AlreadyAwardedCount AS [AlreadyAwardedCount],
        @InsertedAwardCount AS [NewAwardCount],
        @PointTotal AS [PointTotal],
        @MissingMemberSnapshotCount AS [MissingMemberSnapshotCount],
        @MissingChapterSnapshotCount AS [MissingChapterSnapshotCount],
        @InactivePlayerCount AS [InactivePlayerCount],
        @NoChapterCount AS [NoChapterCount],
        @ChapterNotCurrentCount AS [ChapterNotCurrentCount];
END;
GO
