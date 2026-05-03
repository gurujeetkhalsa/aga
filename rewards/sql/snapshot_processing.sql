SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'rewards') IS NULL
    EXEC(N'CREATE SCHEMA [rewards]');
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_create_daily_snapshot]
    @SnapshotDate date = NULL,
    @RunType nvarchar(32) = N'daily',
    @ReplaceExisting bit = 0,
    @MaxMemberAGAID int = 50000
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'rewards.reward_runs', N'U') IS NULL
       OR OBJECT_ID(N'rewards.member_daily_snapshot', N'U') IS NULL
       OR OBJECT_ID(N'rewards.chapter_daily_snapshot', N'U') IS NULL
    BEGIN
        THROW 52200, N'Rewards snapshot tables do not exist. Apply rewards/sql/chapter_rewards_schema.sql first.', 1;
    END;

    SET @SnapshotDate = COALESCE(@SnapshotDate, CAST(SYSUTCDATETIME() AS date));

    IF @RunType NOT IN (N'daily', N'manual', N'import', N'backfill')
    BEGIN
        THROW 52201, N'Unsupported rewards snapshot run type.', 1;
    END;

    IF @ReplaceExisting = 0
       AND
       (
           EXISTS (SELECT 1 FROM [rewards].[member_daily_snapshot] WHERE [Snapshot_Date] = @SnapshotDate)
           OR EXISTS (SELECT 1 FROM [rewards].[chapter_daily_snapshot] WHERE [Snapshot_Date] = @SnapshotDate)
       )
    BEGIN
        SELECT TOP 1
            r.[RunID],
            @SnapshotDate AS [SnapshotDate],
            CAST(1 AS bit) AS [AlreadyExisted],
            (SELECT COUNT(*) FROM [rewards].[member_daily_snapshot] WHERE [Snapshot_Date] = @SnapshotDate) AS [MemberSnapshotCount],
            (SELECT COUNT(*) FROM [rewards].[member_daily_snapshot] WHERE [Snapshot_Date] = @SnapshotDate AND [Is_Active] = 1) AS [ActiveMemberCount],
            (SELECT COUNT(*) FROM [rewards].[member_daily_snapshot] WHERE [Snapshot_Date] = @SnapshotDate AND [Is_Tournament_Pass] = 1) AS [TournamentPassCount],
            (SELECT COUNT(*) FROM [rewards].[chapter_daily_snapshot] WHERE [Snapshot_Date] = @SnapshotDate) AS [ChapterSnapshotCount],
            (SELECT COUNT(*) FROM [rewards].[chapter_daily_snapshot] WHERE [Snapshot_Date] = @SnapshotDate AND [Is_Current] = 1) AS [CurrentChapterCount],
            (SELECT COUNT(*) FROM [rewards].[chapter_daily_snapshot] WHERE [Snapshot_Date] = @SnapshotDate AND [Multiplier] = 1) AS [Multiplier1ChapterCount],
            (SELECT COUNT(*) FROM [rewards].[chapter_daily_snapshot] WHERE [Snapshot_Date] = @SnapshotDate AND [Multiplier] = 2) AS [Multiplier2ChapterCount],
            (SELECT COUNT(*) FROM [rewards].[chapter_daily_snapshot] WHERE [Snapshot_Date] = @SnapshotDate AND [Multiplier] = 3) AS [Multiplier3ChapterCount]
        FROM [rewards].[reward_runs] AS r
        WHERE r.[Snapshot_Date] = @SnapshotDate
        ORDER BY r.[RunID] DESC;

        RETURN;
    END;

    IF @ReplaceExisting = 1
    BEGIN
        DELETE FROM [rewards].[chapter_daily_snapshot]
        WHERE [Snapshot_Date] = @SnapshotDate;

        DELETE FROM [rewards].[member_daily_snapshot]
        WHERE [Snapshot_Date] = @SnapshotDate;
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
        @SnapshotDate,
        SYSUTCDATETIME(),
        N'running',
        NULL
    );

    DECLARE @RunID int = (SELECT TOP 1 [RunID] FROM @InsertedRun);

    INSERT INTO [rewards].[member_daily_snapshot]
    (
        [Snapshot_Date],
        [AGAID],
        [Member_Type],
        [Expiration_Date],
        [ChapterID],
        [Chapter_Code],
        [Is_Active],
        [Is_Tournament_Pass],
        [Created_RunID]
    )
    SELECT
        @SnapshotDate,
        m.[AGAID],
        m.[MemberType],
        CAST(m.[ExpirationDate] AS date),
        m.[ChapterID],
        c.[ChapterCode],
        CASE
            WHEN m.[ExpirationDate] IS NOT NULL
             AND CAST(m.[ExpirationDate] AS date) >= @SnapshotDate
                THEN 1
            ELSE 0
        END,
        CASE
            WHEN LTRIM(RTRIM(COALESCE(m.[MemberType], N''))) = N'Tournament Pass'
                THEN 1
            ELSE 0
        END,
        @RunID
    FROM [membership].[members] AS m
    LEFT JOIN [membership].[chapters] AS c
        ON c.[ChapterID] = m.[ChapterID]
    WHERE m.[AGAID] < @MaxMemberAGAID;

    DECLARE @MemberSnapshotCount int = @@ROWCOUNT;
    DECLARE @ActiveMemberCount int =
    (
        SELECT COUNT(*)
        FROM [rewards].[member_daily_snapshot]
        WHERE [Snapshot_Date] = @SnapshotDate
          AND [Is_Active] = 1
    );
    DECLARE @TournamentPassCount int =
    (
        SELECT COUNT(*)
        FROM [rewards].[member_daily_snapshot]
        WHERE [Snapshot_Date] = @SnapshotDate
          AND [Is_Tournament_Pass] = 1
    );

    ;WITH [chapter_counts] AS
    (
        SELECT
            [ChapterID],
            COUNT(*) AS [Active_Member_Count]
        FROM [rewards].[member_daily_snapshot]
        WHERE [Snapshot_Date] = @SnapshotDate
          AND [ChapterID] IS NOT NULL
          AND [Is_Active] = 1
          AND [Is_Tournament_Pass] = 0
        GROUP BY [ChapterID]
    )
    INSERT INTO [rewards].[chapter_daily_snapshot]
    (
        [Snapshot_Date],
        [ChapterID],
        [Chapter_Code],
        [Chapter_Name],
        [Is_Current],
        [Active_Member_Count],
        [Multiplier],
        [Created_RunID]
    )
    SELECT
        @SnapshotDate,
        c.[ChapterID],
        c.[ChapterCode],
        c.[ChapterName],
        COALESCE(eligibility.[Is_Current], 1),
        COALESCE(counts.[Active_Member_Count], 0),
        CASE
            WHEN COALESCE(counts.[Active_Member_Count], 0) < 5 THEN 3
            WHEN COALESCE(counts.[Active_Member_Count], 0) BETWEEN 5 AND 9 THEN 2
            ELSE 1
        END,
        @RunID
    FROM [membership].[chapters] AS c
    LEFT JOIN [chapter_counts] AS counts
        ON counts.[ChapterID] = c.[ChapterID]
    OUTER APPLY
    (
        SELECT TOP 1
            periods.[Is_Current]
        FROM [rewards].[chapter_eligibility_periods] AS periods
        WHERE periods.[Effective_Start_Date] <= @SnapshotDate
          AND (periods.[Effective_End_Date] IS NULL OR periods.[Effective_End_Date] >= @SnapshotDate)
          AND
          (
              (periods.[ChapterID] IS NOT NULL AND periods.[ChapterID] = c.[ChapterID])
              OR (periods.[ChapterID] IS NULL AND periods.[Chapter_Code] = c.[ChapterCode])
          )
        ORDER BY
            CASE WHEN periods.[ChapterID] = c.[ChapterID] THEN 0 ELSE 1 END,
            periods.[Effective_Start_Date] DESC,
            periods.[Chapter_Eligibility_Period_ID] DESC
    ) AS eligibility
    WHERE c.[ChapterID] IS NOT NULL
      AND LTRIM(RTRIM(COALESCE(c.[ChapterCode], N''))) <> N'';

    DECLARE @ChapterSnapshotCount int = @@ROWCOUNT;
    DECLARE @CurrentChapterCount int =
    (
        SELECT COUNT(*)
        FROM [rewards].[chapter_daily_snapshot]
        WHERE [Snapshot_Date] = @SnapshotDate
          AND [Is_Current] = 1
    );
    DECLARE @Multiplier1ChapterCount int =
    (
        SELECT COUNT(*)
        FROM [rewards].[chapter_daily_snapshot]
        WHERE [Snapshot_Date] = @SnapshotDate
          AND [Multiplier] = 1
    );
    DECLARE @Multiplier2ChapterCount int =
    (
        SELECT COUNT(*)
        FROM [rewards].[chapter_daily_snapshot]
        WHERE [Snapshot_Date] = @SnapshotDate
          AND [Multiplier] = 2
    );
    DECLARE @Multiplier3ChapterCount int =
    (
        SELECT COUNT(*)
        FROM [rewards].[chapter_daily_snapshot]
        WHERE [Snapshot_Date] = @SnapshotDate
          AND [Multiplier] = 3
    );

    UPDATE [rewards].[reward_runs]
    SET
        [Completed_At] = SYSUTCDATETIME(),
        [Status] = N'succeeded',
        [SummaryJson] =
        (
            SELECT
                @MemberSnapshotCount AS [member_snapshot_count],
                @ActiveMemberCount AS [active_member_count],
                @TournamentPassCount AS [tournament_pass_count],
                @ChapterSnapshotCount AS [chapter_snapshot_count],
                @CurrentChapterCount AS [current_chapter_count],
                @Multiplier1ChapterCount AS [multiplier_1_chapter_count],
                @Multiplier2ChapterCount AS [multiplier_2_chapter_count],
                @Multiplier3ChapterCount AS [multiplier_3_chapter_count]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    WHERE [RunID] = @RunID;

    SELECT
        @RunID AS [RunID],
        @SnapshotDate AS [SnapshotDate],
        CAST(0 AS bit) AS [AlreadyExisted],
        @MemberSnapshotCount AS [MemberSnapshotCount],
        @ActiveMemberCount AS [ActiveMemberCount],
        @TournamentPassCount AS [TournamentPassCount],
        @ChapterSnapshotCount AS [ChapterSnapshotCount],
        @CurrentChapterCount AS [CurrentChapterCount],
        @Multiplier1ChapterCount AS [Multiplier1ChapterCount],
        @Multiplier2ChapterCount AS [Multiplier2ChapterCount],
        @Multiplier3ChapterCount AS [Multiplier3ChapterCount];
END;
GO
