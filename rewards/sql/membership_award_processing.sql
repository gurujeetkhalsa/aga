SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'rewards') IS NULL
    EXEC(N'CREATE SCHEMA [rewards]');
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_process_membership_awards]
    @AsOfDate date = NULL,
    @RunType nvarchar(32) = N'daily',
    @DryRun bit = 0,
    @SourceType nvarchar(64) = N'membership_event',
    @RuleVersion nvarchar(32) = N'2026-05-02'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'rewards.membership_events', N'U') IS NULL
       OR OBJECT_ID(N'rewards.member_daily_snapshot', N'U') IS NULL
       OR OBJECT_ID(N'rewards.chapter_daily_snapshot', N'U') IS NULL
       OR OBJECT_ID(N'rewards.transactions', N'U') IS NULL
       OR OBJECT_ID(N'rewards.point_lots', N'U') IS NULL
    BEGIN
        THROW 52300, N'Rewards membership award tables do not exist. Apply rewards/sql/chapter_rewards_schema.sql first.', 1;
    END;

    SET @AsOfDate = COALESCE(@AsOfDate, CAST(SYSUTCDATETIME() AS date));

    IF @RunType NOT IN (N'daily', N'manual', N'import', N'backfill')
    BEGIN
        THROW 52301, N'Unsupported rewards membership award run type.', 1;
    END;

    IF OBJECT_ID(N'tempdb..#MembershipAwardDecisions', N'U') IS NOT NULL
    BEGIN
        DROP TABLE #MembershipAwardDecisions;
    END;

    ;WITH [pending_events] AS
    (
        SELECT
            e.[Membership_Event_ID],
            e.[Message_ID],
            e.[AGAID],
            e.[Event_Type],
            e.[Event_Date],
            e.[Credit_Deadline],
            e.[Member_Type],
            e.[Base_Points],
            CONVERT(nvarchar(64), e.[Membership_Event_ID]) AS [Source_Key]
        FROM [rewards].[membership_events] AS e
        WHERE e.[Status] = N'pending'
          AND e.[Event_Date] <= @AsOfDate
    ),
    [candidate_events] AS
    (
        SELECT
            e.[Membership_Event_ID],
            e.[Message_ID],
            e.[AGAID],
            e.[Event_Type],
            e.[Event_Date],
            e.[Credit_Deadline],
            e.[Member_Type],
            e.[Base_Points],
            e.[Source_Key],
            existing.[TransactionID] AS [Existing_TransactionID],
            deadline_snapshot.[Snapshot_Date] AS [Deadline_Snapshot_Date],
            eligibility.[Eligibility_Date],
            eligibility.[Snapshot_Member_Type],
            eligibility.[ChapterID],
            eligibility.[Chapter_Code],
            eligibility.[Chapter_Active_Member_Count],
            eligibility.[Multiplier]
        FROM [pending_events] AS e
        OUTER APPLY
        (
            SELECT TOP 1
                t.[TransactionID]
            FROM [rewards].[transactions] AS t
            WHERE t.[Source_Type] = @SourceType
              AND t.[Source_Key] = e.[Source_Key]
              AND t.[Transaction_Type] = N'earn'
            ORDER BY t.[TransactionID]
        ) AS existing
        OUTER APPLY
        (
            SELECT TOP 1
                ms.[Snapshot_Date]
            FROM [rewards].[member_daily_snapshot] AS ms
            WHERE ms.[Snapshot_Date] = e.[Credit_Deadline]
              AND ms.[AGAID] = e.[AGAID]
        ) AS deadline_snapshot
        OUTER APPLY
        (
            SELECT TOP 1
                ms.[Snapshot_Date] AS [Eligibility_Date],
                ms.[Member_Type] AS [Snapshot_Member_Type],
                cs.[ChapterID],
                cs.[Chapter_Code],
                cs.[Active_Member_Count] AS [Chapter_Active_Member_Count],
                cs.[Multiplier]
            FROM [rewards].[member_daily_snapshot] AS ms
            INNER JOIN [rewards].[chapter_daily_snapshot] AS cs
                ON cs.[Snapshot_Date] = ms.[Snapshot_Date]
               AND cs.[ChapterID] = ms.[ChapterID]
            WHERE ms.[AGAID] = e.[AGAID]
              AND ms.[Snapshot_Date] >= e.[Event_Date]
              AND ms.[Snapshot_Date] <=
                  CASE
                      WHEN e.[Credit_Deadline] < @AsOfDate THEN e.[Credit_Deadline]
                      ELSE @AsOfDate
                  END
              AND ms.[Is_Active] = 1
              AND ms.[ChapterID] IS NOT NULL
              AND cs.[Is_Current] = 1
            ORDER BY ms.[Snapshot_Date]
        ) AS eligibility
    )
    SELECT
        e.[Membership_Event_ID],
        e.[Message_ID],
        e.[AGAID],
        e.[Event_Type],
        e.[Event_Date],
        e.[Credit_Deadline],
        e.[Member_Type],
        e.[Base_Points],
        e.[Source_Key],
        e.[Existing_TransactionID],
        e.[Deadline_Snapshot_Date],
        e.[Eligibility_Date],
        e.[Snapshot_Member_Type],
        e.[ChapterID],
        e.[Chapter_Code],
        e.[Chapter_Active_Member_Count],
        e.[Multiplier],
        CASE
            WHEN e.[Base_Points] <= 0 THEN N'ineligible'
            WHEN e.[Existing_TransactionID] IS NOT NULL THEN N'already_awarded'
            WHEN e.[Eligibility_Date] IS NOT NULL THEN N'eligible'
            WHEN @AsOfDate > e.[Credit_Deadline] AND e.[Deadline_Snapshot_Date] IS NOT NULL THEN N'expire_no_chapter'
            WHEN @AsOfDate > e.[Credit_Deadline] AND e.[Deadline_Snapshot_Date] IS NULL THEN N'missing_snapshot_coverage'
            ELSE N'waiting_for_chapter'
        END AS [Decision],
        CASE
            WHEN e.[Base_Points] > 0
             AND e.[Existing_TransactionID] IS NULL
             AND e.[Eligibility_Date] IS NOT NULL
                THEN e.[Base_Points] * e.[Multiplier]
            ELSE 0
        END AS [Points]
    INTO #MembershipAwardDecisions
    FROM [candidate_events] AS e;

    DECLARE @PendingEventCount int = (SELECT COUNT(*) FROM #MembershipAwardDecisions);
    DECLARE @EligibleEventCount int = (SELECT COUNT(*) FROM #MembershipAwardDecisions WHERE [Decision] IN (N'eligible', N'already_awarded'));
    DECLARE @AlreadyAwardedCount int = (SELECT COUNT(*) FROM #MembershipAwardDecisions WHERE [Decision] = N'already_awarded');
    DECLARE @NewAwardCount int = (SELECT COUNT(*) FROM #MembershipAwardDecisions WHERE [Decision] = N'eligible');
    DECLARE @PointTotal int = COALESCE((SELECT SUM([Points]) FROM #MembershipAwardDecisions WHERE [Decision] = N'eligible'), 0);
    DECLARE @ExpiringNoChapterCount int = (SELECT COUNT(*) FROM #MembershipAwardDecisions WHERE [Decision] = N'expire_no_chapter');
    DECLARE @WaitingForChapterCount int = (SELECT COUNT(*) FROM #MembershipAwardDecisions WHERE [Decision] = N'waiting_for_chapter');
    DECLARE @MissingSnapshotCoverageCount int = (SELECT COUNT(*) FROM #MembershipAwardDecisions WHERE [Decision] = N'missing_snapshot_coverage');
    DECLARE @IneligibleCount int = (SELECT COUNT(*) FROM #MembershipAwardDecisions WHERE [Decision] = N'ineligible');

    IF @DryRun = 1
    BEGIN
        SELECT
            CAST(NULL AS int) AS [RunID],
            @AsOfDate AS [AsOfDate],
            CAST(1 AS bit) AS [DryRun],
            @PendingEventCount AS [PendingEventCount],
            @EligibleEventCount AS [EligibleEventCount],
            @AlreadyAwardedCount AS [AlreadyAwardedCount],
            @NewAwardCount AS [NewAwardCount],
            @PointTotal AS [PointTotal],
            @ExpiringNoChapterCount AS [ExpiringNoChapterCount],
            @WaitingForChapterCount AS [WaitingForChapterCount],
            @MissingSnapshotCoverageCount AS [MissingSnapshotCoverageCount],
            @IneligibleCount AS [IneligibleCount];

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
        @AsOfDate,
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
        decisions.[ChapterID],
        decisions.[Chapter_Code],
        N'earn',
        decisions.[Points],
        decisions.[Base_Points],
        decisions.[Multiplier],
        decisions.[Chapter_Active_Member_Count],
        decisions.[Eligibility_Date],
        decisions.[Eligibility_Date],
        decisions.[Eligibility_Date],
        SYSUTCDATETIME(),
        @RunID,
        @SourceType,
        decisions.[Source_Key],
        @RuleVersion,
        (
            SELECT
                decisions.[Membership_Event_ID] AS [membership_event_id],
                decisions.[Message_ID] AS [message_id],
                decisions.[AGAID] AS [agaid],
                decisions.[Event_Type] AS [event_type],
                decisions.[Event_Date] AS [event_date],
                decisions.[Credit_Deadline] AS [credit_deadline],
                decisions.[Member_Type] AS [event_member_type],
                decisions.[Snapshot_Member_Type] AS [snapshot_member_type],
                decisions.[ChapterID] AS [chapter_id],
                decisions.[Chapter_Code] AS [chapter_code],
                decisions.[Chapter_Active_Member_Count] AS [chapter_active_member_count],
                decisions.[Multiplier] AS [multiplier],
                decisions.[Base_Points] AS [base_points],
                decisions.[Points] AS [points]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM #MembershipAwardDecisions AS decisions
    WHERE decisions.[Decision] = N'eligible';

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

    UPDATE events
    SET
        [Status] = N'credited',
        [Credited_TransactionID] = tx.[TransactionID],
        [Updated_At] = SYSUTCDATETIME()
    FROM [rewards].[membership_events] AS events
    INNER JOIN #MembershipAwardDecisions AS decisions
        ON decisions.[Membership_Event_ID] = events.[Membership_Event_ID]
    INNER JOIN @InsertedTransactions AS tx
        ON tx.[Source_Key] = decisions.[Source_Key]
    WHERE decisions.[Decision] = N'eligible';

    UPDATE events
    SET
        [Status] = N'credited',
        [Credited_TransactionID] = decisions.[Existing_TransactionID],
        [Updated_At] = SYSUTCDATETIME()
    FROM [rewards].[membership_events] AS events
    INNER JOIN #MembershipAwardDecisions AS decisions
        ON decisions.[Membership_Event_ID] = events.[Membership_Event_ID]
    WHERE decisions.[Decision] = N'already_awarded';

    UPDATE events
    SET
        [Status] = N'expired_no_chapter',
        [Expired_At] = SYSUTCDATETIME(),
        [Updated_At] = SYSUTCDATETIME()
    FROM [rewards].[membership_events] AS events
    INNER JOIN #MembershipAwardDecisions AS decisions
        ON decisions.[Membership_Event_ID] = events.[Membership_Event_ID]
    WHERE decisions.[Decision] = N'expire_no_chapter';

    UPDATE events
    SET
        [Status] = N'ineligible',
        [Updated_At] = SYSUTCDATETIME()
    FROM [rewards].[membership_events] AS events
    INNER JOIN #MembershipAwardDecisions AS decisions
        ON decisions.[Membership_Event_ID] = events.[Membership_Event_ID]
    WHERE decisions.[Decision] = N'ineligible';

    UPDATE [rewards].[reward_runs]
    SET
        [Completed_At] = SYSUTCDATETIME(),
        [Status] = N'succeeded',
        [SummaryJson] =
        (
            SELECT
                @SourceType AS [processor],
                @PendingEventCount AS [pending_event_count],
                @EligibleEventCount AS [eligible_event_count],
                @AlreadyAwardedCount AS [already_awarded_count],
                @InsertedAwardCount AS [new_award_count],
                @PointTotal AS [point_total],
                @ExpiringNoChapterCount AS [expired_no_chapter_count],
                @WaitingForChapterCount AS [waiting_for_chapter_count],
                @MissingSnapshotCoverageCount AS [missing_snapshot_coverage_count],
                @IneligibleCount AS [ineligible_count]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    WHERE [RunID] = @RunID;

    SELECT
        @RunID AS [RunID],
        @AsOfDate AS [AsOfDate],
        CAST(0 AS bit) AS [DryRun],
        @PendingEventCount AS [PendingEventCount],
        @EligibleEventCount AS [EligibleEventCount],
        @AlreadyAwardedCount AS [AlreadyAwardedCount],
        @InsertedAwardCount AS [NewAwardCount],
        @PointTotal AS [PointTotal],
        @ExpiringNoChapterCount AS [ExpiringNoChapterCount],
        @WaitingForChapterCount AS [WaitingForChapterCount],
        @MissingSnapshotCoverageCount AS [MissingSnapshotCoverageCount],
        @IneligibleCount AS [IneligibleCount];
END;
GO
