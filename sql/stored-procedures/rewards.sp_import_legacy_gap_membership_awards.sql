-- Live Azure SQL stored procedure export.
-- Source object: [rewards].[sp_import_legacy_gap_membership_awards].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_import_legacy_gap_membership_awards]
    @MembershipAwardsJson nvarchar(max),
    @DryRun bit = 0,
    @RunType nvarchar(32) = N'import',
    @SourceAsOfDate date = '2026-02-08',
    @LedgerStartDate date = '2026-05-02',
    @ValuationSnapshotDate date = '2026-05-02',
    @SourceType nvarchar(64) = N'legacy_membership_gap_award',
    @RuleVersion nvarchar(32) = N'2026-05-02'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'rewards.reward_runs', N'U') IS NULL
       OR OBJECT_ID(N'rewards.chapter_daily_snapshot', N'U') IS NULL
       OR OBJECT_ID(N'rewards.transactions', N'U') IS NULL
       OR OBJECT_ID(N'rewards.point_lots', N'U') IS NULL
    BEGIN
        THROW 53000, N'Rewards membership award tables do not exist. Apply rewards/sql/chapter_rewards_schema.sql first.', 1;
    END;

    IF @LedgerStartDate < @SourceAsOfDate
        THROW 53001, N'LedgerStartDate must be on or after SourceAsOfDate.', 1;

    IF @RunType NOT IN (N'daily', N'manual', N'import', N'backfill')
        THROW 53002, N'Unsupported rewards membership award run type.', 1;

    IF ISJSON(@MembershipAwardsJson) <> 1
        THROW 53003, N'MembershipAwardsJson must be a JSON array.', 1;

    IF NOT EXISTS (SELECT 1 FROM [rewards].[chapter_daily_snapshot] WHERE [Snapshot_Date] = @ValuationSnapshotDate)
        THROW 53004, N'Chapter valuation snapshot does not exist.', 1;

    DECLARE @Rows table
    (
        [Source_Row_Number] int NOT NULL,
        [ChapterID] int NOT NULL,
        [Input_Chapter_Code] nvarchar(64) NULL,
        [Input_Chapter_Name] nvarchar(256) NULL,
        [Member_Type] nvarchar(128) NOT NULL,
        [Event_Type] nvarchar(32) NOT NULL,
        [Member_Count] int NOT NULL
    );

    INSERT INTO @Rows
    (
        [Source_Row_Number],
        [ChapterID],
        [Input_Chapter_Code],
        [Input_Chapter_Name],
        [Member_Type],
        [Event_Type],
        [Member_Count]
    )
    SELECT
        COALESCE(raw.[Source_Row_Number], CONVERT(int, parsed.[key]) + 1),
        raw.[ChapterID],
        UPPER(NULLIF(LTRIM(RTRIM(raw.[Chapter_Code])), N'')),
        NULLIF(LTRIM(RTRIM(raw.[Chapter_Name])), N''),
        LTRIM(RTRIM(raw.[Member_Type])),
        CASE LOWER(REPLACE(LTRIM(RTRIM(raw.[Event_Type])), N' ', N'_'))
            WHEN N'new_membership' THEN N'new_membership'
            WHEN N'new' THEN N'new_membership'
            WHEN N'renewal' THEN N'renewal'
            WHEN N'renew' THEN N'renewal'
            ELSE LOWER(REPLACE(LTRIM(RTRIM(raw.[Event_Type])), N' ', N'_'))
        END,
        raw.[Member_Count]
    FROM OPENJSON(@MembershipAwardsJson) AS parsed
    CROSS APPLY OPENJSON(parsed.[value])
    WITH
    (
        [Source_Row_Number] int '$.source_row_number',
        [ChapterID] int '$.chapter_id',
        [Chapter_Code] nvarchar(64) '$.chapter_code',
        [Chapter_Name] nvarchar(256) '$.chapter_name',
        [Member_Type] nvarchar(128) '$.member_type',
        [Event_Type] nvarchar(32) '$.event_type',
        [Member_Count] int '$.count'
    ) AS raw;

    IF EXISTS
    (
        SELECT 1
        FROM @Rows
        WHERE [ChapterID] IS NULL
           OR [Member_Type] IS NULL
           OR [Member_Type] = N''
           OR [Event_Type] IS NULL
           OR [Event_Type] = N''
           OR [Member_Count] IS NULL
    )
    BEGIN
        THROW 53005, N'Legacy membership award import contains incomplete rows.', 1;
    END;

    IF EXISTS (SELECT 1 FROM @Rows WHERE [Member_Count] <= 0)
        THROW 53006, N'Legacy membership award import contains non-positive counts.', 1;

    IF EXISTS
    (
        SELECT 1
        FROM @Rows
        WHERE [Event_Type] NOT IN (N'new_membership', N'renewal')
    )
    BEGIN
        THROW 53007, N'Legacy membership award import contains unsupported event types.', 1;
    END;

    IF EXISTS
    (
        SELECT 1
        FROM @Rows
        GROUP BY [ChapterID], [Event_Type], [Member_Type]
        HAVING COUNT(*) > 1
    )
    BEGIN
        THROW 53008, N'Legacy membership award import contains duplicate chapter/event/member-type rows.', 1;
    END;

    IF OBJECT_ID(N'tempdb..#LegacyMembershipAwards', N'U') IS NOT NULL
        DROP TABLE #LegacyMembershipAwards;

    ;WITH [scored] AS
    (
        SELECT
            rows.[Source_Row_Number],
            rows.[ChapterID],
            snapshot.[Chapter_Code],
            snapshot.[Chapter_Name],
            snapshot.[Active_Member_Count],
            snapshot.[Multiplier],
            snapshot.[Is_Current],
            rows.[Input_Chapter_Code],
            rows.[Input_Chapter_Name],
            rows.[Member_Type],
            rows.[Event_Type],
            rows.[Member_Count],
            CASE
                WHEN rows.[Member_Type] = N'Adult Full' THEN 5000
                WHEN rows.[Member_Type] = N'Youth' THEN 2000
                WHEN rows.[Member_Type] = N'Adult Full - Lifetime' THEN 25000
                WHEN rows.[Member_Type] = N'Tournament Pass' THEN 0
                ELSE NULL
            END AS [Base_Points],
            CASE
                WHEN snapshot.[ChapterID] IS NULL THEN N'missing_chapter_snapshot'
                WHEN snapshot.[Is_Current] <> 1 THEN N'chapter_not_current'
                WHEN rows.[Member_Type] NOT IN (N'Adult Full', N'Youth', N'Adult Full - Lifetime', N'Tournament Pass') THEN N'unsupported_member_type'
                WHEN rows.[Member_Type] = N'Tournament Pass' THEN N'ineligible_member_type'
                ELSE N'eligible'
            END AS [Eligibility_Status]
        FROM @Rows AS rows
        LEFT JOIN [rewards].[chapter_daily_snapshot] AS snapshot
            ON snapshot.[Snapshot_Date] = @ValuationSnapshotDate
           AND snapshot.[ChapterID] = rows.[ChapterID]
    )
    SELECT
        scored.[Source_Row_Number],
        scored.[ChapterID],
        scored.[Chapter_Code],
        scored.[Chapter_Name],
        scored.[Active_Member_Count],
        scored.[Multiplier],
        scored.[Is_Current],
        scored.[Input_Chapter_Code],
        scored.[Input_Chapter_Name],
        scored.[Member_Type],
        scored.[Event_Type],
        scored.[Member_Count],
        scored.[Base_Points],
        scored.[Eligibility_Status],
        CASE
            WHEN scored.[Eligibility_Status] = N'eligible'
                THEN scored.[Member_Count] * scored.[Base_Points] * scored.[Multiplier]
            ELSE 0
        END AS [Points],
        CONCAT(
            @SourceType,
            N':',
            CONVERT(nvarchar(32), scored.[ChapterID]),
            N':',
            scored.[Event_Type],
            N':',
            LOWER(REPLACE(REPLACE(scored.[Member_Type], N' ', N'_'), N'-', N'_')),
            N':',
            CONVERT(char(8), @SourceAsOfDate, 112),
            N':',
            CONVERT(char(8), @LedgerStartDate, 112)
        ) AS [Source_Key],
        existing.[TransactionID] AS [Already_TransactionID]
    INTO #LegacyMembershipAwards
    FROM [scored] AS scored
    OUTER APPLY
    (
        SELECT TOP 1
            t.[TransactionID]
        FROM [rewards].[transactions] AS t
        WHERE t.[Source_Type] = @SourceType
          AND t.[Source_Key] = CONCAT(
              @SourceType,
              N':',
              CONVERT(nvarchar(32), scored.[ChapterID]),
              N':',
              scored.[Event_Type],
              N':',
              LOWER(REPLACE(REPLACE(scored.[Member_Type], N' ', N'_'), N'-', N'_')),
              N':',
              CONVERT(char(8), @SourceAsOfDate, 112),
              N':',
              CONVERT(char(8), @LedgerStartDate, 112)
          )
          AND t.[Transaction_Type] = N'earn'
        ORDER BY t.[TransactionID]
    ) AS existing;

    DECLARE @InputRowCount int = (SELECT COUNT(*) FROM #LegacyMembershipAwards);
    DECLARE @ChapterCount int = (SELECT COUNT(DISTINCT [ChapterID]) FROM #LegacyMembershipAwards);
    DECLARE @MemberCountTotal int = COALESCE((SELECT SUM([Member_Count]) FROM #LegacyMembershipAwards), 0);
    DECLARE @EligibleAwardCount int = (SELECT COUNT(*) FROM #LegacyMembershipAwards WHERE [Eligibility_Status] = N'eligible');
    DECLARE @AlreadyAwardedCount int = (SELECT COUNT(*) FROM #LegacyMembershipAwards WHERE [Eligibility_Status] = N'eligible' AND [Already_TransactionID] IS NOT NULL);
    DECLARE @NewAwardCount int = (SELECT COUNT(*) FROM #LegacyMembershipAwards WHERE [Eligibility_Status] = N'eligible' AND [Already_TransactionID] IS NULL);
    DECLARE @PointTotal int = COALESCE((SELECT SUM([Points]) FROM #LegacyMembershipAwards WHERE [Eligibility_Status] = N'eligible' AND [Already_TransactionID] IS NULL), 0);
    DECLARE @MissingChapterSnapshotCount int = (SELECT COUNT(*) FROM #LegacyMembershipAwards WHERE [Eligibility_Status] = N'missing_chapter_snapshot');
    DECLARE @ChapterNotCurrentCount int = (SELECT COUNT(*) FROM #LegacyMembershipAwards WHERE [Eligibility_Status] = N'chapter_not_current');
    DECLARE @UnsupportedMemberTypeCount int = (SELECT COUNT(*) FROM #LegacyMembershipAwards WHERE [Eligibility_Status] = N'unsupported_member_type');
    DECLARE @IneligibleMemberTypeCount int = (SELECT COUNT(*) FROM #LegacyMembershipAwards WHERE [Eligibility_Status] = N'ineligible_member_type');

    IF @DryRun = 1
    BEGIN
        SELECT
            CAST(NULL AS int) AS [RunID],
            CAST(1 AS bit) AS [DryRun],
            @SourceAsOfDate AS [SourceAsOfDate],
            @LedgerStartDate AS [LedgerStartDate],
            @ValuationSnapshotDate AS [ValuationSnapshotDate],
            @InputRowCount AS [InputRowCount],
            @ChapterCount AS [ChapterCount],
            @MemberCountTotal AS [MemberCountTotal],
            @EligibleAwardCount AS [EligibleAwardCount],
            @AlreadyAwardedCount AS [AlreadyAwardedCount],
            @NewAwardCount AS [NewAwardCount],
            @PointTotal AS [PointTotal],
            @MissingChapterSnapshotCount AS [MissingChapterSnapshotCount],
            @ChapterNotCurrentCount AS [ChapterNotCurrentCount],
            @UnsupportedMemberTypeCount AS [UnsupportedMemberTypeCount],
            @IneligibleMemberTypeCount AS [IneligibleMemberTypeCount];

        RETURN;
    END;

    IF @MissingChapterSnapshotCount > 0
       OR @ChapterNotCurrentCount > 0
       OR @UnsupportedMemberTypeCount > 0
    BEGIN
        THROW 53009, N'Legacy membership award import has blocked rows. Run dry-run and resolve them before posting.', 1;
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
        awards.[Base_Points],
        awards.[Multiplier],
        awards.[Active_Member_Count],
        @LedgerStartDate,
        @LedgerStartDate,
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
                awards.[Source_Row_Number] AS [source_row_number],
                awards.[Input_Chapter_Code] AS [input_chapter_code],
                awards.[Input_Chapter_Name] AS [input_chapter_name],
                awards.[ChapterID] AS [chapter_id],
                awards.[Chapter_Code] AS [chapter_code],
                awards.[Chapter_Name] AS [chapter_name],
                awards.[Member_Type] AS [member_type],
                awards.[Event_Type] AS [event_type],
                awards.[Member_Count] AS [member_count],
                awards.[Base_Points] AS [base_points],
                awards.[Multiplier] AS [multiplier],
                awards.[Active_Member_Count] AS [chapter_active_member_count],
                awards.[Points] AS [points]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM #LegacyMembershipAwards AS awards
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
                @SourceAsOfDate AS [source_balance_as_of_date],
                @LedgerStartDate AS [ledger_start_date],
                @ValuationSnapshotDate AS [valuation_snapshot_date],
                @InputRowCount AS [input_row_count],
                @ChapterCount AS [chapter_count],
                @MemberCountTotal AS [member_count_total],
                @EligibleAwardCount AS [eligible_award_count],
                @AlreadyAwardedCount AS [already_awarded_count],
                @InsertedAwardCount AS [new_award_count],
                @PointTotal AS [point_total],
                @MissingChapterSnapshotCount AS [missing_chapter_snapshot_count],
                @ChapterNotCurrentCount AS [chapter_not_current_count],
                @UnsupportedMemberTypeCount AS [unsupported_member_type_count],
                @IneligibleMemberTypeCount AS [ineligible_member_type_count]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    WHERE [RunID] = @RunID;

    SELECT
        @RunID AS [RunID],
        CAST(0 AS bit) AS [DryRun],
        @SourceAsOfDate AS [SourceAsOfDate],
        @LedgerStartDate AS [LedgerStartDate],
        @ValuationSnapshotDate AS [ValuationSnapshotDate],
        @InputRowCount AS [InputRowCount],
        @ChapterCount AS [ChapterCount],
        @MemberCountTotal AS [MemberCountTotal],
        @EligibleAwardCount AS [EligibleAwardCount],
        @AlreadyAwardedCount AS [AlreadyAwardedCount],
        @InsertedAwardCount AS [NewAwardCount],
        @PointTotal AS [PointTotal],
        @MissingChapterSnapshotCount AS [MissingChapterSnapshotCount],
        @ChapterNotCurrentCount AS [ChapterNotCurrentCount],
        @UnsupportedMemberTypeCount AS [UnsupportedMemberTypeCount],
        @IneligibleMemberTypeCount AS [IneligibleMemberTypeCount];
END;
GO
