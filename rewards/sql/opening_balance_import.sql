SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'rewards') IS NULL
    EXEC(N'CREATE SCHEMA [rewards]');
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_import_opening_balances]
    @OpeningBalancesJson nvarchar(max),
    @EffectiveDate date = NULL,
    @RunType nvarchar(32) = N'import',
    @DryRun bit = 0,
    @SourceType nvarchar(64) = N'opening_balance',
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
        THROW 52500, N'Rewards opening-balance import tables do not exist. Apply rewards/sql/chapter_rewards_schema.sql first.', 1;
    END;

    SET @EffectiveDate = COALESCE(@EffectiveDate, CAST(SYSUTCDATETIME() AS date));

    IF @RunType NOT IN (N'daily', N'manual', N'import', N'backfill')
    BEGIN
        THROW 52501, N'Unsupported rewards opening-balance import run type.', 1;
    END;

    IF ISJSON(@OpeningBalancesJson) <> 1
    BEGIN
        THROW 52502, N'OpeningBalancesJson must be a JSON array.', 1;
    END;

    DECLARE @LatestSnapshotDate date =
    (
        SELECT MAX([Snapshot_Date])
        FROM [rewards].[chapter_daily_snapshot]
    );

    DECLARE @Rows table
    (
        [Source_Row_Number] int NOT NULL,
        [Legacy_AGAID] int NOT NULL,
        [Chapter_Code] nvarchar(64) NOT NULL,
        [Chapter_Name] nvarchar(256) NOT NULL,
        [Prior_Available_Points] int NOT NULL,
        [Earned_Points] int NOT NULL,
        [Used_Points] int NOT NULL,
        [Opening_Balance_Points] int NOT NULL,
        [Reconciliation_Delta] int NOT NULL,
        [Source_Key] nvarchar(256) NOT NULL
    );

    INSERT INTO @Rows
    (
        [Source_Row_Number],
        [Legacy_AGAID],
        [Chapter_Code],
        [Chapter_Name],
        [Prior_Available_Points],
        [Earned_Points],
        [Used_Points],
        [Opening_Balance_Points],
        [Reconciliation_Delta],
        [Source_Key]
    )
    SELECT
        COALESCE(raw.[Source_Row_Number], CONVERT(int, parsed.[key]) + 1),
        raw.[Legacy_AGAID],
        UPPER(LTRIM(RTRIM(raw.[Chapter_Code]))),
        LTRIM(RTRIM(raw.[Chapter_Name])),
        raw.[Prior_Available_Points],
        raw.[Earned_Points],
        raw.[Used_Points],
        raw.[Opening_Balance_Points],
        raw.[Opening_Balance_Points] - (raw.[Prior_Available_Points] + raw.[Earned_Points] - raw.[Used_Points]),
        CONCAT(@SourceType, N':', UPPER(LTRIM(RTRIM(raw.[Chapter_Code]))), N':', CONVERT(nvarchar(8), @EffectiveDate, 112))
    FROM OPENJSON(@OpeningBalancesJson) AS parsed
    CROSS APPLY OPENJSON(parsed.[value])
    WITH
    (
        [Source_Row_Number] int '$.source_row_number',
        [Legacy_AGAID] int '$.legacy_agaid',
        [Chapter_Code] nvarchar(64) '$.chapter_code',
        [Chapter_Name] nvarchar(256) '$.chapter_name',
        [Prior_Available_Points] int '$.prior_available_points',
        [Earned_Points] int '$.earned_points',
        [Used_Points] int '$.used_points',
        [Opening_Balance_Points] int '$.opening_balance_points'
    ) AS raw;

    IF EXISTS
    (
        SELECT 1
        FROM @Rows
        WHERE [Legacy_AGAID] IS NULL
           OR [Chapter_Code] IS NULL
           OR [Chapter_Code] = N''
           OR [Chapter_Name] IS NULL
           OR [Chapter_Name] = N''
           OR [Prior_Available_Points] IS NULL
           OR [Earned_Points] IS NULL
           OR [Used_Points] IS NULL
           OR [Opening_Balance_Points] IS NULL
    )
    BEGIN
        THROW 52503, N'Opening balance import contains incomplete rows.', 1;
    END;

    IF EXISTS
    (
        SELECT 1
        FROM @Rows
        WHERE [Prior_Available_Points] < 0
           OR [Earned_Points] < 0
           OR [Used_Points] < 0
           OR [Opening_Balance_Points] < 0
    )
    BEGIN
        THROW 52504, N'Opening balance import contains negative point values.', 1;
    END;

    IF EXISTS
    (
        SELECT 1
        FROM @Rows
        GROUP BY [Chapter_Code]
        HAVING COUNT(*) > 1
    )
    BEGIN
        THROW 52505, N'Opening balance import contains duplicate chapter codes.', 1;
    END;

    DECLARE @ParsedInputRowCount int = (SELECT COUNT(*) FROM @Rows);
    DECLARE @AddedZeroChapterCount int = 0;

    ;WITH [latest_chapters] AS
    (
        SELECT
            c.[ChapterID],
            c.[Chapter_Code],
            c.[Chapter_Name],
            ROW_NUMBER() OVER (ORDER BY c.[Chapter_Code]) AS [RowNumber]
        FROM [rewards].[chapter_daily_snapshot] AS c
        WHERE c.[Snapshot_Date] = @LatestSnapshotDate
          AND NOT EXISTS
          (
              SELECT 1
              FROM @Rows AS rows
              WHERE rows.[Chapter_Code] = c.[Chapter_Code]
          )
    )
    INSERT INTO @Rows
    (
        [Source_Row_Number],
        [Legacy_AGAID],
        [Chapter_Code],
        [Chapter_Name],
        [Prior_Available_Points],
        [Earned_Points],
        [Used_Points],
        [Opening_Balance_Points],
        [Reconciliation_Delta],
        [Source_Key]
    )
    SELECT
        @ParsedInputRowCount + chapters.[RowNumber],
        chapters.[ChapterID],
        chapters.[Chapter_Code],
        chapters.[Chapter_Name],
        0,
        0,
        0,
        0,
        0,
        CONCAT(@SourceType, N':', chapters.[Chapter_Code], N':', CONVERT(nvarchar(8), @EffectiveDate, 112))
    FROM [latest_chapters] AS chapters;

    SET @AddedZeroChapterCount = @@ROWCOUNT;

    IF OBJECT_ID(N'tempdb..#OpeningBalanceImport', N'U') IS NOT NULL
    BEGIN
        DROP TABLE #OpeningBalanceImport;
    END;

    ;WITH [latest_chapters] AS
    (
        SELECT
            c.[ChapterID],
            c.[Chapter_Code],
            c.[Chapter_Name],
            c.[Active_Member_Count],
            c.[Multiplier],
            c.[Is_Current],
            c.[Snapshot_Date]
        FROM [rewards].[chapter_daily_snapshot] AS c
        WHERE c.[Snapshot_Date] = @LatestSnapshotDate
    )
    SELECT
        rows.[Source_Row_Number],
        rows.[Legacy_AGAID],
        COALESCE(chapters.[ChapterID], rows.[Legacy_AGAID]) AS [ChapterID],
        rows.[Chapter_Code],
        rows.[Chapter_Name],
        chapters.[ChapterID] AS [Snapshot_ChapterID],
        chapters.[Chapter_Name] AS [Snapshot_Chapter_Name],
        chapters.[Active_Member_Count],
        chapters.[Multiplier],
        chapters.[Is_Current],
        chapters.[Snapshot_Date] AS [Snapshot_Date],
        rows.[Prior_Available_Points],
        rows.[Earned_Points],
        rows.[Used_Points],
        rows.[Opening_Balance_Points],
        rows.[Reconciliation_Delta],
        rows.[Source_Key],
        existing.[TransactionID] AS [Already_TransactionID]
    INTO #OpeningBalanceImport
    FROM @Rows AS rows
    LEFT JOIN [latest_chapters] AS chapters
        ON chapters.[Chapter_Code] = rows.[Chapter_Code]
    OUTER APPLY
    (
        SELECT TOP 1
            t.[TransactionID]
        FROM [rewards].[transactions] AS t
        WHERE t.[Source_Type] = @SourceType
          AND t.[Source_Key] = rows.[Source_Key]
          AND t.[Transaction_Type] = N'earn'
        ORDER BY t.[TransactionID]
    ) AS existing;

    DECLARE @SetupRowCount int = (SELECT COUNT(*) FROM #OpeningBalanceImport);
    DECLARE @PositiveBalanceRowCount int = (SELECT COUNT(*) FROM #OpeningBalanceImport WHERE [Opening_Balance_Points] > 0);
    DECLARE @ZeroBalanceRowCount int = (SELECT COUNT(*) FROM #OpeningBalanceImport WHERE [Opening_Balance_Points] = 0);
    DECLARE @MissingSnapshotCount int = (SELECT COUNT(*) FROM #OpeningBalanceImport WHERE [Opening_Balance_Points] > 0 AND [Snapshot_ChapterID] IS NULL);
    DECLARE @ReconciliationIssueCount int = (SELECT COUNT(*) FROM #OpeningBalanceImport WHERE [Reconciliation_Delta] <> 0);
    DECLARE @AlreadyImportedCount int = (SELECT COUNT(*) FROM #OpeningBalanceImport WHERE [Opening_Balance_Points] > 0 AND [Already_TransactionID] IS NOT NULL);
    DECLARE @NewImportCount int = (SELECT COUNT(*) FROM #OpeningBalanceImport WHERE [Opening_Balance_Points] > 0 AND [Already_TransactionID] IS NULL);
    DECLARE @InputPointTotal int = COALESCE((SELECT SUM([Opening_Balance_Points]) FROM #OpeningBalanceImport), 0);
    DECLARE @NewPointTotal int = COALESCE((SELECT SUM([Opening_Balance_Points]) FROM #OpeningBalanceImport WHERE [Opening_Balance_Points] > 0 AND [Already_TransactionID] IS NULL), 0);

    IF @DryRun = 1
    BEGIN
        SELECT
            CAST(NULL AS int) AS [RunID],
            @EffectiveDate AS [EffectiveDate],
            CAST(1 AS bit) AS [DryRun],
            @ParsedInputRowCount AS [InputRowCount],
            @SetupRowCount AS [SetupRowCount],
            @AddedZeroChapterCount AS [AddedZeroChapterCount],
            @PositiveBalanceRowCount AS [PositiveBalanceRowCount],
            @ZeroBalanceRowCount AS [ZeroBalanceRowCount],
            @MissingSnapshotCount AS [MissingSnapshotCount],
            @ReconciliationIssueCount AS [ReconciliationIssueCount],
            @AlreadyImportedCount AS [AlreadyImportedCount],
            @NewImportCount AS [NewImportCount],
            @InputPointTotal AS [InputPointTotal],
            @NewPointTotal AS [NewPointTotal];

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
        @EffectiveDate,
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
        rows.[ChapterID],
        rows.[Chapter_Code],
        N'earn',
        rows.[Opening_Balance_Points],
        rows.[Opening_Balance_Points],
        NULL,
        rows.[Active_Member_Count],
        @EffectiveDate,
        @EffectiveDate,
        @EffectiveDate,
        SYSUTCDATETIME(),
        @RunID,
        @SourceType,
        rows.[Source_Key],
        @RuleVersion,
        (
            SELECT
                rows.[Source_Row_Number] AS [source_row_number],
                rows.[Legacy_AGAID] AS [legacy_agaid],
                rows.[ChapterID] AS [chapter_id],
                rows.[Chapter_Code] AS [chapter_code],
                rows.[Chapter_Name] AS [chapter_name],
                rows.[Snapshot_ChapterID] AS [snapshot_chapter_id],
                rows.[Snapshot_Chapter_Name] AS [snapshot_chapter_name],
                rows.[Prior_Available_Points] AS [prior_available_points],
                rows.[Earned_Points] AS [earned_points],
                rows.[Used_Points] AS [used_points],
                rows.[Opening_Balance_Points] AS [opening_balance_points],
                rows.[Reconciliation_Delta] AS [reconciliation_delta],
                @EffectiveDate AS [grandfathered_earned_date],
                rows.[Snapshot_Date] AS [mapping_snapshot_date]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM #OpeningBalanceImport AS rows
    WHERE rows.[Opening_Balance_Points] > 0
      AND rows.[Already_TransactionID] IS NULL;

    DECLARE @InsertedImportCount int = @@ROWCOUNT;

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
                @ParsedInputRowCount AS [input_row_count],
                @SetupRowCount AS [setup_row_count],
                @AddedZeroChapterCount AS [added_zero_chapter_count],
                @PositiveBalanceRowCount AS [positive_balance_row_count],
                @ZeroBalanceRowCount AS [zero_balance_row_count],
                @MissingSnapshotCount AS [missing_snapshot_count],
                @ReconciliationIssueCount AS [reconciliation_issue_count],
                @AlreadyImportedCount AS [already_imported_count],
                @InsertedImportCount AS [new_import_count],
                @InputPointTotal AS [input_point_total],
                @NewPointTotal AS [new_point_total]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    WHERE [RunID] = @RunID;

    SELECT
        @RunID AS [RunID],
        @EffectiveDate AS [EffectiveDate],
        CAST(0 AS bit) AS [DryRun],
        @ParsedInputRowCount AS [InputRowCount],
        @SetupRowCount AS [SetupRowCount],
        @AddedZeroChapterCount AS [AddedZeroChapterCount],
        @PositiveBalanceRowCount AS [PositiveBalanceRowCount],
        @ZeroBalanceRowCount AS [ZeroBalanceRowCount],
        @MissingSnapshotCount AS [MissingSnapshotCount],
        @ReconciliationIssueCount AS [ReconciliationIssueCount],
        @AlreadyImportedCount AS [AlreadyImportedCount],
        @InsertedImportCount AS [NewImportCount],
        @InputPointTotal AS [InputPointTotal],
        @NewPointTotal AS [NewPointTotal];
END;
GO
