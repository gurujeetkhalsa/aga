SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'rewards') IS NULL
    EXEC(N'CREATE SCHEMA [rewards]');
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_process_point_expirations]
    @AsOfDate date = NULL,
    @RunType nvarchar(32) = N'daily',
    @DryRun bit = 0,
    @SourceType nvarchar(64) = N'point_expiration',
    @RuleVersion nvarchar(32) = N'2026-05-02'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'rewards.transactions', N'U') IS NULL
       OR OBJECT_ID(N'rewards.point_lots', N'U') IS NULL
       OR OBJECT_ID(N'rewards.lot_allocations', N'U') IS NULL
    BEGIN
        THROW 52600, N'Rewards point expiration tables do not exist. Apply rewards/sql/chapter_rewards_schema.sql first.', 1;
    END;

    SET @AsOfDate = COALESCE(@AsOfDate, CAST(SYSUTCDATETIME() AS date));

    IF @RunType NOT IN (N'daily', N'manual', N'import', N'backfill')
    BEGIN
        THROW 52601, N'Unsupported rewards point expiration run type.', 1;
    END;

    IF OBJECT_ID(N'tempdb..#PointExpirations', N'U') IS NOT NULL
    BEGIN
        DROP TABLE #PointExpirations;
    END;

    SELECT
        l.[LotID],
        l.[Earn_TransactionID],
        l.[ChapterID],
        l.[Chapter_Code],
        l.[Original_Points],
        l.[Remaining_Points],
        l.[Earned_Date],
        l.[Expires_On],
        l.[Source_Type] AS [Earn_Source_Type],
        l.[Source_Key] AS [Earn_Source_Key],
        CONCAT(@SourceType, N':lot:', CONVERT(nvarchar(32), l.[LotID]), N':expires:', CONVERT(char(8), l.[Expires_On], 112)) AS [Source_Key],
        existing.[TransactionID] AS [Existing_TransactionID]
    INTO #PointExpirations
    FROM [rewards].[point_lots] AS l
    OUTER APPLY
    (
        SELECT TOP 1
            t.[TransactionID]
        FROM [rewards].[transactions] AS t
        WHERE t.[Source_Type] = @SourceType
          AND t.[Source_Key] = CONCAT(@SourceType, N':lot:', CONVERT(nvarchar(32), l.[LotID]), N':expires:', CONVERT(char(8), l.[Expires_On], 112))
          AND t.[Transaction_Type] = N'expire'
          AND t.[ChapterID] = l.[ChapterID]
        ORDER BY t.[TransactionID]
    ) AS existing
    WHERE l.[Remaining_Points] > 0
      AND l.[Expires_On] < @AsOfDate;

    DECLARE @ExpiringLotCount int = (SELECT COUNT(*) FROM #PointExpirations);
    DECLARE @AlreadyExpiredCount int = (SELECT COUNT(*) FROM #PointExpirations WHERE [Existing_TransactionID] IS NOT NULL);
    DECLARE @NewExpirationCount int = (SELECT COUNT(*) FROM #PointExpirations WHERE [Existing_TransactionID] IS NULL);
    DECLARE @ExpiredPointTotal int = COALESCE((SELECT SUM([Remaining_Points]) FROM #PointExpirations WHERE [Existing_TransactionID] IS NULL), 0);
    DECLARE @ChapterCount int = (SELECT COUNT(*) FROM (SELECT DISTINCT [ChapterID] FROM #PointExpirations WHERE [Existing_TransactionID] IS NULL) AS chapters);

    IF @DryRun = 1
    BEGIN
        SELECT
            CAST(NULL AS int) AS [RunID],
            @AsOfDate AS [AsOfDate],
            CAST(1 AS bit) AS [DryRun],
            @ExpiringLotCount AS [ExpiringLotCount],
            @AlreadyExpiredCount AS [AlreadyExpiredCount],
            @NewExpirationCount AS [NewExpirationCount],
            @ExpiredPointTotal AS [ExpiredPointTotal],
            @ChapterCount AS [ChapterCount];

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
        INSERTED.[Source_Key]
    INTO @InsertedTransactions
    (
        [TransactionID],
        [ChapterID],
        [Chapter_Code],
        [Points_Delta],
        [Source_Key]
    )
    SELECT
        expirations.[ChapterID],
        expirations.[Chapter_Code],
        N'expire',
        -expirations.[Remaining_Points],
        NULL,
        NULL,
        NULL,
        @AsOfDate,
        expirations.[Earned_Date],
        expirations.[Expires_On],
        SYSUTCDATETIME(),
        @RunID,
        @SourceType,
        expirations.[Source_Key],
        @RuleVersion,
        (
            SELECT
                expirations.[LotID] AS [lot_id],
                expirations.[Earn_TransactionID] AS [earn_transaction_id],
                expirations.[ChapterID] AS [chapter_id],
                expirations.[Chapter_Code] AS [chapter_code],
                expirations.[Original_Points] AS [original_points],
                expirations.[Remaining_Points] AS [expired_points],
                expirations.[Earned_Date] AS [earned_date],
                expirations.[Expires_On] AS [expires_on],
                expirations.[Earn_Source_Type] AS [earn_source_type],
                expirations.[Earn_Source_Key] AS [earn_source_key]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    FROM #PointExpirations AS expirations
    WHERE expirations.[Existing_TransactionID] IS NULL;

    DECLARE @InsertedExpirationCount int = @@ROWCOUNT;

    INSERT INTO [rewards].[lot_allocations]
    (
        [Debit_TransactionID],
        [LotID],
        [Points_Allocated],
        [Allocated_At]
    )
    SELECT
        tx.[TransactionID],
        expirations.[LotID],
        expirations.[Remaining_Points],
        SYSUTCDATETIME()
    FROM @InsertedTransactions AS tx
    INNER JOIN #PointExpirations AS expirations
        ON expirations.[Source_Key] = tx.[Source_Key];

    UPDATE lots
    SET [Remaining_Points] = 0
    FROM [rewards].[point_lots] AS lots
    INNER JOIN #PointExpirations AS expirations
        ON expirations.[LotID] = lots.[LotID]
    INNER JOIN @InsertedTransactions AS tx
        ON tx.[Source_Key] = expirations.[Source_Key];

    UPDATE [rewards].[reward_runs]
    SET
        [Completed_At] = SYSUTCDATETIME(),
        [Status] = N'succeeded',
        [SummaryJson] =
        (
            SELECT
                @SourceType AS [processor],
                @ExpiringLotCount AS [expiring_lot_count],
                @AlreadyExpiredCount AS [already_expired_count],
                @InsertedExpirationCount AS [new_expiration_count],
                @ExpiredPointTotal AS [expired_point_total],
                @ChapterCount AS [chapter_count]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    WHERE [RunID] = @RunID;

    SELECT
        @RunID AS [RunID],
        @AsOfDate AS [AsOfDate],
        CAST(0 AS bit) AS [DryRun],
        @ExpiringLotCount AS [ExpiringLotCount],
        @AlreadyExpiredCount AS [AlreadyExpiredCount],
        @InsertedExpirationCount AS [NewExpirationCount],
        @ExpiredPointTotal AS [ExpiredPointTotal],
        @ChapterCount AS [ChapterCount];
END;
GO
