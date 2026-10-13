-- Copyright 2026, American Go Association, All rights reserved

-- Live Azure SQL stored procedure export.
-- Source object: [rewards].[sp_post_manual_redemption].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_post_manual_redemption]
    @ChapterID int = NULL,
    @ChapterCode nvarchar(64) = NULL,
    @RequestDate date = NULL,
    @Points int,
    @RedemptionCategory nvarchar(64),
    @PaymentMode nvarchar(64),
    @Description nvarchar(512),
    @ReceiptReference nvarchar(256) = NULL,
    @ExternalRequestID nvarchar(64) = NULL,
    @Notes nvarchar(max) = NULL,
    @SourcePayloadJson nvarchar(max) = NULL,
    @DryRun bit = 0,
    @RunType nvarchar(32) = N'manual',
    @PostedByPrincipalName nvarchar(256) = NULL,
    @PostedByPrincipalId nvarchar(128) = NULL,
    @SourceType nvarchar(64) = N'redemption',
    @RuleVersion nvarchar(32) = N'2026-05-02'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'rewards.reward_runs', N'U') IS NULL
       OR OBJECT_ID(N'rewards.transactions', N'U') IS NULL
       OR OBJECT_ID(N'rewards.point_lots', N'U') IS NULL
       OR OBJECT_ID(N'rewards.lot_allocations', N'U') IS NULL
       OR OBJECT_ID(N'rewards.redemption_requests', N'U') IS NULL
    BEGIN
        THROW 52900, N'Rewards redemption tables do not exist. Apply rewards/sql/chapter_rewards_schema.sql and rewards/sql/redemption_processing.sql first.', 1;
    END;

    SET @RequestDate = COALESCE(@RequestDate, CAST(SYSUTCDATETIME() AS date));
    SET @ChapterCode = NULLIF(UPPER(LTRIM(RTRIM(@ChapterCode))), N'');
    SET @RedemptionCategory = LOWER(LTRIM(RTRIM(COALESCE(@RedemptionCategory, N''))));
    SET @PaymentMode = LOWER(LTRIM(RTRIM(COALESCE(@PaymentMode, N''))));
    SET @Description = NULLIF(LTRIM(RTRIM(@Description)), N'');
    SET @ReceiptReference = NULLIF(LTRIM(RTRIM(@ReceiptReference)), N'');
    SET @ExternalRequestID = NULLIF(LTRIM(RTRIM(@ExternalRequestID)), N'');

    IF @RunType NOT IN (N'daily', N'manual', N'import', N'backfill')
        THROW 52901, N'Unsupported rewards redemption run type.', 1;

    IF @ChapterID IS NULL AND @ChapterCode IS NULL
        THROW 52902, N'ChapterID or ChapterCode is required.', 1;

    IF @Points IS NULL OR @Points <= 0
        THROW 52903, N'Manual redemption points must be positive.', 1;

    IF @RedemptionCategory NOT IN (N'chapter_renewal', N'go_promotion', N'other')
        THROW 52904, N'Unsupported manual redemption category.', 1;

    IF @PaymentMode NOT IN (N'dues_credit', N'reimbursement', N'other')
        THROW 52905, N'Unsupported manual redemption payment mode.', 1;

    IF @Description IS NULL
        THROW 52906, N'Manual redemption description is required.', 1;

    IF @SourcePayloadJson IS NOT NULL AND ISJSON(@SourcePayloadJson) <> 1
        THROW 52907, N'SourcePayloadJson must be valid JSON when provided.', 1;

    DECLARE @Chapter table
    (
        [ChapterID] int NOT NULL,
        [Chapter_Code] nvarchar(64) NOT NULL,
        [Chapter_Name] nvarchar(256) NULL
    );

    ;WITH [latest_snapshot] AS
    (
        SELECT MAX([Snapshot_Date]) AS [Snapshot_Date]
        FROM [rewards].[chapter_daily_snapshot]
    ),
    [chapter_lookup] AS
    (
        SELECT
            c.[ChapterID],
            c.[Chapter_Code],
            c.[Chapter_Name],
            0 AS [Lookup_Order]
        FROM [rewards].[chapter_daily_snapshot] AS c
        INNER JOIN [latest_snapshot] AS latest
            ON latest.[Snapshot_Date] = c.[Snapshot_Date]

        UNION ALL

        SELECT
            l.[ChapterID],
            MAX(l.[Chapter_Code]) AS [Chapter_Code],
            CAST(NULL AS nvarchar(256)) AS [Chapter_Name],
            1 AS [Lookup_Order]
        FROM [rewards].[point_lots] AS l
        GROUP BY l.[ChapterID]

        UNION ALL

        SELECT
            t.[ChapterID],
            MAX(t.[Chapter_Code]) AS [Chapter_Code],
            CAST(NULL AS nvarchar(256)) AS [Chapter_Name],
            2 AS [Lookup_Order]
        FROM [rewards].[transactions] AS t
        GROUP BY t.[ChapterID]
    )
    INSERT INTO @Chapter ([ChapterID], [Chapter_Code], [Chapter_Name])
    SELECT TOP 1
        lookup.[ChapterID],
        lookup.[Chapter_Code],
        lookup.[Chapter_Name]
    FROM [chapter_lookup] AS lookup
    WHERE (@ChapterID IS NOT NULL AND lookup.[ChapterID] = @ChapterID)
       OR (@ChapterCode IS NOT NULL AND UPPER(lookup.[Chapter_Code]) = @ChapterCode)
    ORDER BY
        CASE WHEN @ChapterID IS NOT NULL AND lookup.[ChapterID] = @ChapterID THEN 0 ELSE 1 END,
        lookup.[Lookup_Order],
        lookup.[ChapterID];

    IF NOT EXISTS (SELECT 1 FROM @Chapter)
        THROW 52908, N'Manual redemption chapter was not found.', 1;

    DECLARE @ResolvedChapterID int = (SELECT TOP 1 [ChapterID] FROM @Chapter);
    DECLARE @ResolvedChapterCode nvarchar(64) = (SELECT TOP 1 [Chapter_Code] FROM @Chapter);
    DECLARE @ResolvedChapterName nvarchar(256) = (SELECT TOP 1 [Chapter_Name] FROM @Chapter);

    IF @ResolvedChapterCode IS NULL OR LTRIM(RTRIM(@ResolvedChapterCode)) = N''
        THROW 52908, N'Manual redemption chapter was not found.', 1;

    IF @ExternalRequestID IS NULL AND @DryRun = 0
    BEGIN
        SET @ExternalRequestID = CONCAT(
            N'manual-',
            CONVERT(char(8), SYSUTCDATETIME(), 112),
            N'-',
            LOWER(LEFT(CONVERT(nvarchar(36), NEWID()), 12))
        );
    END;

    DECLARE @SourceKey nvarchar(256) =
        CASE
            WHEN @ExternalRequestID IS NULL THEN NULL
            ELSE CONCAT(@SourceType, N':manual:', @ExternalRequestID)
        END;

    DECLARE @ExistingRedemptionID bigint = NULL;
    DECLARE @ExistingPostedTransactionID bigint = NULL;
    DECLARE @ExistingTransactionID bigint = NULL;

    IF @ExternalRequestID IS NOT NULL
    BEGIN
        SELECT TOP 1
            @ExistingRedemptionID = request.[RedemptionID],
            @ExistingPostedTransactionID = request.[Posted_TransactionID]
        FROM [rewards].[redemption_requests] AS request
        WHERE request.[External_Request_ID] = @ExternalRequestID
        ORDER BY request.[RedemptionID];

        SELECT TOP 1
            @ExistingTransactionID = tx.[TransactionID]
        FROM [rewards].[transactions] AS tx
        WHERE tx.[Source_Type] = @SourceType
          AND tx.[Source_Key] = @SourceKey
          AND tx.[Transaction_Type] = N'redeem'
          AND tx.[ChapterID] = @ResolvedChapterID
        ORDER BY tx.[TransactionID];
    END;

    DECLARE @AvailablePoints int =
    (
        SELECT COALESCE(SUM(l.[Remaining_Points]), 0)
        FROM [rewards].[point_lots] AS l
        WHERE l.[ChapterID] = @ResolvedChapterID
          AND l.[Remaining_Points] > 0
          AND l.[Expires_On] >= @RequestDate
    );
    DECLARE @AvailableLotCount int =
    (
        SELECT COUNT(*)
        FROM [rewards].[point_lots] AS l
        WHERE l.[ChapterID] = @ResolvedChapterID
          AND l.[Remaining_Points] > 0
          AND l.[Expires_On] >= @RequestDate
    );
    DECLARE @AlreadyPostedCount int = CASE WHEN @ExistingPostedTransactionID IS NOT NULL OR @ExistingTransactionID IS NOT NULL THEN 1 ELSE 0 END;
    DECLARE @InsufficientBalanceCount int = CASE WHEN @AlreadyPostedCount = 0 AND @Points > @AvailablePoints THEN 1 ELSE 0 END;
    DECLARE @NewPostCount int = CASE WHEN @AlreadyPostedCount = 0 AND @InsufficientBalanceCount = 0 THEN 1 ELSE 0 END;

    IF @DryRun = 1
    BEGIN
        SELECT
            CAST(NULL AS int) AS [RunID],
            CAST(1 AS bit) AS [DryRun],
            @ResolvedChapterID AS [ChapterID],
            @ResolvedChapterCode AS [Chapter_Code],
            @ResolvedChapterName AS [Chapter_Name],
            @RequestDate AS [Request_Date],
            @Points AS [Points],
            CAST(@Points AS decimal(12, 3)) / CAST(1000 AS decimal(12, 3)) AS [Amount_USD],
            @RedemptionCategory AS [Redemption_Category],
            @PaymentMode AS [Payment_Mode],
            @Description AS [Description],
            @ReceiptReference AS [Receipt_Reference],
            @ExternalRequestID AS [External_Request_ID],
            @ExistingRedemptionID AS [Existing_RedemptionID],
            COALESCE(@ExistingPostedTransactionID, @ExistingTransactionID) AS [Existing_TransactionID],
            @AlreadyPostedCount AS [AlreadyPostedCount],
            @NewPostCount AS [NewPostCount],
            @AvailablePoints AS [Available_Points],
            CASE WHEN @InsufficientBalanceCount = 0 THEN @AvailablePoints - @Points ELSE @AvailablePoints END AS [Available_After_Points],
            @AvailableLotCount AS [Available_Lot_Count],
            @InsufficientBalanceCount AS [InsufficientBalanceCount],
            CASE WHEN @Points > @AvailablePoints THEN @Points - @AvailablePoints ELSE 0 END AS [Shortfall_Points];

        RETURN;
    END;

    IF @ExistingRedemptionID IS NOT NULL
       AND @ExistingPostedTransactionID IS NULL
       AND @ExistingTransactionID IS NULL
    BEGIN
        THROW 52909, N'Manual redemption request ID already exists but is not posted.', 1;
    END;

    IF @AlreadyPostedCount > 0
    BEGIN
        SELECT
            CAST(NULL AS int) AS [RunID],
            CAST(0 AS bit) AS [DryRun],
            @ResolvedChapterID AS [ChapterID],
            @ResolvedChapterCode AS [Chapter_Code],
            @ResolvedChapterName AS [Chapter_Name],
            @RequestDate AS [Request_Date],
            @Points AS [Points],
            CAST(@Points AS decimal(12, 3)) / CAST(1000 AS decimal(12, 3)) AS [Amount_USD],
            @RedemptionCategory AS [Redemption_Category],
            @PaymentMode AS [Payment_Mode],
            @Description AS [Description],
            @ReceiptReference AS [Receipt_Reference],
            @ExternalRequestID AS [External_Request_ID],
            @ExistingRedemptionID AS [Existing_RedemptionID],
            COALESCE(@ExistingPostedTransactionID, @ExistingTransactionID) AS [Existing_TransactionID],
            @AlreadyPostedCount AS [AlreadyPostedCount],
            CAST(0 AS int) AS [NewPostCount],
            @AvailablePoints AS [Available_Points],
            @AvailablePoints AS [Available_After_Points],
            @AvailableLotCount AS [Available_Lot_Count],
            CAST(0 AS int) AS [InsufficientBalanceCount],
            CAST(0 AS int) AS [Shortfall_Points];

        RETURN;
    END;

    IF @InsufficientBalanceCount > 0
        THROW 52910, N'Manual redemption exceeds the chapter available point balance on the request date.', 1;

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
        @RequestDate,
        SYSUTCDATETIME(),
        N'running',
        NULL
    );

    DECLARE @RunID int = (SELECT TOP 1 [RunID] FROM @InsertedRun);

    DECLARE @InsertedRequest table ([RedemptionID] bigint NOT NULL);

    INSERT INTO [rewards].[redemption_requests]
    (
        [External_Request_ID],
        [ChapterID],
        [Chapter_Code],
        [Chapter_Name],
        [Request_Date],
        [Points],
        [Amount_USD],
        [Redemption_Category],
        [Payment_Mode],
        [Description],
        [Receipt_Reference],
        [Legacy_Gap],
        [Status],
        [Source_Payload_Json],
        [Notes],
        [Created_By_Principal_Name],
        [Created_By_Principal_Id]
    )
    OUTPUT INSERTED.[RedemptionID] INTO @InsertedRequest ([RedemptionID])
    VALUES
    (
        @ExternalRequestID,
        @ResolvedChapterID,
        @ResolvedChapterCode,
        @ResolvedChapterName,
        @RequestDate,
        @Points,
        CAST(@Points AS decimal(12, 3)) / CAST(1000 AS decimal(12, 3)),
        @RedemptionCategory,
        @PaymentMode,
        @Description,
        @ReceiptReference,
        CAST(0 AS bit),
        N'approved',
        @SourcePayloadJson,
        @Notes,
        @PostedByPrincipalName,
        @PostedByPrincipalId
    );

    DECLARE @RedemptionID bigint = (SELECT TOP 1 [RedemptionID] FROM @InsertedRequest);

    DECLARE @AvailableLots table
    (
        [LotID] bigint NOT NULL PRIMARY KEY,
        [Earned_Date] date NOT NULL,
        [Expires_On] date NOT NULL,
        [Current_Remaining_Points] int NOT NULL
    );

    INSERT INTO @AvailableLots
    (
        [LotID],
        [Earned_Date],
        [Expires_On],
        [Current_Remaining_Points]
    )
    SELECT
        l.[LotID],
        l.[Earned_Date],
        l.[Expires_On],
        l.[Remaining_Points]
    FROM [rewards].[point_lots] AS l WITH (UPDLOCK, HOLDLOCK)
    WHERE l.[ChapterID] = @ResolvedChapterID
      AND l.[Remaining_Points] > 0
      AND l.[Expires_On] >= @RequestDate;

    DECLARE @Allocations table
    (
        [LotID] bigint NOT NULL,
        [Points_Allocated] int NOT NULL
    );

    DECLARE
        @RequestRemaining int = @Points,
        @LotID bigint,
        @LotRemaining int,
        @Allocated int;

    WHILE @RequestRemaining > 0
    BEGIN
        SELECT TOP 1
            @LotID = lots.[LotID],
            @LotRemaining = lots.[Current_Remaining_Points]
        FROM @AvailableLots AS lots
        WHERE lots.[Current_Remaining_Points] > 0
        ORDER BY
            lots.[Earned_Date],
            lots.[Expires_On],
            lots.[LotID];

        IF @LotID IS NULL
            THROW 52911, N'Could not allocate a manual redemption against available point lots.', 1;

        SET @Allocated = CASE WHEN @LotRemaining >= @RequestRemaining THEN @RequestRemaining ELSE @LotRemaining END;

        INSERT INTO @Allocations ([LotID], [Points_Allocated])
        VALUES (@LotID, @Allocated);

        UPDATE @AvailableLots
        SET [Current_Remaining_Points] = [Current_Remaining_Points] - @Allocated
        WHERE [LotID] = @LotID;

        SET @RequestRemaining = @RequestRemaining - @Allocated;
        SET @LotID = NULL;
        SET @LotRemaining = NULL;
    END;

    DECLARE @InsertedTransactions table
    (
        [TransactionID] bigint NOT NULL,
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
        [MetadataJson],
        [Created_By]
    )
    OUTPUT INSERTED.[TransactionID], INSERTED.[Source_Key]
    INTO @InsertedTransactions ([TransactionID], [Source_Key])
    VALUES
    (
        @ResolvedChapterID,
        @ResolvedChapterCode,
        N'redeem',
        -@Points,
        NULL,
        NULL,
        NULL,
        @RequestDate,
        NULL,
        @RequestDate,
        SYSUTCDATETIME(),
        @RunID,
        @SourceType,
        @SourceKey,
        @RuleVersion,
        (
            SELECT
                @RedemptionID AS [redemption_id],
                @ExternalRequestID AS [external_request_id],
                CAST(0 AS bit) AS [legacy_gap],
                CAST(1 AS bit) AS [manual],
                @RedemptionCategory AS [redemption_category],
                @PaymentMode AS [payment_mode],
                CAST(@Points AS decimal(12, 3)) / CAST(1000 AS decimal(12, 3)) AS [amount_usd],
                @Description AS [description],
                @ReceiptReference AS [receipt_reference],
                @Notes AS [notes],
                @PostedByPrincipalName AS [posted_by_principal_name],
                @PostedByPrincipalId AS [posted_by_principal_id]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        @PostedByPrincipalName
    );

    DECLARE @TransactionID bigint = (SELECT TOP 1 [TransactionID] FROM @InsertedTransactions);

    INSERT INTO [rewards].[lot_allocations]
    (
        [Debit_TransactionID],
        [LotID],
        [Points_Allocated]
    )
    SELECT
        @TransactionID,
        allocations.[LotID],
        SUM(allocations.[Points_Allocated])
    FROM @Allocations AS allocations
    GROUP BY allocations.[LotID];

    ;WITH [lot_usage] AS
    (
        SELECT
            [LotID],
            SUM([Points_Allocated]) AS [Points_Allocated]
        FROM @Allocations
        GROUP BY [LotID]
    )
    UPDATE lots
    SET [Remaining_Points] = lots.[Remaining_Points] - usage.[Points_Allocated]
    FROM [rewards].[point_lots] AS lots
    INNER JOIN [lot_usage] AS usage
        ON usage.[LotID] = lots.[LotID];

    UPDATE request
    SET
        [Status] = N'posted',
        [Posted_TransactionID] = @TransactionID,
        [Posted_At] = SYSUTCDATETIME(),
        [Posted_By_Principal_Name] = @PostedByPrincipalName,
        [Posted_By_Principal_Id] = @PostedByPrincipalId
    FROM [rewards].[redemption_requests] AS request
    WHERE request.[RedemptionID] = @RedemptionID;

    UPDATE [rewards].[reward_runs]
    SET
        [Completed_At] = SYSUTCDATETIME(),
        [Status] = N'succeeded',
        [SummaryJson] =
        (
            SELECT
                @SourceType AS [processor],
                CAST(1 AS bit) AS [manual],
                @ResolvedChapterID AS [chapter_id],
                @ResolvedChapterCode AS [chapter_code],
                @ExternalRequestID AS [external_request_id],
                @RedemptionCategory AS [redemption_category],
                @PaymentMode AS [payment_mode],
                @Points AS [point_total],
                @PostedByPrincipalName AS [posted_by_principal_name],
                @PostedByPrincipalId AS [posted_by_principal_id]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    WHERE [RunID] = @RunID;

    SELECT
        @RunID AS [RunID],
        CAST(0 AS bit) AS [DryRun],
        @ResolvedChapterID AS [ChapterID],
        @ResolvedChapterCode AS [Chapter_Code],
        @ResolvedChapterName AS [Chapter_Name],
        @RequestDate AS [Request_Date],
        @Points AS [Points],
        CAST(@Points AS decimal(12, 3)) / CAST(1000 AS decimal(12, 3)) AS [Amount_USD],
        @RedemptionCategory AS [Redemption_Category],
        @PaymentMode AS [Payment_Mode],
        @Description AS [Description],
        @ReceiptReference AS [Receipt_Reference],
        @ExternalRequestID AS [External_Request_ID],
        @RedemptionID AS [Existing_RedemptionID],
        @TransactionID AS [Existing_TransactionID],
        CAST(0 AS int) AS [AlreadyPostedCount],
        CAST(1 AS int) AS [NewPostCount],
        @AvailablePoints AS [Available_Points],
        @AvailablePoints - @Points AS [Available_After_Points],
        @AvailableLotCount AS [Available_Lot_Count],
        CAST(0 AS int) AS [InsufficientBalanceCount],
        CAST(0 AS int) AS [Shortfall_Points];
END;
GO
