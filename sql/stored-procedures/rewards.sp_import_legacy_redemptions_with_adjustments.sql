-- Live Azure SQL stored procedure export.
-- Source object: [rewards].[sp_import_legacy_redemptions_with_adjustments].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_import_legacy_redemptions_with_adjustments]
    @RedemptionsJson nvarchar(max),
    @DryRun bit = 0,
    @RunType nvarchar(32) = N'import',
    @SourceAsOfDate date = '2026-02-08',
    @LedgerStartDate date = '2026-05-02',
    @PostedByPrincipalName nvarchar(256) = NULL,
    @PostedByPrincipalId nvarchar(128) = NULL,
    @AllowDuesCreditShortfallAdjustment bit = 0,
    @SourceType nvarchar(64) = N'redemption',
    @AdjustmentSourceType nvarchar(64) = N'legacy_dues_credit_adjustment',
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
        THROW 52800, N'Rewards redemption tables do not exist. Apply rewards/sql/chapter_rewards_schema.sql and rewards/sql/redemption_processing.sql first.', 1;
    END;

    IF @RunType NOT IN (N'daily', N'manual', N'import', N'backfill')
        THROW 52801, N'Unsupported rewards redemption import run type.', 1;

    IF ISJSON(@RedemptionsJson) <> 1
        THROW 52802, N'RedemptionsJson must be a JSON array.', 1;

    DECLARE @Parsed table
    (
        [Source_Row_Number] int NOT NULL,
        [External_Request_ID] nvarchar(64) NOT NULL,
        [Source_ChapterID] int NOT NULL,
        [Source_Chapter_Name] nvarchar(256) NULL,
        [Request_Date] date NOT NULL,
        [Points] int NOT NULL,
        [Redemption_Category] nvarchar(64) NOT NULL,
        [Payment_Mode] nvarchar(64) NOT NULL,
        [Description] nvarchar(512) NULL,
        [Receipt_Reference] nvarchar(256) NULL,
        [Notes] nvarchar(max) NULL,
        [Source_Key] nvarchar(256) NOT NULL
    );

    INSERT INTO @Parsed
    (
        [Source_Row_Number],
        [External_Request_ID],
        [Source_ChapterID],
        [Source_Chapter_Name],
        [Request_Date],
        [Points],
        [Redemption_Category],
        [Payment_Mode],
        [Description],
        [Receipt_Reference],
        [Notes],
        [Source_Key]
    )
    SELECT
        COALESCE(raw.[Source_Row_Number], CONVERT(int, parsed.[key]) + 1),
        LTRIM(RTRIM(raw.[External_Request_ID])),
        raw.[ChapterID],
        NULLIF(LTRIM(RTRIM(raw.[Chapter_Name])), N''),
        raw.[Request_Date],
        raw.[Points],
        CASE
            WHEN raw.[Redemption_Category] IS NOT NULL AND LTRIM(RTRIM(raw.[Redemption_Category])) <> N''
                THEN LOWER(LTRIM(RTRIM(raw.[Redemption_Category])))
            WHEN raw.[Notes] = N'Chapter Renewal'
                THEN N'chapter_renewal'
            WHEN raw.[Notes] = N'Go Promotion'
                THEN N'go_promotion'
            ELSE N'other'
        END,
        CASE
            WHEN raw.[Payment_Mode] IS NOT NULL AND LTRIM(RTRIM(raw.[Payment_Mode])) <> N''
                THEN LOWER(LTRIM(RTRIM(raw.[Payment_Mode])))
            WHEN raw.[Notes] = N'Chapter Renewal'
                THEN N'dues_credit'
            WHEN raw.[Notes] = N'Go Promotion'
                THEN N'reimbursement'
            ELSE N'other'
        END,
        NULLIF(LTRIM(RTRIM(COALESCE(raw.[Description], raw.[Notes]))), N''),
        NULLIF(LTRIM(RTRIM(raw.[Receipt_Reference])), N''),
        NULLIF(LTRIM(RTRIM(raw.[Notes])), N''),
        CONCAT(@SourceType, N':', LTRIM(RTRIM(raw.[External_Request_ID])))
    FROM OPENJSON(@RedemptionsJson) AS parsed
    CROSS APPLY OPENJSON(parsed.[value])
    WITH
    (
        [Source_Row_Number] int '$.source_row_number',
        [External_Request_ID] nvarchar(64) '$.request_id',
        [ChapterID] int '$.chapter_id',
        [Chapter_Name] nvarchar(256) '$.chapter_name',
        [Request_Date] date '$.request_date',
        [Points] int '$.points',
        [Redemption_Category] nvarchar(64) '$.redemption_category',
        [Payment_Mode] nvarchar(64) '$.payment_mode',
        [Description] nvarchar(512) '$.description',
        [Receipt_Reference] nvarchar(256) '$.receipt_ref',
        [Notes] nvarchar(max) '$.notes'
    ) AS raw;

    IF EXISTS
    (
        SELECT 1
        FROM @Parsed
        WHERE [External_Request_ID] IS NULL
           OR [External_Request_ID] = N''
           OR [Source_ChapterID] IS NULL
           OR [Request_Date] IS NULL
           OR [Points] IS NULL
    )
    BEGIN
        THROW 52803, N'Redemption import contains incomplete rows.', 1;
    END;

    IF EXISTS (SELECT 1 FROM @Parsed WHERE [Points] <= 0)
        THROW 52804, N'Redemption import contains non-positive point values.', 1;

    IF EXISTS
    (
        SELECT 1
        FROM @Parsed
        WHERE [Request_Date] <= @SourceAsOfDate
           OR [Request_Date] > @LedgerStartDate
    )
    BEGIN
        THROW 52805, N'Legacy-gap redemption dates must be after the source balance date and on or before the ledger start date.', 1;
    END;

    IF EXISTS
    (
        SELECT 1
        FROM @Parsed
        WHERE [Redemption_Category] NOT IN (N'chapter_renewal', N'go_promotion', N'other')
           OR [Payment_Mode] NOT IN (N'dues_credit', N'reimbursement', N'other')
    )
    BEGIN
        THROW 52806, N'Redemption import contains unsupported category or payment mode values.', 1;
    END;

    IF EXISTS
    (
        SELECT 1
        FROM @Parsed
        GROUP BY [External_Request_ID]
        HAVING COUNT(*) > 1
    )
    BEGIN
        THROW 52807, N'Redemption import contains duplicate request IDs.', 1;
    END;

    DECLARE @LatestSnapshotDate date =
    (
        SELECT MAX([Snapshot_Date])
        FROM [rewards].[chapter_daily_snapshot]
    );

    DECLARE @Candidates table
    (
        [Source_Row_Number] int NOT NULL,
        [External_Request_ID] nvarchar(64) NOT NULL,
        [Source_ChapterID] int NOT NULL,
        [ChapterID] int NOT NULL,
        [Chapter_Code] nvarchar(64) NOT NULL,
        [Chapter_Name] nvarchar(256) NULL,
        [Request_Date] date NOT NULL,
        [Points] int NOT NULL,
        [Redemption_Category] nvarchar(64) NOT NULL,
        [Payment_Mode] nvarchar(64) NOT NULL,
        [Description] nvarchar(512) NULL,
        [Receipt_Reference] nvarchar(256) NULL,
        [Notes] nvarchar(max) NULL,
        [Source_Key] nvarchar(256) NOT NULL,
        [Existing_RedemptionID] bigint NULL,
        [Existing_Posted_TransactionID] bigint NULL,
        [Existing_TransactionID] bigint NULL
    );

    ;WITH [chapter_lookup] AS
    (
        SELECT
            c.[ChapterID],
            c.[Chapter_Code],
            c.[Chapter_Name],
            0 AS [Lookup_Source_Order]
        FROM [rewards].[chapter_daily_snapshot] AS c
        WHERE c.[Snapshot_Date] = @LatestSnapshotDate

        UNION ALL

        SELECT
            c.[ChapterID],
            c.[ChapterCode],
            c.[ChapterName],
            1
        FROM [membership].[chapters] AS c
        WHERE c.[ChapterID] IS NOT NULL
          AND LTRIM(RTRIM(COALESCE(c.[ChapterCode], N''))) <> N''
    )
    INSERT INTO @Candidates
    (
        [Source_Row_Number],
        [External_Request_ID],
        [Source_ChapterID],
        [ChapterID],
        [Chapter_Code],
        [Chapter_Name],
        [Request_Date],
        [Points],
        [Redemption_Category],
        [Payment_Mode],
        [Description],
        [Receipt_Reference],
        [Notes],
        [Source_Key],
        [Existing_RedemptionID],
        [Existing_Posted_TransactionID],
        [Existing_TransactionID]
    )
    SELECT
        p.[Source_Row_Number],
        p.[External_Request_ID],
        p.[Source_ChapterID],
        chapter.[ChapterID],
        chapter.[Chapter_Code],
        COALESCE(chapter.[Chapter_Name], p.[Source_Chapter_Name]),
        p.[Request_Date],
        p.[Points],
        p.[Redemption_Category],
        p.[Payment_Mode],
        p.[Description],
        p.[Receipt_Reference],
        p.[Notes],
        p.[Source_Key],
        request.[RedemptionID],
        request.[Posted_TransactionID],
        existing_tx.[TransactionID]
    FROM @Parsed AS p
    OUTER APPLY
    (
        SELECT TOP 1
            lookup.[ChapterID],
            lookup.[Chapter_Code],
            lookup.[Chapter_Name]
        FROM [chapter_lookup] AS lookup
        WHERE lookup.[ChapterID] = p.[Source_ChapterID]
           OR UPPER(LTRIM(RTRIM(lookup.[Chapter_Name]))) = UPPER(LTRIM(RTRIM(p.[Source_Chapter_Name])))
        ORDER BY
            CASE WHEN lookup.[ChapterID] = p.[Source_ChapterID] THEN 0 ELSE 1 END,
            lookup.[Lookup_Source_Order],
            lookup.[ChapterID]
    ) AS chapter
    LEFT JOIN [rewards].[redemption_requests] AS request
        ON request.[External_Request_ID] = p.[External_Request_ID]
    OUTER APPLY
    (
        SELECT TOP 1
            t.[TransactionID]
        FROM [rewards].[transactions] AS t
        WHERE t.[Source_Type] = @SourceType
          AND t.[Source_Key] = p.[Source_Key]
          AND t.[Transaction_Type] = N'redeem'
        ORDER BY t.[TransactionID]
    ) AS existing_tx
    WHERE chapter.[ChapterID] IS NOT NULL
      AND chapter.[Chapter_Code] IS NOT NULL
      AND LTRIM(RTRIM(chapter.[Chapter_Code])) <> N'';

    IF (SELECT COUNT(*) FROM @Candidates) <> (SELECT COUNT(*) FROM @Parsed)
    BEGIN
        THROW 52808, N'One or more legacy redemption rows could not be mapped to a current chapter.', 1;
    END;

    DECLARE @ChapterNeed table
    (
        [ChapterID] int NOT NULL,
        [Chapter_Code] nvarchar(64) NOT NULL,
        [Chapter_Name] nvarchar(256) NULL,
        [Requested_Points] int NOT NULL,
        [Dues_Credit_Points] int NOT NULL,
        [Eligible_Remaining_Points] int NOT NULL,
        [Shortfall_Points] int NOT NULL,
        [Allowed_Adjustment_Points] int NOT NULL,
        [Blocking_Shortfall_Points] int NOT NULL
    );

    ;WITH [candidate_need] AS
    (
        SELECT
            c.[ChapterID],
            MAX(c.[Chapter_Code]) AS [Chapter_Code],
            MAX(c.[Chapter_Name]) AS [Chapter_Name],
            SUM(c.[Points]) AS [Requested_Points],
            SUM(CASE WHEN c.[Payment_Mode] = N'dues_credit' THEN c.[Points] ELSE 0 END) AS [Dues_Credit_Points]
        FROM @Candidates AS c
        WHERE c.[Existing_Posted_TransactionID] IS NULL
          AND c.[Existing_TransactionID] IS NULL
        GROUP BY c.[ChapterID]
    ),
    [eligible_lots] AS
    (
        SELECT
            l.[ChapterID],
            SUM(l.[Remaining_Points]) AS [Eligible_Remaining_Points]
        FROM [rewards].[point_lots] AS l
        WHERE l.[Remaining_Points] > 0
          AND l.[Source_Type] IN (N'opening_balance', @AdjustmentSourceType)
        GROUP BY l.[ChapterID]
    ),
    [scored] AS
    (
        SELECT
            n.[ChapterID],
            n.[Chapter_Code],
            n.[Chapter_Name],
            n.[Requested_Points],
            n.[Dues_Credit_Points],
            COALESCE(l.[Eligible_Remaining_Points], 0) AS [Eligible_Remaining_Points],
            CASE
                WHEN n.[Requested_Points] > COALESCE(l.[Eligible_Remaining_Points], 0)
                    THEN n.[Requested_Points] - COALESCE(l.[Eligible_Remaining_Points], 0)
                ELSE 0
            END AS [Shortfall_Points]
        FROM [candidate_need] AS n
        LEFT JOIN [eligible_lots] AS l
            ON l.[ChapterID] = n.[ChapterID]
    )
    INSERT INTO @ChapterNeed
    (
        [ChapterID],
        [Chapter_Code],
        [Chapter_Name],
        [Requested_Points],
        [Dues_Credit_Points],
        [Eligible_Remaining_Points],
        [Shortfall_Points],
        [Allowed_Adjustment_Points],
        [Blocking_Shortfall_Points]
    )
    SELECT
        s.[ChapterID],
        s.[Chapter_Code],
        s.[Chapter_Name],
        s.[Requested_Points],
        s.[Dues_Credit_Points],
        s.[Eligible_Remaining_Points],
        s.[Shortfall_Points],
        CASE
            WHEN @AllowDuesCreditShortfallAdjustment = 1
             AND s.[Shortfall_Points] > 0
             AND s.[Shortfall_Points] <= s.[Dues_Credit_Points]
                THEN s.[Shortfall_Points]
            ELSE 0
        END,
        CASE
            WHEN s.[Shortfall_Points] = 0 THEN 0
            WHEN @AllowDuesCreditShortfallAdjustment = 1
             AND s.[Shortfall_Points] <= s.[Dues_Credit_Points]
                THEN 0
            ELSE s.[Shortfall_Points]
        END
    FROM [scored] AS s;

    DECLARE @InputRowCount int = (SELECT COUNT(*) FROM @Candidates);
    DECLARE @ExistingRequestCount int = (SELECT COUNT(*) FROM @Candidates WHERE [Existing_RedemptionID] IS NOT NULL);
    DECLARE @AlreadyPostedCount int =
    (
        SELECT COUNT(*)
        FROM @Candidates
        WHERE [Existing_Posted_TransactionID] IS NOT NULL
           OR [Existing_TransactionID] IS NOT NULL
    );
    DECLARE @NewPostCount int =
    (
        SELECT COUNT(*)
        FROM @Candidates
        WHERE [Existing_Posted_TransactionID] IS NULL
          AND [Existing_TransactionID] IS NULL
    );
    DECLARE @MissingOpeningLotCount int =
    (
        SELECT COUNT(*)
        FROM @ChapterNeed
        WHERE [Eligible_Remaining_Points] = 0
          AND [Allowed_Adjustment_Points] = 0
    );
    DECLARE @InsufficientBalanceCount int =
    (
        SELECT COUNT(*)
        FROM @ChapterNeed
        WHERE [Blocking_Shortfall_Points] > 0
    );
    DECLARE @ShortfallAdjustmentCount int =
    (
        SELECT COUNT(*)
        FROM @ChapterNeed
        WHERE [Allowed_Adjustment_Points] > 0
    );
    DECLARE @ShortfallAdjustmentPoints int =
    (
        SELECT COALESCE(SUM([Allowed_Adjustment_Points]), 0)
        FROM @ChapterNeed
    );
    DECLARE @InputPointTotal int = COALESCE((SELECT SUM([Points]) FROM @Candidates), 0);
    DECLARE @NewPointTotal int =
    (
        SELECT COALESCE(SUM([Points]), 0)
        FROM @Candidates
        WHERE [Existing_Posted_TransactionID] IS NULL
          AND [Existing_TransactionID] IS NULL
    );
    DECLARE @ChapterCount int = (SELECT COUNT(DISTINCT [ChapterID]) FROM @Candidates);
    DECLARE @DuesCreditCount int = (SELECT COUNT(*) FROM @Candidates WHERE [Payment_Mode] = N'dues_credit');
    DECLARE @DuesCreditPoints int = COALESCE((SELECT SUM([Points]) FROM @Candidates WHERE [Payment_Mode] = N'dues_credit'), 0);
    DECLARE @ReimbursementCount int = (SELECT COUNT(*) FROM @Candidates WHERE [Payment_Mode] = N'reimbursement');
    DECLARE @ReimbursementPoints int = COALESCE((SELECT SUM([Points]) FROM @Candidates WHERE [Payment_Mode] = N'reimbursement'), 0);

    IF @DryRun = 1
    BEGIN
        SELECT
            CAST(NULL AS int) AS [RunID],
            CAST(1 AS bit) AS [DryRun],
            @SourceAsOfDate AS [SourceAsOfDate],
            @LedgerStartDate AS [LedgerStartDate],
            @InputRowCount AS [InputRowCount],
            @ExistingRequestCount AS [ExistingRequestCount],
            @AlreadyPostedCount AS [AlreadyPostedCount],
            @NewPostCount AS [NewPostCount],
            @MissingOpeningLotCount AS [MissingOpeningLotCount],
            @InsufficientBalanceCount AS [InsufficientBalanceCount],
            @ShortfallAdjustmentCount AS [ShortfallAdjustmentCount],
            @ShortfallAdjustmentPoints AS [ShortfallAdjustmentPoints],
            @InputPointTotal AS [InputPointTotal],
            @NewPointTotal AS [NewPointTotal],
            @ChapterCount AS [ChapterCount],
            @DuesCreditCount AS [DuesCreditCount],
            @DuesCreditPoints AS [DuesCreditPoints],
            @ReimbursementCount AS [ReimbursementCount],
            @ReimbursementPoints AS [ReimbursementPoints];

        RETURN;
    END;

    IF @MissingOpeningLotCount > 0
        THROW 52809, N'One or more legacy-gap redemptions cannot be mapped to an opening or approved adjustment lot.', 1;

    IF @InsufficientBalanceCount > 0
        THROW 52810, N'One or more legacy-gap redemptions exceed the chapter opening-balance plus approved dues-credit adjustment amount.', 1;

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

    DECLARE @InsertedAdjustments table
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
        [MetadataJson],
        [Created_By]
    )
    OUTPUT INSERTED.[TransactionID], INSERTED.[ChapterID], INSERTED.[Chapter_Code], INSERTED.[Points_Delta], INSERTED.[Source_Key]
    INTO @InsertedAdjustments ([TransactionID], [ChapterID], [Chapter_Code], [Points_Delta], [Source_Key])
    SELECT
        n.[ChapterID],
        n.[Chapter_Code],
        N'adjustment',
        n.[Allowed_Adjustment_Points],
        NULL,
        NULL,
        NULL,
        @LedgerStartDate,
        @LedgerStartDate,
        @LedgerStartDate,
        SYSUTCDATETIME(),
        @RunID,
        @AdjustmentSourceType,
        CONCAT(@AdjustmentSourceType, N':', CONVERT(nvarchar(32), n.[ChapterID]), N':', CONVERT(char(8), @LedgerStartDate, 112)),
        @RuleVersion,
        (
            SELECT
                CAST(1 AS bit) AS [legacy_gap],
                @SourceAsOfDate AS [source_balance_as_of_date],
                @LedgerStartDate AS [ledger_start_date],
                n.[Requested_Points] AS [requested_points],
                n.[Eligible_Remaining_Points] AS [eligible_remaining_points],
                n.[Allowed_Adjustment_Points] AS [shortfall_adjustment_points],
                N'dues_credit_shortfall' AS [reason],
                @PostedByPrincipalName AS [posted_by_principal_name],
                @PostedByPrincipalId AS [posted_by_principal_id]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        @PostedByPrincipalName
    FROM @ChapterNeed AS n
    WHERE n.[Allowed_Adjustment_Points] > 0
      AND NOT EXISTS
      (
          SELECT 1
          FROM [rewards].[transactions] AS existing
          WHERE existing.[Source_Type] = @AdjustmentSourceType
            AND existing.[Source_Key] = CONCAT(@AdjustmentSourceType, N':', CONVERT(nvarchar(32), n.[ChapterID]), N':', CONVERT(char(8), @LedgerStartDate, 112))
            AND existing.[Transaction_Type] = N'adjustment'
            AND existing.[ChapterID] = n.[ChapterID]
      );

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
        a.[TransactionID],
        a.[ChapterID],
        a.[Chapter_Code],
        a.[Points_Delta],
        a.[Points_Delta],
        @LedgerStartDate,
        DATEADD(year, 2, @LedgerStartDate),
        @AdjustmentSourceType,
        a.[Source_Key]
    FROM @InsertedAdjustments AS a;

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
    SELECT
        c.[External_Request_ID],
        c.[ChapterID],
        c.[Chapter_Code],
        c.[Chapter_Name],
        c.[Request_Date],
        c.[Points],
        CAST(c.[Points] AS decimal(12, 3)) / CAST(1000 AS decimal(12, 3)),
        c.[Redemption_Category],
        c.[Payment_Mode],
        c.[Description],
        c.[Receipt_Reference],
        CAST(1 AS bit),
        N'approved',
        (
            SELECT
                c.[Source_Row_Number] AS [source_row_number],
                c.[External_Request_ID] AS [request_id],
                c.[Source_ChapterID] AS [source_chapter_id],
                c.[ChapterID] AS [resolved_chapter_id],
                c.[Chapter_Code] AS [chapter_code],
                c.[Chapter_Name] AS [chapter_name],
                c.[Request_Date] AS [request_date],
                c.[Points] AS [points],
                c.[Redemption_Category] AS [redemption_category],
                c.[Payment_Mode] AS [payment_mode],
                c.[Notes] AS [notes]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        c.[Notes],
        @PostedByPrincipalName,
        @PostedByPrincipalId
    FROM @Candidates AS c
    WHERE c.[Existing_RedemptionID] IS NULL
      AND c.[Existing_Posted_TransactionID] IS NULL
      AND c.[Existing_TransactionID] IS NULL;

    DECLARE @PostingRequests table
    (
        [RedemptionID] bigint NOT NULL,
        [External_Request_ID] nvarchar(64) NOT NULL,
        [ChapterID] int NOT NULL,
        [Chapter_Code] nvarchar(64) NOT NULL,
        [Chapter_Name] nvarchar(256) NULL,
        [Request_Date] date NOT NULL,
        [Points] int NOT NULL,
        [Redemption_Category] nvarchar(64) NOT NULL,
        [Payment_Mode] nvarchar(64) NOT NULL,
        [Description] nvarchar(512) NULL,
        [Receipt_Reference] nvarchar(256) NULL,
        [Notes] nvarchar(max) NULL,
        [Source_Key] nvarchar(256) NOT NULL
    );

    INSERT INTO @PostingRequests
    (
        [RedemptionID],
        [External_Request_ID],
        [ChapterID],
        [Chapter_Code],
        [Chapter_Name],
        [Request_Date],
        [Points],
        [Redemption_Category],
        [Payment_Mode],
        [Description],
        [Receipt_Reference],
        [Notes],
        [Source_Key]
    )
    SELECT
        request.[RedemptionID],
        c.[External_Request_ID],
        c.[ChapterID],
        c.[Chapter_Code],
        c.[Chapter_Name],
        c.[Request_Date],
        c.[Points],
        c.[Redemption_Category],
        c.[Payment_Mode],
        c.[Description],
        c.[Receipt_Reference],
        c.[Notes],
        c.[Source_Key]
    FROM @Candidates AS c
    INNER JOIN [rewards].[redemption_requests] AS request
        ON request.[External_Request_ID] = c.[External_Request_ID]
    WHERE request.[Posted_TransactionID] IS NULL
      AND request.[Status] IN (N'draft', N'approved')
      AND c.[Existing_TransactionID] IS NULL;

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
    SELECT
        p.[ChapterID],
        p.[Chapter_Code],
        N'redeem',
        -p.[Points],
        NULL,
        NULL,
        NULL,
        p.[Request_Date],
        NULL,
        p.[Request_Date],
        SYSUTCDATETIME(),
        @RunID,
        @SourceType,
        p.[Source_Key],
        @RuleVersion,
        (
            SELECT
                p.[RedemptionID] AS [redemption_id],
                p.[External_Request_ID] AS [external_request_id],
                CAST(1 AS bit) AS [legacy_gap],
                @SourceAsOfDate AS [source_balance_as_of_date],
                @LedgerStartDate AS [ledger_start_date],
                p.[Redemption_Category] AS [redemption_category],
                p.[Payment_Mode] AS [payment_mode],
                CAST(p.[Points] AS decimal(12, 3)) / CAST(1000 AS decimal(12, 3)) AS [amount_usd],
                p.[Description] AS [description],
                p.[Receipt_Reference] AS [receipt_reference],
                p.[Notes] AS [notes],
                @PostedByPrincipalName AS [posted_by_principal_name],
                @PostedByPrincipalId AS [posted_by_principal_id]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        @PostedByPrincipalName
    FROM @PostingRequests AS p;

    DECLARE @AvailableLots table
    (
        [LotID] bigint NOT NULL PRIMARY KEY,
        [ChapterID] int NOT NULL,
        [Source_Type] nvarchar(64) NOT NULL,
        [Earned_Date] date NOT NULL,
        [Expires_On] date NOT NULL,
        [Current_Remaining_Points] int NOT NULL
    );

    INSERT INTO @AvailableLots
    (
        [LotID],
        [ChapterID],
        [Source_Type],
        [Earned_Date],
        [Expires_On],
        [Current_Remaining_Points]
    )
    SELECT
        l.[LotID],
        l.[ChapterID],
        l.[Source_Type],
        l.[Earned_Date],
        l.[Expires_On],
        l.[Remaining_Points]
    FROM [rewards].[point_lots] AS l WITH (UPDLOCK, HOLDLOCK)
    WHERE l.[Remaining_Points] > 0
      AND l.[Source_Type] IN (N'opening_balance', @AdjustmentSourceType)
      AND EXISTS
      (
          SELECT 1
          FROM @PostingRequests AS p
          WHERE p.[ChapterID] = l.[ChapterID]
      );

    DECLARE @Allocations table
    (
        [Source_Key] nvarchar(256) NOT NULL,
        [LotID] bigint NOT NULL,
        [Points_Allocated] int NOT NULL
    );

    DECLARE
        @RequestSourceKey nvarchar(256),
        @RequestChapterID int,
        @RequestPaymentMode nvarchar(64),
        @RequestRemaining int,
        @LotID bigint,
        @LotRemaining int,
        @Allocated int;

    DECLARE request_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            [Source_Key],
            [ChapterID],
            [Payment_Mode],
            [Points]
        FROM @PostingRequests
        ORDER BY [ChapterID], [Request_Date], [RedemptionID];

    OPEN request_cursor;
    FETCH NEXT FROM request_cursor INTO @RequestSourceKey, @RequestChapterID, @RequestPaymentMode, @RequestRemaining;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        WHILE @RequestRemaining > 0
        BEGIN
            SELECT TOP 1
                @LotID = lots.[LotID],
                @LotRemaining = lots.[Current_Remaining_Points]
            FROM @AvailableLots AS lots
            WHERE lots.[ChapterID] = @RequestChapterID
              AND lots.[Current_Remaining_Points] > 0
              AND
              (
                  @RequestPaymentMode = N'dues_credit'
                  OR lots.[Source_Type] = N'opening_balance'
              )
            ORDER BY
                lots.[Earned_Date],
                CASE WHEN lots.[Source_Type] = N'opening_balance' THEN 0 ELSE 1 END,
                lots.[Expires_On],
                lots.[LotID];

            IF @LotID IS NULL
            BEGIN
                CLOSE request_cursor;
                DEALLOCATE request_cursor;
                THROW 52811, N'Could not allocate a legacy redemption against available point lots.', 1;
            END;

            SET @Allocated = CASE WHEN @LotRemaining >= @RequestRemaining THEN @RequestRemaining ELSE @LotRemaining END;

            INSERT INTO @Allocations ([Source_Key], [LotID], [Points_Allocated])
            VALUES (@RequestSourceKey, @LotID, @Allocated);

            UPDATE @AvailableLots
            SET [Current_Remaining_Points] = [Current_Remaining_Points] - @Allocated
            WHERE [LotID] = @LotID;

            SET @RequestRemaining = @RequestRemaining - @Allocated;
            SET @LotID = NULL;
            SET @LotRemaining = NULL;
        END;

        FETCH NEXT FROM request_cursor INTO @RequestSourceKey, @RequestChapterID, @RequestPaymentMode, @RequestRemaining;
    END;

    CLOSE request_cursor;
    DEALLOCATE request_cursor;

    INSERT INTO [rewards].[lot_allocations]
    (
        [Debit_TransactionID],
        [LotID],
        [Points_Allocated]
    )
    SELECT
        tx.[TransactionID],
        allocations.[LotID],
        SUM(allocations.[Points_Allocated])
    FROM @Allocations AS allocations
    INNER JOIN @InsertedTransactions AS tx
        ON tx.[Source_Key] = allocations.[Source_Key]
    GROUP BY tx.[TransactionID], allocations.[LotID];

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
        [Posted_TransactionID] = tx.[TransactionID],
        [Posted_At] = SYSUTCDATETIME(),
        [Posted_By_Principal_Name] = @PostedByPrincipalName,
        [Posted_By_Principal_Id] = @PostedByPrincipalId
    FROM [rewards].[redemption_requests] AS request
    INNER JOIN @PostingRequests AS p
        ON p.[RedemptionID] = request.[RedemptionID]
    INNER JOIN @InsertedTransactions AS tx
        ON tx.[Source_Key] = p.[Source_Key];

    DECLARE @InsertedTransactionCount int = (SELECT COUNT(*) FROM @InsertedTransactions);
    DECLARE @InsertedPointTotal int =
    (
        SELECT COALESCE(SUM(-t.[Points_Delta]), 0)
        FROM [rewards].[transactions] AS t
        INNER JOIN @InsertedTransactions AS inserted
            ON inserted.[TransactionID] = t.[TransactionID]
    );
    DECLARE @InsertedAdjustmentCount int = (SELECT COUNT(*) FROM @InsertedAdjustments);
    DECLARE @InsertedAdjustmentPoints int = (SELECT COALESCE(SUM([Points_Delta]), 0) FROM @InsertedAdjustments);

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
                @InputRowCount AS [input_row_count],
                @ExistingRequestCount AS [existing_request_count],
                @AlreadyPostedCount AS [already_posted_count],
                @InsertedTransactionCount AS [new_post_count],
                @InsertedAdjustmentCount AS [shortfall_adjustment_count],
                @InsertedAdjustmentPoints AS [shortfall_adjustment_points],
                @InputPointTotal AS [input_point_total],
                @InsertedPointTotal AS [new_point_total],
                @ChapterCount AS [chapter_count],
                @DuesCreditCount AS [dues_credit_count],
                @DuesCreditPoints AS [dues_credit_points],
                @ReimbursementCount AS [reimbursement_count],
                @ReimbursementPoints AS [reimbursement_points]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    WHERE [RunID] = @RunID;

    SELECT
        @RunID AS [RunID],
        CAST(0 AS bit) AS [DryRun],
        @SourceAsOfDate AS [SourceAsOfDate],
        @LedgerStartDate AS [LedgerStartDate],
        @InputRowCount AS [InputRowCount],
        @ExistingRequestCount AS [ExistingRequestCount],
        @AlreadyPostedCount AS [AlreadyPostedCount],
        @InsertedTransactionCount AS [NewPostCount],
        CAST(0 AS int) AS [MissingOpeningLotCount],
        CAST(0 AS int) AS [InsufficientBalanceCount],
        @InsertedAdjustmentCount AS [ShortfallAdjustmentCount],
        @InsertedAdjustmentPoints AS [ShortfallAdjustmentPoints],
        @InputPointTotal AS [InputPointTotal],
        @InsertedPointTotal AS [NewPointTotal],
        @ChapterCount AS [ChapterCount],
        @DuesCreditCount AS [DuesCreditCount],
        @DuesCreditPoints AS [DuesCreditPoints],
        @ReimbursementCount AS [ReimbursementCount],
        @ReimbursementPoints AS [ReimbursementPoints];
END;
GO
