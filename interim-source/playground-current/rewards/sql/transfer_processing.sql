-- Copyright 2026, American Go Association, All rights reserved

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'rewards') IS NULL
    EXEC(N'CREATE SCHEMA [rewards]');
GO

IF OBJECT_ID(N'rewards.chapter_transfers', N'U') IS NULL
BEGIN
    CREATE TABLE [rewards].[chapter_transfers]
    (
        [TransferID] bigint IDENTITY(1, 1) NOT NULL,
        [External_Transfer_ID] nvarchar(64) NOT NULL,
        [From_ChapterID] int NOT NULL,
        [From_Chapter_Code] nvarchar(64) NOT NULL,
        [From_Chapter_Name] nvarchar(256) NULL,
        [To_ChapterID] int NOT NULL,
        [To_Chapter_Code] nvarchar(64) NOT NULL,
        [To_Chapter_Name] nvarchar(256) NULL,
        [Transfer_Date] date NOT NULL,
        [Points] int NOT NULL,
        [Description] nvarchar(512) NOT NULL,
        [Notes] nvarchar(max) NULL,
        [Status] nvarchar(32) NOT NULL CONSTRAINT [DF_chapter_transfers_Status] DEFAULT N'pending',
        [Source_Payload_Json] nvarchar(max) NULL,
        [RunID] int NULL,
        [Out_TransactionID] bigint NULL,
        [Posted_At] datetime2(0) NULL,
        [Posted_By_Principal_Name] nvarchar(256) NULL,
        [Posted_By_Principal_Id] nvarchar(128) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_transfers_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_chapter_transfers] PRIMARY KEY CLUSTERED ([TransferID]),
        CONSTRAINT [UQ_chapter_transfers_External] UNIQUE ([External_Transfer_ID]),
        CONSTRAINT [FK_chapter_transfers_RunID] FOREIGN KEY ([RunID]) REFERENCES [rewards].[reward_runs] ([RunID]),
        CONSTRAINT [FK_chapter_transfers_Out_TransactionID] FOREIGN KEY ([Out_TransactionID]) REFERENCES [rewards].[transactions] ([TransactionID]),
        CONSTRAINT [CK_chapter_transfers_Points] CHECK ([Points] > 0),
        CONSTRAINT [CK_chapter_transfers_Different_Chapters] CHECK ([From_ChapterID] <> [To_ChapterID]),
        CONSTRAINT [CK_chapter_transfers_Status] CHECK ([Status] IN (N'pending', N'posted')),
        CONSTRAINT [CK_chapter_transfers_Source_Payload] CHECK ([Source_Payload_Json] IS NULL OR ISJSON([Source_Payload_Json]) = 1)
    );
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_chapter_transfers_From_Date'
      AND [object_id] = OBJECT_ID(N'rewards.chapter_transfers')
)
BEGIN
    CREATE INDEX [IX_chapter_transfers_From_Date]
        ON [rewards].[chapter_transfers] ([From_ChapterID], [Transfer_Date] DESC, [TransferID] DESC)
        INCLUDE ([To_ChapterID], [Points], [Status], [Out_TransactionID]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_chapter_transfers_To_Date'
      AND [object_id] = OBJECT_ID(N'rewards.chapter_transfers')
)
BEGIN
    CREATE INDEX [IX_chapter_transfers_To_Date]
        ON [rewards].[chapter_transfers] ([To_ChapterID], [Transfer_Date] DESC, [TransferID] DESC)
        INCLUDE ([From_ChapterID], [Points], [Status]);
END;
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_post_chapter_transfer]
    @FromChapterID int = NULL,
    @FromChapterCode nvarchar(64) = NULL,
    @ToChapterID int = NULL,
    @ToChapterCode nvarchar(64) = NULL,
    @TransferDate date = NULL,
    @Points int,
    @Description nvarchar(512),
    @ExternalTransferID nvarchar(64) = NULL,
    @Notes nvarchar(max) = NULL,
    @SourcePayloadJson nvarchar(max) = NULL,
    @DryRun bit = 0,
    @RunType nvarchar(32) = N'manual',
    @PostedByPrincipalName nvarchar(256) = NULL,
    @PostedByPrincipalId nvarchar(128) = NULL,
    @SourceType nvarchar(64) = N'chapter_transfer',
    @RuleVersion nvarchar(32) = N'2026-09-01'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'rewards.reward_runs', N'U') IS NULL
       OR OBJECT_ID(N'rewards.transactions', N'U') IS NULL
       OR OBJECT_ID(N'rewards.point_lots', N'U') IS NULL
       OR OBJECT_ID(N'rewards.lot_allocations', N'U') IS NULL
       OR OBJECT_ID(N'rewards.chapter_transfers', N'U') IS NULL
       OR OBJECT_ID(N'rewards.v_chapter_balances', N'V') IS NULL
    BEGIN
        THROW 53000, N'Rewards transfer objects do not exist. Apply the rewards schema, reporting views, and transfer processing SQL first.', 1;
    END;

    SET @TransferDate = COALESCE(@TransferDate, CAST(SYSUTCDATETIME() AS date));
    SET @FromChapterCode = NULLIF(UPPER(LTRIM(RTRIM(@FromChapterCode))), N'');
    SET @ToChapterCode = NULLIF(UPPER(LTRIM(RTRIM(@ToChapterCode))), N'');
    SET @Description = NULLIF(LTRIM(RTRIM(@Description)), N'');
    SET @ExternalTransferID = NULLIF(LTRIM(RTRIM(@ExternalTransferID)), N'');
    SET @Notes = NULLIF(LTRIM(RTRIM(@Notes)), N'');

    IF @RunType NOT IN (N'daily', N'manual', N'import', N'backfill')
        THROW 53001, N'Unsupported rewards transfer run type.', 1;

    IF @FromChapterID IS NULL AND @FromChapterCode IS NULL
        THROW 53002, N'A source ChapterID or ChapterCode is required.', 1;

    IF @ToChapterID IS NULL AND @ToChapterCode IS NULL
        THROW 53003, N'A destination ChapterID or ChapterCode is required.', 1;

    IF @Points IS NULL OR @Points <= 0
        THROW 53004, N'Chapter transfer points must be positive.', 1;

    IF @Description IS NULL
        THROW 53005, N'Chapter transfer description is required.', 1;

    IF @SourcePayloadJson IS NOT NULL AND ISJSON(@SourcePayloadJson) <> 1
        THROW 53006, N'SourcePayloadJson must be valid JSON when provided.', 1;

    DECLARE @FromChapter table
    (
        [ChapterID] int NOT NULL,
        [Chapter_Code] nvarchar(64) NOT NULL,
        [Chapter_Name] nvarchar(256) NULL
    );
    DECLARE @ToChapter table
    (
        [ChapterID] int NOT NULL,
        [Chapter_Code] nvarchar(64) NOT NULL,
        [Chapter_Name] nvarchar(256) NULL
    );

    INSERT INTO @FromChapter ([ChapterID], [Chapter_Code], [Chapter_Name])
    SELECT TOP 1
        balance.[ChapterID],
        balance.[Chapter_Code],
        balance.[Chapter_Name]
    FROM [rewards].[v_chapter_balances] AS balance
    WHERE (@FromChapterID IS NOT NULL AND balance.[ChapterID] = @FromChapterID)
       OR (@FromChapterID IS NULL AND @FromChapterCode IS NOT NULL AND UPPER(balance.[Chapter_Code]) = @FromChapterCode)
    ORDER BY balance.[ChapterID];

    INSERT INTO @ToChapter ([ChapterID], [Chapter_Code], [Chapter_Name])
    SELECT TOP 1
        balance.[ChapterID],
        balance.[Chapter_Code],
        balance.[Chapter_Name]
    FROM [rewards].[v_chapter_balances] AS balance
    WHERE (@ToChapterID IS NOT NULL AND balance.[ChapterID] = @ToChapterID)
       OR (@ToChapterID IS NULL AND @ToChapterCode IS NOT NULL AND UPPER(balance.[Chapter_Code]) = @ToChapterCode)
    ORDER BY balance.[ChapterID];

    IF NOT EXISTS (SELECT 1 FROM @FromChapter)
        THROW 53007, N'Chapter transfer source chapter was not found.', 1;

    IF NOT EXISTS (SELECT 1 FROM @ToChapter)
        THROW 53008, N'Chapter transfer destination chapter was not found.', 1;

    DECLARE @ResolvedFromChapterID int = (SELECT TOP 1 [ChapterID] FROM @FromChapter);
    DECLARE @ResolvedFromChapterCode nvarchar(64) = (SELECT TOP 1 [Chapter_Code] FROM @FromChapter);
    DECLARE @ResolvedFromChapterName nvarchar(256) = (SELECT TOP 1 [Chapter_Name] FROM @FromChapter);
    DECLARE @ResolvedToChapterID int = (SELECT TOP 1 [ChapterID] FROM @ToChapter);
    DECLARE @ResolvedToChapterCode nvarchar(64) = (SELECT TOP 1 [Chapter_Code] FROM @ToChapter);
    DECLARE @ResolvedToChapterName nvarchar(256) = (SELECT TOP 1 [Chapter_Name] FROM @ToChapter);

    IF @FromChapterID IS NOT NULL AND @FromChapterCode IS NOT NULL AND UPPER(@ResolvedFromChapterCode) <> @FromChapterCode
        THROW 53007, N'The source chapter ID and code do not identify the same chapter.', 1;

    IF @ToChapterID IS NOT NULL AND @ToChapterCode IS NOT NULL AND UPPER(@ResolvedToChapterCode) <> @ToChapterCode
        THROW 53008, N'The destination chapter ID and code do not identify the same chapter.', 1;

    IF @ResolvedFromChapterID = @ResolvedToChapterID
        THROW 53009, N'The source and destination chapters must be different.', 1;

    IF @ExternalTransferID IS NULL AND @DryRun = 0
    BEGIN
        SET @ExternalTransferID = CONCAT(
            N'transfer-',
            CONVERT(char(8), SYSUTCDATETIME(), 112),
            N'-',
            LOWER(LEFT(CONVERT(nvarchar(36), NEWID()), 12))
        );
    END;

    DECLARE @SourceKeyBase nvarchar(192) =
        CASE
            WHEN @ExternalTransferID IS NULL THEN NULL
            ELSE CONCAT(@SourceType, N':manual:', @ExternalTransferID)
        END;
    DECLARE @ExistingTransferID bigint = NULL;
    DECLARE @ExistingRunID int = NULL;
    DECLARE @ExistingOutTransactionID bigint = NULL;
    DECLARE @ExistingFromChapterID int = NULL;
    DECLARE @ExistingToChapterID int = NULL;
    DECLARE @ExistingPoints int = NULL;
    DECLARE @ExistingTransferDate date = NULL;
    DECLARE @ExistingDescription nvarchar(512) = NULL;

    IF @ExternalTransferID IS NOT NULL
    BEGIN
        SELECT TOP 1
            @ExistingTransferID = transfer.[TransferID],
            @ExistingRunID = transfer.[RunID],
            @ExistingOutTransactionID = transfer.[Out_TransactionID],
            @ExistingFromChapterID = transfer.[From_ChapterID],
            @ExistingToChapterID = transfer.[To_ChapterID],
            @ExistingPoints = transfer.[Points],
            @ExistingTransferDate = transfer.[Transfer_Date],
            @ExistingDescription = transfer.[Description]
        FROM [rewards].[chapter_transfers] AS transfer
        WHERE transfer.[External_Transfer_ID] = @ExternalTransferID;
    END;

    IF @ExistingTransferID IS NOT NULL
       AND
       (
           @ExistingFromChapterID <> @ResolvedFromChapterID
           OR @ExistingToChapterID <> @ResolvedToChapterID
           OR @ExistingPoints <> @Points
           OR @ExistingTransferDate <> @TransferDate
           OR @ExistingDescription <> @Description
       )
    BEGIN
        THROW 53010, N'External transfer ID already exists with different transfer details.', 1;
    END;

    DECLARE @FromAvailablePoints int;
    DECLARE @FromAvailableLotCount int;
    DECLARE @ToAvailablePoints int;

    SELECT
        @FromAvailablePoints = COALESCE(SUM(lot.[Remaining_Points]), 0),
        @FromAvailableLotCount = COUNT(*)
    FROM [rewards].[point_lots] AS lot
    WHERE lot.[ChapterID] = @ResolvedFromChapterID
      AND lot.[Remaining_Points] > 0
      AND lot.[Expires_On] >= @TransferDate;

    SELECT @ToAvailablePoints = COALESCE(SUM(lot.[Remaining_Points]), 0)
    FROM [rewards].[point_lots] AS lot
    WHERE lot.[ChapterID] = @ResolvedToChapterID
      AND lot.[Remaining_Points] > 0
      AND lot.[Expires_On] >= @TransferDate;

    DECLARE @AlreadyPostedCount int = CASE WHEN @ExistingOutTransactionID IS NULL THEN 0 ELSE 1 END;
    DECLARE @InsufficientBalanceCount int = CASE WHEN @AlreadyPostedCount = 0 AND @Points > @FromAvailablePoints THEN 1 ELSE 0 END;

    IF @DryRun = 1 OR @AlreadyPostedCount = 1
    BEGIN
        SELECT
            @ExistingRunID AS [RunID],
            CAST(@DryRun AS bit) AS [DryRun],
            @ExistingTransferID AS [TransferID],
            @ExternalTransferID AS [External_Transfer_ID],
            @ResolvedFromChapterID AS [From_ChapterID],
            @ResolvedFromChapterCode AS [From_Chapter_Code],
            @ResolvedFromChapterName AS [From_Chapter_Name],
            @ResolvedToChapterID AS [To_ChapterID],
            @ResolvedToChapterCode AS [To_Chapter_Code],
            @ResolvedToChapterName AS [To_Chapter_Name],
            @TransferDate AS [Transfer_Date],
            @Points AS [Points],
            @Description AS [Description],
            @ExistingOutTransactionID AS [Out_TransactionID],
            @AlreadyPostedCount AS [AlreadyPostedCount],
            CASE WHEN @AlreadyPostedCount = 0 AND @InsufficientBalanceCount = 0 THEN 1 ELSE 0 END AS [NewPostCount],
            @FromAvailablePoints AS [From_Available_Points],
            CASE WHEN @InsufficientBalanceCount = 0 AND @AlreadyPostedCount = 0 THEN @FromAvailablePoints - @Points ELSE @FromAvailablePoints END AS [From_Available_After_Points],
            @ToAvailablePoints AS [To_Available_Points],
            CASE WHEN @InsufficientBalanceCount = 0 AND @AlreadyPostedCount = 0 THEN @ToAvailablePoints + @Points ELSE @ToAvailablePoints END AS [To_Available_After_Points],
            @FromAvailableLotCount AS [Available_Lot_Count],
            @InsufficientBalanceCount AS [InsufficientBalanceCount],
            CASE WHEN @Points > @FromAvailablePoints THEN @Points - @FromAvailablePoints ELSE 0 END AS [Shortfall_Points],
            CAST(NULL AS int) AS [Transferred_Lot_Count],
            CAST(NULL AS int) AS [In_Transaction_Count];
        RETURN;
    END;

    IF @ExistingTransferID IS NOT NULL AND @ExistingOutTransactionID IS NULL
        THROW 53011, N'External transfer ID already exists but is not posted.', 1;

    IF @InsufficientBalanceCount > 0
        THROW 53012, N'Chapter transfer exceeds the source chapter available point balance on the transfer date.', 1;

    DECLARE @StartedTransaction bit = 0;

    BEGIN TRY
        IF @@TRANCOUNT = 0
        BEGIN
            BEGIN TRANSACTION;
            SET @StartedTransaction = 1;
        END;

        DECLARE @AvailableLots table
        (
            [LotID] bigint NOT NULL PRIMARY KEY,
            [Earn_TransactionID] bigint NOT NULL,
            [Remaining_Points] int NOT NULL,
            [Earned_Date] date NOT NULL,
            [Expires_On] date NOT NULL,
            [Earn_Source_Type] nvarchar(64) NOT NULL,
            [Earn_Source_Key] nvarchar(256) NOT NULL
        );

        INSERT INTO @AvailableLots
        (
            [LotID],
            [Earn_TransactionID],
            [Remaining_Points],
            [Earned_Date],
            [Expires_On],
            [Earn_Source_Type],
            [Earn_Source_Key]
        )
        SELECT
            lot.[LotID],
            lot.[Earn_TransactionID],
            lot.[Remaining_Points],
            lot.[Earned_Date],
            lot.[Expires_On],
            lot.[Source_Type],
            lot.[Source_Key]
        FROM [rewards].[point_lots] AS lot WITH (UPDLOCK, HOLDLOCK)
        WHERE lot.[ChapterID] = @ResolvedFromChapterID
          AND lot.[Remaining_Points] > 0
          AND lot.[Expires_On] >= @TransferDate;

        DECLARE @LockedAvailablePoints int = COALESCE((SELECT SUM([Remaining_Points]) FROM @AvailableLots), 0);
        IF @Points > @LockedAvailablePoints
            THROW 53012, N'Chapter transfer exceeds the source chapter available point balance on the transfer date.', 1;

        DECLARE @Allocations table
        (
            [LotID] bigint NOT NULL PRIMARY KEY,
            [Earn_TransactionID] bigint NOT NULL,
            [Points_Allocated] int NOT NULL,
            [Earned_Date] date NOT NULL,
            [Expires_On] date NOT NULL,
            [Earn_Source_Type] nvarchar(64) NOT NULL,
            [Earn_Source_Key] nvarchar(256) NOT NULL,
            [In_Source_Key] nvarchar(256) NOT NULL
        );

        ;WITH [ordered_lots] AS
        (
            SELECT
                lot.*,
                COALESCE
                (
                    SUM(lot.[Remaining_Points]) OVER
                    (
                        ORDER BY lot.[Earned_Date], lot.[Expires_On], lot.[LotID]
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                    ),
                    0
                ) AS [Prior_Points]
            FROM @AvailableLots AS lot
        )
        INSERT INTO @Allocations
        (
            [LotID],
            [Earn_TransactionID],
            [Points_Allocated],
            [Earned_Date],
            [Expires_On],
            [Earn_Source_Type],
            [Earn_Source_Key],
            [In_Source_Key]
        )
        SELECT
            lot.[LotID],
            lot.[Earn_TransactionID],
            CASE
                WHEN @Points - lot.[Prior_Points] >= lot.[Remaining_Points] THEN lot.[Remaining_Points]
                ELSE @Points - lot.[Prior_Points]
            END,
            lot.[Earned_Date],
            lot.[Expires_On],
            lot.[Earn_Source_Type],
            lot.[Earn_Source_Key],
            CONCAT(@SourceKeyBase, N':in:lot:', CONVERT(nvarchar(32), lot.[LotID]))
        FROM [ordered_lots] AS lot
        WHERE lot.[Prior_Points] < @Points;

        DECLARE @TransferredLotCount int = (SELECT COUNT(*) FROM @Allocations);
        DECLARE @AllocatedPoints int = COALESCE((SELECT SUM([Points_Allocated]) FROM @Allocations), 0);
        IF @AllocatedPoints <> @Points
            THROW 53013, N'Could not allocate the full chapter transfer against source point lots.', 1;

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
            @TransferDate,
            SYSUTCDATETIME(),
            N'running',
            NULL
        );

        DECLARE @RunID int = (SELECT TOP 1 [RunID] FROM @InsertedRun);
        DECLARE @InsertedTransfer table ([TransferID] bigint NOT NULL);
        INSERT INTO [rewards].[chapter_transfers]
        (
            [External_Transfer_ID],
            [From_ChapterID],
            [From_Chapter_Code],
            [From_Chapter_Name],
            [To_ChapterID],
            [To_Chapter_Code],
            [To_Chapter_Name],
            [Transfer_Date],
            [Points],
            [Description],
            [Notes],
            [Status],
            [Source_Payload_Json],
            [RunID],
            [Posted_By_Principal_Name],
            [Posted_By_Principal_Id]
        )
        OUTPUT INSERTED.[TransferID] INTO @InsertedTransfer ([TransferID])
        VALUES
        (
            @ExternalTransferID,
            @ResolvedFromChapterID,
            @ResolvedFromChapterCode,
            @ResolvedFromChapterName,
            @ResolvedToChapterID,
            @ResolvedToChapterCode,
            @ResolvedToChapterName,
            @TransferDate,
            @Points,
            @Description,
            @Notes,
            N'pending',
            @SourcePayloadJson,
            @RunID,
            @PostedByPrincipalName,
            @PostedByPrincipalId
        );

        DECLARE @TransferID bigint = (SELECT TOP 1 [TransferID] FROM @InsertedTransfer);
        DECLARE @OutSourceKey nvarchar(256) = CONCAT(@SourceKeyBase, N':out');
        DECLARE @InsertedOut table ([TransactionID] bigint NOT NULL);

        INSERT INTO [rewards].[transactions]
        (
            [ChapterID],
            [Chapter_Code],
            [Transaction_Type],
            [Points_Delta],
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
        OUTPUT INSERTED.[TransactionID] INTO @InsertedOut ([TransactionID])
        SELECT
            @ResolvedFromChapterID,
            @ResolvedFromChapterCode,
            N'transfer_out',
            -@Points,
            @TransferDate,
            NULL,
            @TransferDate,
            SYSUTCDATETIME(),
            @RunID,
            @SourceType,
            @OutSourceKey,
            @RuleVersion,
            (
                SELECT
                    @TransferID AS [transfer_id],
                    @ExternalTransferID AS [external_transfer_id],
                    N'out' AS [direction],
                    @ResolvedFromChapterID AS [from_chapter_id],
                    @ResolvedFromChapterCode AS [from_chapter_code],
                    @ResolvedToChapterID AS [to_chapter_id],
                    @ResolvedToChapterCode AS [to_chapter_code],
                    @Points AS [points],
                    @Description AS [description]
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            LEFT(@PostedByPrincipalName, 128);

        DECLARE @OutTransactionID bigint = (SELECT TOP 1 [TransactionID] FROM @InsertedOut);

        INSERT INTO [rewards].[lot_allocations]
        (
            [Debit_TransactionID],
            [LotID],
            [Points_Allocated],
            [Allocated_At]
        )
        SELECT
            @OutTransactionID,
            allocation.[LotID],
            allocation.[Points_Allocated],
            SYSUTCDATETIME()
        FROM @Allocations AS allocation;

        UPDATE lot
        SET lot.[Remaining_Points] = lot.[Remaining_Points] - allocation.[Points_Allocated]
        FROM [rewards].[point_lots] AS lot
        INNER JOIN @Allocations AS allocation
            ON allocation.[LotID] = lot.[LotID];

        DECLARE @InsertedIn table
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
            INTO @InsertedIn ([TransactionID], [Source_Key])
        SELECT
            @ResolvedToChapterID,
            @ResolvedToChapterCode,
            N'transfer_in',
            allocation.[Points_Allocated],
            @TransferDate,
            allocation.[Earned_Date],
            @TransferDate,
            SYSUTCDATETIME(),
            @RunID,
            @SourceType,
            allocation.[In_Source_Key],
            @RuleVersion,
            (
                SELECT
                    @TransferID AS [transfer_id],
                    @ExternalTransferID AS [external_transfer_id],
                    N'in' AS [direction],
                    @ResolvedFromChapterID AS [from_chapter_id],
                    @ResolvedFromChapterCode AS [from_chapter_code],
                    @ResolvedToChapterID AS [to_chapter_id],
                    @ResolvedToChapterCode AS [to_chapter_code],
                    @Points AS [transfer_points],
                    allocation.[Points_Allocated] AS [lot_points],
                    allocation.[LotID] AS [source_lot_id],
                    allocation.[Earn_TransactionID] AS [source_earn_transaction_id],
                    allocation.[Earn_Source_Type] AS [source_earn_source_type],
                    allocation.[Earn_Source_Key] AS [source_earn_source_key],
                    allocation.[Earned_Date] AS [original_earned_date],
                    allocation.[Expires_On] AS [original_expires_on],
                    @Description AS [description]
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            LEFT(@PostedByPrincipalName, 128)
        FROM @Allocations AS allocation;

        DECLARE @InTransactionCount int = @@ROWCOUNT;

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
            @ResolvedToChapterID,
            @ResolvedToChapterCode,
            allocation.[Points_Allocated],
            allocation.[Points_Allocated],
            allocation.[Earned_Date],
            allocation.[Expires_On],
            @SourceType,
            allocation.[In_Source_Key]
        FROM @Allocations AS allocation
        INNER JOIN @InsertedIn AS tx
            ON tx.[Source_Key] = allocation.[In_Source_Key];

        UPDATE [rewards].[chapter_transfers]
        SET
            [Status] = N'posted',
            [Out_TransactionID] = @OutTransactionID,
            [Posted_At] = SYSUTCDATETIME()
        WHERE [TransferID] = @TransferID;

        UPDATE [rewards].[reward_runs]
        SET
            [Completed_At] = SYSUTCDATETIME(),
            [Status] = N'succeeded',
            [SummaryJson] =
            (
                SELECT
                    @SourceType AS [processor],
                    @TransferID AS [transfer_id],
                    @ExternalTransferID AS [external_transfer_id],
                    @ResolvedFromChapterID AS [from_chapter_id],
                    @ResolvedFromChapterCode AS [from_chapter_code],
                    @ResolvedToChapterID AS [to_chapter_id],
                    @ResolvedToChapterCode AS [to_chapter_code],
                    @Points AS [points],
                    @TransferredLotCount AS [transferred_lot_count],
                    @InTransactionCount AS [in_transaction_count]
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        WHERE [RunID] = @RunID;

        IF @StartedTransaction = 1
            COMMIT TRANSACTION;

        SELECT
            @RunID AS [RunID],
            CAST(0 AS bit) AS [DryRun],
            @TransferID AS [TransferID],
            @ExternalTransferID AS [External_Transfer_ID],
            @ResolvedFromChapterID AS [From_ChapterID],
            @ResolvedFromChapterCode AS [From_Chapter_Code],
            @ResolvedFromChapterName AS [From_Chapter_Name],
            @ResolvedToChapterID AS [To_ChapterID],
            @ResolvedToChapterCode AS [To_Chapter_Code],
            @ResolvedToChapterName AS [To_Chapter_Name],
            @TransferDate AS [Transfer_Date],
            @Points AS [Points],
            @Description AS [Description],
            @OutTransactionID AS [Out_TransactionID],
            CAST(0 AS int) AS [AlreadyPostedCount],
            CAST(1 AS int) AS [NewPostCount],
            @LockedAvailablePoints AS [From_Available_Points],
            @LockedAvailablePoints - @Points AS [From_Available_After_Points],
            @ToAvailablePoints AS [To_Available_Points],
            @ToAvailablePoints + @Points AS [To_Available_After_Points],
            (SELECT COUNT(*) FROM @AvailableLots) AS [Available_Lot_Count],
            CAST(0 AS int) AS [InsufficientBalanceCount],
            CAST(0 AS int) AS [Shortfall_Points],
            @TransferredLotCount AS [Transferred_Lot_Count],
            @InTransactionCount AS [In_Transaction_Count];
    END TRY
    BEGIN CATCH
        IF @StartedTransaction = 1 AND XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO
