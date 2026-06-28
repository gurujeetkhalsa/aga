-- Live Azure SQL stored procedure export.
-- Source object: [rewards].[sp_process_chapter_renewal_notices].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_process_chapter_renewal_notices]
    @MessageId nvarchar(256),
    @ReceivedAt datetime2(0),
    @NoticeDate date,
    @NoticesJson nvarchar(max),
    @PointsPerRenewal int = 35000,
    @DryRun bit = 0,
    @RunType nvarchar(32) = N'daily',
    @SourceType nvarchar(64) = N'chapter_auto_renewal',
    @RuleVersion nvarchar(32) = N'2026-05-05'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'rewards.reward_runs', N'U') IS NULL
       OR OBJECT_ID(N'rewards.transactions', N'U') IS NULL
       OR OBJECT_ID(N'rewards.point_lots', N'U') IS NULL
       OR OBJECT_ID(N'rewards.lot_allocations', N'U') IS NULL
       OR OBJECT_ID(N'rewards.redemption_requests', N'U') IS NULL
       OR OBJECT_ID(N'rewards.chapter_renewal_notice_results', N'U') IS NULL
    BEGIN
        THROW 53100, N'Chapter renewal notice rewards tables do not exist. Apply the rewards schema, redemption processing, and chapter renewal notice processing SQL first.', 1;
    END;

    IF @RunType NOT IN (N'daily', N'manual', N'import', N'backfill')
        THROW 53101, N'Unsupported chapter renewal notice run type.', 1;

    IF @PointsPerRenewal <= 0
        THROW 53102, N'PointsPerRenewal must be positive.', 1;

    IF ISJSON(@NoticesJson) <> 1
        THROW 53103, N'NoticesJson must be a JSON array.', 1;

    DECLARE @Parsed table
    (
        [Source_Row_Number] int NOT NULL,
        [ChapterID] int NOT NULL,
        [Member_Raw] nvarchar(256) NULL,
        [Member_Type] nvarchar(64) NULL,
        [Row_Payload_Json] nvarchar(max) NULL,
        [Source_Payload_Json] nvarchar(max) NULL,
        [External_Request_ID] nvarchar(64) NOT NULL,
        [Source_Key] nvarchar(256) NOT NULL
    );

    INSERT INTO @Parsed
    (
        [Source_Row_Number],
        [ChapterID],
        [Member_Raw],
        [Member_Type],
        [Row_Payload_Json],
        [Source_Payload_Json],
        [External_Request_ID],
        [Source_Key]
    )
    SELECT
        COALESCE(raw.[Source_Row_Number], CONVERT(int, parsed.[key]) + 1),
        raw.[ChapterID],
        NULLIF(LTRIM(RTRIM(raw.[Member_Raw])), N''),
        NULLIF(LTRIM(RTRIM(raw.[Member_Type])), N''),
        raw.[Row_Payload_Json],
        raw.[Source_Payload_Json],
        LEFT(CONCAT(N'auto-renew:', @MessageId, N':', CONVERT(nvarchar(20), raw.[ChapterID])), 64),
        CONCAT(@MessageId, N':', CONVERT(nvarchar(20), raw.[ChapterID]))
    FROM OPENJSON(@NoticesJson) AS parsed
    CROSS APPLY OPENJSON(parsed.[value])
    WITH
    (
        [Source_Row_Number] int '$.source_row_number',
        [ChapterID] int '$.chapter_id',
        [Member_Raw] nvarchar(256) '$.member_raw',
        [Member_Type] nvarchar(64) '$.member_type',
        [Row_Payload_Json] nvarchar(max) '$.row_payload' AS JSON,
        [Source_Payload_Json] nvarchar(max) '$.source_payload' AS JSON
    ) AS raw
    WHERE raw.[ChapterID] IS NOT NULL;

    IF EXISTS
    (
        SELECT 1
        FROM @Parsed
        GROUP BY [ChapterID]
        HAVING COUNT(*) > 1
    )
    BEGIN
        THROW 53104, N'Chapter renewal notice email contains duplicate ChapterID values.', 1;
    END;

    DECLARE @LatestSnapshotDate date =
    (
        SELECT MAX([Snapshot_Date])
        FROM [rewards].[chapter_daily_snapshot]
    );

    DECLARE @Candidates table
    (
        [Source_Row_Number] int NOT NULL,
        [ChapterID] int NOT NULL,
        [Chapter_Code] nvarchar(64) NULL,
        [Chapter_Name] nvarchar(256) NULL,
        [Points_Required] int NOT NULL,
        [Available_Points] int NOT NULL,
        [External_Request_ID] nvarchar(64) NOT NULL,
        [Source_Key] nvarchar(256) NOT NULL,
        [Decision] nvarchar(32) NOT NULL,
        [Existing_NoticeID] bigint NULL,
        [Existing_RedemptionID] bigint NULL,
        [Existing_TransactionID] bigint NULL,
        [Source_Payload_Json] nvarchar(max) NULL
    );

    ;WITH [latest_chapters] AS
    (
        SELECT
            c.[ChapterID],
            c.[Chapter_Code],
            c.[Chapter_Name]
        FROM [rewards].[chapter_daily_snapshot] AS c
        WHERE c.[Snapshot_Date] = @LatestSnapshotDate
    ),
    [chapter_lookup] AS
    (
        SELECT
            m.[ChapterID],
            CONVERT(nvarchar(64), m.[ChapterCode]) AS [Chapter_Code],
            CONVERT(nvarchar(256), m.[ChapterName]) AS [Chapter_Name]
        FROM [membership].[chapters] AS m
    ),
    [available_points] AS
    (
        SELECT
            l.[ChapterID],
            SUM(l.[Remaining_Points]) AS [Available_Points]
        FROM [rewards].[point_lots] AS l
        WHERE l.[Remaining_Points] > 0
          AND l.[Expires_On] >= @NoticeDate
        GROUP BY l.[ChapterID]
    )
    INSERT INTO @Candidates
    (
        [Source_Row_Number],
        [ChapterID],
        [Chapter_Code],
        [Chapter_Name],
        [Points_Required],
        [Available_Points],
        [External_Request_ID],
        [Source_Key],
        [Decision],
        [Existing_NoticeID],
        [Existing_RedemptionID],
        [Existing_TransactionID],
        [Source_Payload_Json]
    )
    SELECT
        p.[Source_Row_Number],
        p.[ChapterID],
        COALESCE(latest.[Chapter_Code], chapters.[Chapter_Code]),
        COALESCE(latest.[Chapter_Name], chapters.[Chapter_Name]),
        @PointsPerRenewal,
        COALESCE(points.[Available_Points], 0),
        p.[External_Request_ID],
        p.[Source_Key],
        CASE
            WHEN notice.[NoticeID] IS NOT NULL THEN notice.[Decision]
            WHEN request.[Posted_TransactionID] IS NOT NULL OR existing_tx.[TransactionID] IS NOT NULL THEN N'already_posted'
            WHEN COALESCE(latest.[Chapter_Code], chapters.[Chapter_Code]) IS NULL THEN N'chapter_not_found'
            WHEN COALESCE(points.[Available_Points], 0) < @PointsPerRenewal THEN N'insufficient_points'
            ELSE N'eligible'
        END,
        notice.[NoticeID],
        request.[RedemptionID],
        COALESCE(request.[Posted_TransactionID], existing_tx.[TransactionID]),
        p.[Source_Payload_Json]
    FROM @Parsed AS p
    LEFT JOIN [latest_chapters] AS latest
        ON latest.[ChapterID] = p.[ChapterID]
    LEFT JOIN [chapter_lookup] AS chapters
        ON chapters.[ChapterID] = p.[ChapterID]
    LEFT JOIN [available_points] AS points
        ON points.[ChapterID] = p.[ChapterID]
    LEFT JOIN [rewards].[chapter_renewal_notice_results] AS notice
        ON notice.[Message_ID] = @MessageId
       AND notice.[ChapterID] = p.[ChapterID]
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
    ) AS existing_tx;

    IF @DryRun = 1
    BEGIN
        SELECT
            CAST(NULL AS int) AS [RunID],
            c.[Source_Row_Number],
            @MessageId AS [Message_ID],
            c.[ChapterID],
            c.[Chapter_Code],
            c.[Chapter_Name],
            @NoticeDate AS [Notice_Date],
            c.[Points_Required],
            c.[Available_Points],
            CASE WHEN c.[Decision] = N'eligible' THEN N'would_post' ELSE c.[Decision] END AS [Decision],
            c.[Existing_RedemptionID] AS [RedemptionID],
            c.[Existing_TransactionID] AS [TransactionID]
        FROM @Candidates AS c
        ORDER BY c.[Source_Row_Number], c.[ChapterID];

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
        @NoticeDate,
        SYSUTCDATETIME(),
        N'running',
        NULL
    );

    DECLARE @RunID int = (SELECT TOP 1 [RunID] FROM @InsertedRun);

    INSERT INTO [rewards].[chapter_renewal_notice_results]
    (
        [Message_ID],
        [Source_Row_Number],
        [ChapterID],
        [Chapter_Code],
        [Chapter_Name],
        [Notice_Date],
        [Received_At],
        [Points_Required],
        [Available_Points],
        [Decision],
        [RedemptionID],
        [TransactionID],
        [RunID],
        [Source_Payload_Json]
    )
    SELECT
        @MessageId,
        c.[Source_Row_Number],
        c.[ChapterID],
        c.[Chapter_Code],
        c.[Chapter_Name],
        @NoticeDate,
        @ReceivedAt,
        c.[Points_Required],
        c.[Available_Points],
        c.[Decision],
        c.[Existing_RedemptionID],
        c.[Existing_TransactionID],
        @RunID,
        c.[Source_Payload_Json]
    FROM @Candidates AS c
    WHERE c.[Existing_NoticeID] IS NULL
      AND c.[Decision] IN (N'already_posted', N'insufficient_points', N'chapter_not_found');

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
        @NoticeDate,
        c.[Points_Required],
        CAST(c.[Points_Required] AS decimal(12, 3)) / CAST(1000 AS decimal(12, 3)),
        N'chapter_renewal',
        N'dues_credit',
        N'Automatic Chapter Renewal',
        c.[External_Request_ID],
        CAST(0 AS bit),
        N'approved',
        c.[Source_Payload_Json],
        N'Automatic chapter renewal from ClubExpress Membership Renewal Emails',
        N'clubexpress-mailbox',
        NULL
    FROM @Candidates AS c
    WHERE c.[Existing_NoticeID] IS NULL
      AND c.[Existing_RedemptionID] IS NULL
      AND c.[Existing_TransactionID] IS NULL
      AND c.[Decision] = N'eligible';

    DECLARE @PostingRequests table
    (
        [RedemptionID] bigint NOT NULL,
        [External_Request_ID] nvarchar(64) NOT NULL,
        [ChapterID] int NOT NULL,
        [Chapter_Code] nvarchar(64) NOT NULL,
        [Chapter_Name] nvarchar(256) NULL,
        [Points] int NOT NULL,
        [Source_Key] nvarchar(256) NOT NULL,
        [Source_Payload_Json] nvarchar(max) NULL
    );

    INSERT INTO @PostingRequests
    (
        [RedemptionID],
        [External_Request_ID],
        [ChapterID],
        [Chapter_Code],
        [Chapter_Name],
        [Points],
        [Source_Key],
        [Source_Payload_Json]
    )
    SELECT
        request.[RedemptionID],
        request.[External_Request_ID],
        c.[ChapterID],
        c.[Chapter_Code],
        c.[Chapter_Name],
        c.[Points_Required],
        c.[Source_Key],
        c.[Source_Payload_Json]
    FROM @Candidates AS c
    INNER JOIN [rewards].[redemption_requests] AS request
        ON request.[External_Request_ID] = c.[External_Request_ID]
    WHERE c.[Existing_NoticeID] IS NULL
      AND c.[Existing_TransactionID] IS NULL
      AND c.[Decision] = N'eligible'
      AND request.[Posted_TransactionID] IS NULL
      AND request.[Status] IN (N'draft', N'approved');

    DECLARE @AvailableLots table
    (
        [LotID] bigint NOT NULL PRIMARY KEY,
        [ChapterID] int NOT NULL,
        [Earned_Date] date NOT NULL,
        [Expires_On] date NOT NULL,
        [Current_Remaining_Points] int NOT NULL
    );

    INSERT INTO @AvailableLots
    (
        [LotID],
        [ChapterID],
        [Earned_Date],
        [Expires_On],
        [Current_Remaining_Points]
    )
    SELECT
        l.[LotID],
        l.[ChapterID],
        l.[Earned_Date],
        l.[Expires_On],
        l.[Remaining_Points]
    FROM [rewards].[point_lots] AS l WITH (UPDLOCK, HOLDLOCK)
    WHERE l.[Remaining_Points] > 0
      AND l.[Expires_On] >= @NoticeDate
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
        @RequestRemaining int,
        @LotID bigint,
        @LotRemaining int,
        @Allocated int;

    DECLARE request_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT
            [Source_Key],
            [ChapterID],
            [Points]
        FROM @PostingRequests
        ORDER BY [ChapterID], [RedemptionID];

    OPEN request_cursor;
    FETCH NEXT FROM request_cursor INTO @RequestSourceKey, @RequestChapterID, @RequestRemaining;

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
            ORDER BY
                lots.[Earned_Date],
                lots.[Expires_On],
                lots.[LotID];

            IF @LotID IS NULL
            BEGIN
                CLOSE request_cursor;
                DEALLOCATE request_cursor;
                THROW 53105, N'Could not allocate an automatic chapter renewal against available point lots.', 1;
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

        FETCH NEXT FROM request_cursor INTO @RequestSourceKey, @RequestChapterID, @RequestRemaining;
    END;

    CLOSE request_cursor;
    DEALLOCATE request_cursor;

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
        @NoticeDate,
        NULL,
        @NoticeDate,
        SYSUTCDATETIME(),
        @RunID,
        @SourceType,
        p.[Source_Key],
        @RuleVersion,
        (
            SELECT
                p.[RedemptionID] AS [redemption_id],
                p.[External_Request_ID] AS [external_request_id],
                CAST(0 AS bit) AS [legacy_gap],
                N'chapter_renewal' AS [redemption_category],
                N'dues_credit' AS [payment_mode],
                CAST(p.[Points] AS decimal(12, 3)) / CAST(1000 AS decimal(12, 3)) AS [amount_usd],
                N'Automatic Chapter Renewal' AS [description],
                @MessageId AS [message_id],
                @ReceivedAt AS [received_at],
                @NoticeDate AS [notice_date]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ),
        N'clubexpress-mailbox'
    FROM @PostingRequests AS p;

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
        [Posted_By_Principal_Name] = N'clubexpress-mailbox',
        [Posted_By_Principal_Id] = NULL
    FROM [rewards].[redemption_requests] AS request
    INNER JOIN @PostingRequests AS p
        ON p.[RedemptionID] = request.[RedemptionID]
    INNER JOIN @InsertedTransactions AS tx
        ON tx.[Source_Key] = p.[Source_Key];

    INSERT INTO [rewards].[chapter_renewal_notice_results]
    (
        [Message_ID],
        [Source_Row_Number],
        [ChapterID],
        [Chapter_Code],
        [Chapter_Name],
        [Notice_Date],
        [Received_At],
        [Points_Required],
        [Available_Points],
        [Decision],
        [RedemptionID],
        [TransactionID],
        [RunID],
        [Source_Payload_Json]
    )
    SELECT
        @MessageId,
        c.[Source_Row_Number],
        c.[ChapterID],
        c.[Chapter_Code],
        c.[Chapter_Name],
        @NoticeDate,
        @ReceivedAt,
        c.[Points_Required],
        c.[Available_Points],
        N'posted',
        p.[RedemptionID],
        tx.[TransactionID],
        @RunID,
        c.[Source_Payload_Json]
    FROM @Candidates AS c
    INNER JOIN @PostingRequests AS p
        ON p.[Source_Key] = c.[Source_Key]
    INNER JOIN @InsertedTransactions AS tx
        ON tx.[Source_Key] = c.[Source_Key]
    WHERE c.[Existing_NoticeID] IS NULL;

    DECLARE @InputRowCount int = (SELECT COUNT(*) FROM @Candidates);
    DECLARE @ExistingNoticeCount int = (SELECT COUNT(*) FROM @Candidates WHERE [Existing_NoticeID] IS NOT NULL);
    DECLARE @NewPostedCount int = (SELECT COUNT(*) FROM @InsertedTransactions);
    DECLARE @AlreadyPostedCount int = (SELECT COUNT(*) FROM @Candidates WHERE [Decision] = N'already_posted');
    DECLARE @InsufficientPointsCount int = (SELECT COUNT(*) FROM @Candidates WHERE [Decision] = N'insufficient_points');
    DECLARE @ChapterNotFoundCount int = (SELECT COUNT(*) FROM @Candidates WHERE [Decision] = N'chapter_not_found');
    DECLARE @DebitedPointTotal int =
    (
        SELECT COALESCE(SUM(-t.[Points_Delta]), 0)
        FROM [rewards].[transactions] AS t
        INNER JOIN @InsertedTransactions AS inserted
            ON inserted.[TransactionID] = t.[TransactionID]
    );

    UPDATE [rewards].[reward_runs]
    SET
        [Completed_At] = SYSUTCDATETIME(),
        [Status] = N'succeeded',
        [SummaryJson] =
        (
            SELECT
                @SourceType AS [processor],
                @MessageId AS [message_id],
                @ReceivedAt AS [received_at],
                @NoticeDate AS [notice_date],
                @PointsPerRenewal AS [points_per_renewal],
                @InputRowCount AS [input_row_count],
                @ExistingNoticeCount AS [existing_notice_count],
                @NewPostedCount AS [new_post_count],
                @AlreadyPostedCount AS [already_posted_count],
                @InsufficientPointsCount AS [insufficient_points_count],
                @ChapterNotFoundCount AS [chapter_not_found_count],
                @DebitedPointTotal AS [debited_point_total]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        )
    WHERE [RunID] = @RunID;

    SELECT
        COALESCE(result.[RunID], @RunID) AS [RunID],
        c.[Source_Row_Number],
        @MessageId AS [Message_ID],
        c.[ChapterID],
        COALESCE(result.[Chapter_Code], c.[Chapter_Code]) AS [Chapter_Code],
        COALESCE(result.[Chapter_Name], c.[Chapter_Name]) AS [Chapter_Name],
        @NoticeDate AS [Notice_Date],
        c.[Points_Required],
        COALESCE(result.[Available_Points], c.[Available_Points]) AS [Available_Points],
        COALESCE(result.[Decision], c.[Decision]) AS [Decision],
        COALESCE(result.[RedemptionID], c.[Existing_RedemptionID]) AS [RedemptionID],
        COALESCE(result.[TransactionID], c.[Existing_TransactionID]) AS [TransactionID],
        result.[ClubExpress_Renewal_Message_ID],
        result.[ClubExpress_Renewed_At],
        result.[ClubExpress_Renewal_Recorded_At]
    FROM @Candidates AS c
    LEFT JOIN [rewards].[chapter_renewal_notice_results] AS result
        ON result.[Message_ID] = @MessageId
       AND result.[ChapterID] = c.[ChapterID]
    ORDER BY c.[Source_Row_Number], c.[ChapterID];
END;
GO
