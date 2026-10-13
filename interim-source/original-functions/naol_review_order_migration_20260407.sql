IF COL_LENGTH(N'[integration].[journal_review_member_match]', N'ReviewGameOrder') IS NULL
BEGIN
    ALTER TABLE [integration].[journal_review_member_match]
    ADD [ReviewGameOrder] INT NULL;
END
GO

CREATE OR ALTER PROCEDURE [membership].[sp_process_journal_news_email]
    @MessageId NVARCHAR(255),
    @ReceivedAt DATETIME2(7),
    @JournalDate DATE,
    @MatchesJson NVARCHAR(MAX),
    @ReviewMatchesJson NVARCHAR(MAX) = NULL,
    @Sender NVARCHAR(255) = NULL,
    @Subject NVARCHAR(500) = NULL,
    @BlobPath NVARCHAR(400) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @MessageId IS NULL OR LTRIM(RTRIM(@MessageId)) = N''
        THROW 50140, 'MessageId is required.', 1;

    IF @ReceivedAt IS NULL
        THROW 50141, 'ReceivedAt is required.', 1;

    IF @JournalDate IS NULL
        THROW 50142, 'JournalDate is required.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE [integration].[clubexpress_email_log] AS target
        USING
        (
            SELECT
                @MessageId AS MessageId,
                N'american_go_e_journal' AS MessageType
        ) AS source
        ON target.MessageId = source.MessageId
           AND target.MessageType = source.MessageType
        WHEN NOT MATCHED THEN
            INSERT (MessageId, MessageType, Sender, Subject, ReceivedAt, BlobPath, Status)
            VALUES (@MessageId, source.MessageType, @Sender, @Subject, @ReceivedAt, @BlobPath, N'received')
        WHEN MATCHED THEN
            UPDATE
            SET Sender = COALESCE(@Sender, target.Sender),
                Subject = COALESCE(@Subject, target.Subject),
                ReceivedAt = @ReceivedAt,
                BlobPath = COALESCE(@BlobPath, target.BlobPath),
                LastUpdated = SYSDATETIME();

        IF EXISTS
        (
            SELECT 1
            FROM [integration].[clubexpress_email_log]
            WHERE MessageId = @MessageId
              AND MessageType = N'american_go_e_journal'
              AND Status IN (N'processed', N'ignored')
        )
        BEGIN
            COMMIT TRANSACTION;
            RETURN;
        END;

        DELETE FROM [integration].[journal_article_member_match]
        WHERE MessageId = @MessageId;

        DELETE FROM [integration].[journal_review_member_match]
        WHERE MessageId = @MessageId;

        INSERT INTO [integration].[journal_article_member_match]
        (
            [MessageId],
            [JournalDate],
            [AGAID],
            [MatchedName],
            [ArticleTitle],
            [ArticleLink]
        )
        SELECT DISTINCT
            @MessageId,
            @JournalDate,
            matches.[AGAID],
            LEFT(matches.[MatchedName], 200),
            LEFT(matches.[ArticleTitle], 500),
            LEFT(matches.[ArticleLink], 1000)
        FROM OPENJSON(COALESCE(@MatchesJson, N'[]'))
        WITH
        (
            [AGAID] INT '$.AGAID',
            [MatchedName] NVARCHAR(200) '$.MatchedName',
            [ArticleTitle] NVARCHAR(500) '$.ArticleTitle',
            [ArticleLink] NVARCHAR(1000) '$.ArticleLink'
        ) AS matches
        WHERE EXISTS
        (
            SELECT 1
            FROM [ratings].[ratings] AS r
            WHERE r.[Pin_Player] = matches.[AGAID]
        )
          AND matches.[AGAID] IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(matches.[MatchedName])), N'') IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(matches.[ArticleTitle])), N'') IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(matches.[ArticleLink])), N'') IS NOT NULL;

        ;WITH review_matches AS
        (
            SELECT
                TRY_CONVERT(INT, json_rows.[AGAID]) AS [AGAID],
                json_rows.[MatchedName],
                json_rows.[ReviewTitle],
                json_rows.[BlogLink],
                json_rows.[ReviewerName],
                json_rows.[ReviewerRank],
                json_rows.[ReviewedPlayerName],
                json_rows.[ReviewedPlayerRank],
                json_rows.[OpponentName],
                json_rows.[OpponentRank],
                json_rows.[GameLink],
                json_rows.[VideoLink],
                TRY_CONVERT(INT, json_rows.[VideoReviewCount]) AS [VideoReviewCount],
                TRY_CONVERT(INT, json_rows.[ReviewGameOrder]) AS [ReviewGameOrder],
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS [JsonRowNumber]
            FROM OPENJSON(COALESCE(@ReviewMatchesJson, N'[]'))
            WITH
            (
                [AGAID] NVARCHAR(50) '$.AGAID',
                [MatchedName] NVARCHAR(200) '$.MatchedName',
                [ReviewTitle] NVARCHAR(500) '$.ReviewTitle',
                [BlogLink] NVARCHAR(1000) '$.BlogLink',
                [ReviewerName] NVARCHAR(200) '$.ReviewerName',
                [ReviewerRank] NVARCHAR(40) '$.ReviewerRank',
                [ReviewedPlayerName] NVARCHAR(200) '$.ReviewedPlayerName',
                [ReviewedPlayerRank] NVARCHAR(40) '$.ReviewedPlayerRank',
                [OpponentName] NVARCHAR(200) '$.OpponentName',
                [OpponentRank] NVARCHAR(40) '$.OpponentRank',
                [GameLink] NVARCHAR(1000) '$.GameLink',
                [VideoLink] NVARCHAR(1000) '$.VideoLink',
                [VideoReviewCount] NVARCHAR(50) '$.VideoReviewCount',
                [ReviewGameOrder] NVARCHAR(50) '$.ReviewGameOrder'
            ) AS json_rows
        ),
        distinct_review_games AS
        (
            SELECT
                review_matches.[BlogLink],
                review_matches.[ReviewerName],
                review_matches.[ReviewerRank],
                review_matches.[VideoLink],
                review_matches.[GameLink],
                MIN(review_matches.[JsonRowNumber]) AS [FirstJsonRowNumber],
                MAX(review_matches.[VideoReviewCount]) AS [StoredVideoReviewCount],
                MAX(review_matches.[ReviewGameOrder]) AS [StoredReviewGameOrder]
            FROM review_matches
            WHERE NULLIF(LTRIM(RTRIM(review_matches.[GameLink])), N'') IS NOT NULL
            GROUP BY
                review_matches.[BlogLink],
                review_matches.[ReviewerName],
                review_matches.[ReviewerRank],
                review_matches.[VideoLink],
                review_matches.[GameLink]
        ),
        computed_review_groups AS
        (
            SELECT
                distinct_review_games.[BlogLink],
                distinct_review_games.[ReviewerName],
                distinct_review_games.[ReviewerRank],
                distinct_review_games.[VideoLink],
                distinct_review_games.[GameLink],
                COALESCE(
                    NULLIF(distinct_review_games.[StoredVideoReviewCount], 0),
                    COUNT(*) OVER (
                        PARTITION BY
                            distinct_review_games.[BlogLink],
                            distinct_review_games.[ReviewerName],
                            distinct_review_games.[ReviewerRank],
                            distinct_review_games.[VideoLink]
                    )
                ) AS [ComputedVideoReviewCount],
                COALESCE(
                    NULLIF(distinct_review_games.[StoredReviewGameOrder], 0),
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            distinct_review_games.[BlogLink],
                            distinct_review_games.[ReviewerName],
                            distinct_review_games.[ReviewerRank],
                            distinct_review_games.[VideoLink]
                        ORDER BY distinct_review_games.[FirstJsonRowNumber]
                    )
                ) AS [ComputedReviewGameOrder]
            FROM distinct_review_games
        )
        INSERT INTO [integration].[journal_review_member_match]
        (
            [MessageId],
            [JournalDate],
            [AGAID],
            [MatchedName],
            [ReviewTitle],
            [BlogLink],
            [ReviewerName],
            [ReviewerRank],
            [ReviewedPlayerName],
            [ReviewedPlayerRank],
            [OpponentName],
            [OpponentRank],
            [GameLink],
            [VideoLink],
            [VideoReviewCount],
            [ReviewGameOrder]
        )
        SELECT DISTINCT
            @MessageId,
            @JournalDate,
            matches.[AGAID],
            LEFT(matches.[MatchedName], 200),
            LEFT(matches.[ReviewTitle], 500),
            LEFT(matches.[BlogLink], 1000),
            LEFT(matches.[ReviewerName], 200),
            LEFT(matches.[ReviewerRank], 40),
            LEFT(matches.[ReviewedPlayerName], 200),
            LEFT(matches.[ReviewedPlayerRank], 40),
            LEFT(matches.[OpponentName], 200),
            LEFT(matches.[OpponentRank], 40),
            LEFT(matches.[GameLink], 1000),
            LEFT(matches.[VideoLink], 1000),
            computed.[ComputedVideoReviewCount],
            computed.[ComputedReviewGameOrder]
        FROM review_matches AS matches
        LEFT JOIN computed_review_groups AS computed
            ON ISNULL(computed.[BlogLink], N'') = ISNULL(matches.[BlogLink], N'')
           AND computed.[ReviewerName] = matches.[ReviewerName]
           AND computed.[ReviewerRank] = matches.[ReviewerRank]
           AND computed.[VideoLink] = matches.[VideoLink]
           AND ISNULL(computed.[GameLink], N'') = ISNULL(matches.[GameLink], N'')
        WHERE EXISTS
        (
            SELECT 1
            FROM [ratings].[ratings] AS r
            WHERE r.[Pin_Player] = matches.[AGAID]
        )
          AND matches.[AGAID] IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(matches.[MatchedName])), N'') IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(matches.[ReviewTitle])), N'') IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(matches.[ReviewerName])), N'') IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(matches.[ReviewerRank])), N'') IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(matches.[ReviewedPlayerName])), N'') IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(matches.[OpponentName])), N'') IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(matches.[VideoLink])), N'') IS NOT NULL;

        UPDATE [integration].[clubexpress_email_log]
        SET Status = N'processed',
            ErrorMessage = NULL,
            ProcessedAt = SYSDATETIME(),
            LastUpdated = SYSDATETIME()
        WHERE MessageId = @MessageId
          AND MessageType = N'american_go_e_journal';

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        UPDATE [integration].[clubexpress_email_log]
        SET Status = N'error',
            ErrorMessage = ERROR_MESSAGE(),
            LastUpdated = SYSDATETIME()
        WHERE MessageId = @MessageId
          AND MessageType = N'american_go_e_journal';

        THROW;
    END CATCH;
END;
GO


IF OBJECT_ID(N'[integration].[clubexpress_email_log]', N'U') IS NULL
BEGIN
    CREATE TABLE [integration].[clubexpress_email_log]
    (
        [EmailLogID] INT IDENTITY(1,1) NOT NULL,
        [MessageId] NVARCHAR(255) NOT NULL,
        [MessageType] NVARCHAR(50) NOT NULL,
        [Sender] NVARCHAR(255) NULL,
        [Subject] NVARCHAR(500) NULL,
        [ReceivedAt] DATETIME2(7) NOT NULL,
        [BlobPath] NVARCHAR(400) NULL,
        [Status] NVARCHAR(30) NOT NULL
            CONSTRAINT [DF_clubexpress_email_log_Status] DEFAULT (N'received'),
        [ErrorMessage] NVARCHAR(MAX) NULL,
        [ProcessedAt] DATETIME2(7) NULL,
        [CreatedDate] DATETIME2(7) NOT NULL
            CONSTRAINT [DF_clubexpress_email_log_CreatedDate] DEFAULT (SYSDATETIME()),
        [LastUpdated] DATETIME2(7) NOT NULL
            CONSTRAINT [DF_clubexpress_email_log_LastUpdated] DEFAULT (SYSDATETIME()),
        CONSTRAINT [PK_clubexpress_email_log] PRIMARY KEY CLUSTERED ([EmailLogID] ASC)
    );

    CREATE UNIQUE NONCLUSTERED INDEX [UX_clubexpress_email_log_MessageId_MessageType]
        ON [integration].[clubexpress_email_log] ([MessageId] ASC, [MessageType] ASC);
END;
GO

CREATE OR ALTER PROCEDURE [membership].[sp_log_clubexpress_email]
    @MessageId NVARCHAR(255),
    @MessageType NVARCHAR(50),
    @ReceivedAt DATETIME2(7),
    @Sender NVARCHAR(255) = NULL,
    @Subject NVARCHAR(500) = NULL,
    @BlobPath NVARCHAR(400) = NULL,
    @Status NVARCHAR(30) = NULL,
    @ErrorMessage NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @MessageId IS NULL OR LTRIM(RTRIM(@MessageId)) = N''
        THROW 50130, 'MessageId is required.', 1;

    IF @MessageType IS NULL OR LTRIM(RTRIM(@MessageType)) = N''
        THROW 50131, 'MessageType is required.', 1;

    IF @ReceivedAt IS NULL
        THROW 50132, 'ReceivedAt is required.', 1;

    MERGE [integration].[clubexpress_email_log] AS target
    USING
    (
        SELECT
            @MessageId AS MessageId,
            @MessageType AS MessageType
    ) AS source
    ON target.MessageId = source.MessageId
       AND target.MessageType = source.MessageType
    WHEN NOT MATCHED THEN
        INSERT (MessageId, MessageType, Sender, Subject, ReceivedAt, BlobPath, Status, ErrorMessage, ProcessedAt)
        VALUES
        (
            @MessageId,
            @MessageType,
            @Sender,
            @Subject,
            @ReceivedAt,
            @BlobPath,
            COALESCE(@Status, N'received'),
            @ErrorMessage,
            CASE WHEN @Status IN (N'processed', N'ignored') THEN SYSDATETIME() ELSE NULL END
        )
    WHEN MATCHED THEN
        UPDATE
        SET Sender = COALESCE(@Sender, target.Sender),
            Subject = COALESCE(@Subject, target.Subject),
            ReceivedAt = @ReceivedAt,
            BlobPath = COALESCE(@BlobPath, target.BlobPath),
            Status = COALESCE(@Status, target.Status),
            ErrorMessage = @ErrorMessage,
            ProcessedAt = CASE
                WHEN COALESCE(@Status, target.Status) IN (N'processed', N'ignored') THEN SYSDATETIME()
                ELSE target.ProcessedAt
            END,
            LastUpdated = SYSDATETIME();
END;
GO

CREATE OR ALTER PROCEDURE [membership].[sp_process_new_member_email]
    @MessageId NVARCHAR(255),
    @ReceivedAt DATETIME2(7),
    @AGAID INT,
    @MemberType NVARCHAR(50),
    @FirstName NVARCHAR(100),
    @LastName NVARCHAR(100),
    @EmailAddress NVARCHAR(255) = NULL,
    @JoinDate DATE,
    @ExpirationDate DATE,
    @Sender NVARCHAR(255) = NULL,
    @Subject NVARCHAR(500) = NULL,
    @BlobPath NVARCHAR(400) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @MessageId IS NULL OR LTRIM(RTRIM(@MessageId)) = N''
        THROW 50101, 'MessageId is required.', 1;

    IF @AGAID IS NULL
        THROW 50102, 'AGAID is required.', 1;

    IF @JoinDate IS NULL
        THROW 50103, 'JoinDate is required.', 1;

    IF @ExpirationDate IS NULL
        THROW 50104, 'ExpirationDate is required.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE [integration].[clubexpress_email_log] AS target
        USING
        (
            SELECT
                @MessageId AS MessageId,
                N'new_member_signup' AS MessageType
        ) AS source
        ON target.MessageId = source.MessageId
           AND target.MessageType = source.MessageType
        WHEN NOT MATCHED THEN
            INSERT (MessageId, MessageType, Sender, Subject, ReceivedAt, BlobPath, Status)
            VALUES (@MessageId, source.MessageType, @Sender, @Subject, @ReceivedAt, @BlobPath, N'received')
        WHEN MATCHED THEN
            UPDATE
            SET Sender = COALESCE(@Sender, target.Sender),
                Subject = COALESCE(@Subject, target.Subject),
                ReceivedAt = @ReceivedAt,
                BlobPath = COALESCE(@BlobPath, target.BlobPath),
                LastUpdated = SYSDATETIME();

        IF EXISTS
        (
            SELECT 1
            FROM [integration].[clubexpress_email_log]
            WHERE MessageId = @MessageId
              AND MessageType = N'new_member_signup'
              AND Status IN (N'processed', N'ignored')
        )
        BEGIN
            COMMIT TRANSACTION;
            RETURN;
        END;

        IF @AGAID >= 50000
        BEGIN
            UPDATE [integration].[clubexpress_email_log]
            SET Status = N'ignored',
                ErrorMessage = N'Test account signup email ignored because AGAID >= 50000.',
                ProcessedAt = SYSDATETIME(),
                LastUpdated = SYSDATETIME()
            WHERE MessageId = @MessageId
              AND MessageType = N'new_member_signup';

            COMMIT TRANSACTION;
            RETURN;
        END;

        IF UPPER(LTRIM(RTRIM(COALESCE(@MemberType, N'')))) = N'CHAPTER'
        BEGIN
            UPDATE [integration].[clubexpress_email_log]
            SET Status = N'ignored',
                ErrorMessage = N'Chapter membership email ignored.',
                ProcessedAt = SYSDATETIME(),
                LastUpdated = SYSDATETIME()
            WHERE MessageId = @MessageId
              AND MessageType = N'new_member_signup';

            COMMIT TRANSACTION;
            RETURN;
        END;

        DECLARE @MemberExists BIT = 0;
        DECLARE @OldExpiration DATE = NULL;

        SELECT
            @MemberExists = 1,
            @OldExpiration = ExpirationDate
        FROM [membership].[members] WITH (UPDLOCK, HOLDLOCK)
        WHERE AGAID = @AGAID;

        IF @MemberExists = 0
        BEGIN
            INSERT INTO [membership].[members]
            (
                AGAID,
                MemberType,
                FirstName,
                LastName,
                EmailAddress,
                Status,
                JoinDate,
                ExpirationDate,
                LastRenewalDate,
                CreatedDate,
                LastUpdated
            )
            VALUES
            (
                @AGAID,
                @MemberType,
                @FirstName,
                @LastName,
                @EmailAddress,
                N'Active',
                @JoinDate,
                @ExpirationDate,
                @JoinDate,
                SYSDATETIME(),
                SYSDATETIME()
            );

            INSERT INTO [membership].[membership_events]
            (
                AGAID,
                EventType,
                EventDate,
                Source,
                Details,
                OldExpiration,
                NewExpiration
            )
            VALUES
            (
                @AGAID,
                N'signup',
                SYSDATETIME(),
                N'clubexpress_email_new_member',
                CONCAT(N'MessageId=', @MessageId, N'; Subject=', COALESCE(@Subject, N'')),
                @OldExpiration,
                @ExpirationDate
            );
        END;
        ELSE
        BEGIN
            UPDATE [membership].[members]
            SET MemberType = COALESCE(@MemberType, MemberType),
                FirstName = COALESCE(@FirstName, FirstName),
                LastName = COALESCE(@LastName, LastName),
                EmailAddress = COALESCE(@EmailAddress, EmailAddress),
                Status = COALESCE(Status, N'Active'),
                JoinDate = COALESCE(JoinDate, @JoinDate),
                ExpirationDate = @ExpirationDate,
                LastRenewalDate = COALESCE(LastRenewalDate, @JoinDate),
                LastUpdated = SYSDATETIME()
            WHERE AGAID = @AGAID;

            INSERT INTO [membership].[membership_events]
            (
                AGAID,
                EventType,
                EventDate,
                Source,
                Details,
                OldExpiration,
                NewExpiration
            )
            VALUES
            (
                @AGAID,
                N'update',
                SYSDATETIME(),
                N'clubexpress_email_new_member',
                CONCAT(N'MessageId=', @MessageId, N'; Subject=', COALESCE(@Subject, N'')),
                @OldExpiration,
                @ExpirationDate
            );
        END;

        UPDATE [integration].[clubexpress_email_log]
        SET Status = N'processed',
            ErrorMessage = NULL,
            ProcessedAt = SYSDATETIME(),
            LastUpdated = SYSDATETIME()
        WHERE MessageId = @MessageId
          AND MessageType = N'new_member_signup';

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        UPDATE [integration].[clubexpress_email_log]
        SET Status = N'error',
            ErrorMessage = ERROR_MESSAGE(),
            LastUpdated = SYSDATETIME()
        WHERE MessageId = @MessageId
          AND MessageType = N'new_member_signup';

        THROW;
    END CATCH;
END;
GO

CREATE OR ALTER PROCEDURE [membership].[sp_process_membership_renewal]
    @MessageId NVARCHAR(255),
    @ReceivedAt DATETIME2(7),
    @AGAID INT,
    @ExpirationDate DATE,
    @PhoneNumber NVARCHAR(50) = NULL,
    @EmailAddress NVARCHAR(255) = NULL,
    @LoginName NVARCHAR(255) = NULL,
    @MemberType NVARCHAR(50) = NULL,
    @IsChapterMember BIT = 0,
    @Sender NVARCHAR(255) = NULL,
    @Subject NVARCHAR(500) = NULL,
    @BlobPath NVARCHAR(400) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @MessageId IS NULL OR LTRIM(RTRIM(@MessageId)) = N''
        THROW 50111, 'MessageId is required.', 1;

    IF @AGAID IS NULL
        THROW 50112, 'AGAID is required.', 1;

    IF @ExpirationDate IS NULL
        THROW 50113, 'ExpirationDate is required.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE [integration].[clubexpress_email_log] AS target
        USING
        (
            SELECT
                @MessageId AS MessageId,
                N'member_renewal' AS MessageType
        ) AS source
        ON target.MessageId = source.MessageId
           AND target.MessageType = source.MessageType
        WHEN NOT MATCHED THEN
            INSERT (MessageId, MessageType, Sender, Subject, ReceivedAt, BlobPath, Status)
            VALUES (@MessageId, source.MessageType, @Sender, @Subject, @ReceivedAt, @BlobPath, N'received')
        WHEN MATCHED THEN
            UPDATE
            SET Sender = COALESCE(@Sender, target.Sender),
                Subject = COALESCE(@Subject, target.Subject),
                ReceivedAt = @ReceivedAt,
                BlobPath = COALESCE(@BlobPath, target.BlobPath),
                LastUpdated = SYSDATETIME();

        IF EXISTS
        (
            SELECT 1
            FROM [integration].[clubexpress_email_log]
            WHERE MessageId = @MessageId
              AND MessageType = N'member_renewal'
              AND Status IN (N'processed', N'ignored')
        )
        BEGIN
            COMMIT TRANSACTION;
            RETURN;
        END;

        IF @AGAID >= 50000
        BEGIN
            UPDATE [integration].[clubexpress_email_log]
            SET Status = N'ignored',
                ErrorMessage = N'Test account renewal email ignored because AGAID >= 50000.',
                ProcessedAt = SYSDATETIME(),
                LastUpdated = SYSDATETIME()
            WHERE MessageId = @MessageId
              AND MessageType = N'member_renewal';

            COMMIT TRANSACTION;
            RETURN;
        END;

        IF @IsChapterMember = 1
        BEGIN
            UPDATE [integration].[clubexpress_email_log]
            SET Status = N'ignored',
                ErrorMessage = N'Chapter membership renewal email ignored.',
                ProcessedAt = SYSDATETIME(),
                LastUpdated = SYSDATETIME()
            WHERE MessageId = @MessageId
              AND MessageType = N'member_renewal';

            COMMIT TRANSACTION;
            RETURN;
        END;

        DECLARE @OldExpiration DATE;

        SELECT @OldExpiration = ExpirationDate
        FROM [membership].[members] WITH (UPDLOCK, HOLDLOCK)
        WHERE AGAID = @AGAID;

        IF @OldExpiration IS NULL
            THROW 50114, 'Renewal email referenced a member that does not exist.', 1;

        UPDATE [membership].[members]
        SET PhoneNumber = COALESCE(@PhoneNumber, PhoneNumber),
            EmailAddress = COALESCE(@EmailAddress, EmailAddress),
            LoginName = COALESCE(@LoginName, LoginName),
            MemberType = COALESCE(@MemberType, MemberType),
            Status = N'Active',
            ExpirationDate = @ExpirationDate,
            LastRenewalDate = CAST(@ReceivedAt AS DATE),
            LastUpdated = SYSDATETIME()
        WHERE AGAID = @AGAID;

        INSERT INTO [membership].[membership_events]
        (
            AGAID,
            EventType,
            EventDate,
            Source,
            Details,
            OldExpiration,
            NewExpiration
        )
        VALUES
        (
            @AGAID,
            N'renewal',
            SYSDATETIME(),
            N'clubexpress_email_renewal',
            CONCAT(N'MessageId=', @MessageId, N'; Subject=', COALESCE(@Subject, N'')),
            @OldExpiration,
            @ExpirationDate
        );

        UPDATE [integration].[clubexpress_email_log]
        SET Status = N'processed',
            ErrorMessage = NULL,
            ProcessedAt = SYSDATETIME(),
            LastUpdated = SYSDATETIME()
        WHERE MessageId = @MessageId
          AND MessageType = N'member_renewal';

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        UPDATE [integration].[clubexpress_email_log]
        SET Status = N'error',
            ErrorMessage = ERROR_MESSAGE(),
            LastUpdated = SYSDATETIME()
        WHERE MessageId = @MessageId
          AND MessageType = N'member_renewal';

        THROW;
    END CATCH;
END;
GO
