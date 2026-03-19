IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'integration')
BEGIN
    EXEC(N'CREATE SCHEMA [integration]');
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
