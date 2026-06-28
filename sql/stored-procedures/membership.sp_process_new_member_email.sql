-- Live Azure SQL stored procedure export.
-- Source object: [membership].[sp_process_new_member_email].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
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
