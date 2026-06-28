-- Live Azure SQL stored procedure export.
-- Source object: [membership].[sp_process_membership_renewal].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
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
