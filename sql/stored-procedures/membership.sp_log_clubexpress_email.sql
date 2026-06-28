-- Live Azure SQL stored procedure export.
-- Source object: [membership].[sp_log_clubexpress_email].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
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
