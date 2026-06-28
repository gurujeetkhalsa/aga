-- Live Azure SQL stored procedure export.
-- Source object: [membership].[sp_record_clubexpress_parsed_event].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [membership].[sp_record_clubexpress_parsed_event]
    @MessageId nvarchar(256),
    @EventKey nvarchar(512),
    @MessageType nvarchar(64),
    @EventType nvarchar(64),
    @ReceivedAt datetime2(0),
    @EventDate date = NULL,
    @AGAID int = NULL,
    @ChapterID int = NULL,
    @ParsedItemCount int = 1,
    @Sender nvarchar(512) = NULL,
    @Subject nvarchar(512) = NULL,
    @BlobPath nvarchar(1024) = NULL,
    @ParsedPayloadJson nvarchar(max),
    @DownstreamPayloadJson nvarchar(max) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'membership.clubexpress_parsed_events', N'U') IS NULL
        THROW 54000, N'membership.clubexpress_parsed_events does not exist. Apply clubexpress_parsed_event_staging.sql first.', 1;

    DECLARE @NormalizedMessageId nvarchar(256) = NULLIF(LTRIM(RTRIM(COALESCE(@MessageId, N''))), N'');
    DECLARE @NormalizedEventKey nvarchar(512) = NULLIF(LTRIM(RTRIM(COALESCE(@EventKey, N''))), N'');
    DECLARE @NormalizedMessageType nvarchar(64) = NULLIF(LTRIM(RTRIM(COALESCE(@MessageType, N''))), N'');
    DECLARE @NormalizedEventType nvarchar(64) = NULLIF(LTRIM(RTRIM(COALESCE(@EventType, N''))), N'');

    IF @NormalizedMessageId IS NULL OR @NormalizedEventKey IS NULL OR @NormalizedMessageType IS NULL OR @NormalizedEventType IS NULL
        THROW 54001, N'MessageId, EventKey, MessageType, and EventType are required.', 1;

    IF @ParsedItemCount < 0
        THROW 54002, N'ParsedItemCount must be non-negative.', 1;

    IF @ParsedPayloadJson IS NULL OR ISJSON(@ParsedPayloadJson) <> 1
        THROW 54003, N'ParsedPayloadJson must be valid JSON.', 1;

    IF @DownstreamPayloadJson IS NOT NULL AND ISJSON(@DownstreamPayloadJson) <> 1
        THROW 54004, N'DownstreamPayloadJson must be valid JSON when supplied.', 1;

    IF EXISTS
    (
        SELECT 1
        FROM [membership].[clubexpress_parsed_events] WITH (UPDLOCK, HOLDLOCK)
        WHERE [Event_Key] = @NormalizedEventKey
    )
    BEGIN
        UPDATE [membership].[clubexpress_parsed_events]
        SET
            [Message_ID] = @NormalizedMessageId,
            [Message_Type] = @NormalizedMessageType,
            [Event_Type] = @NormalizedEventType,
            [Received_At] = @ReceivedAt,
            [Event_Date] = @EventDate,
            [AGAID] = @AGAID,
            [ChapterID] = @ChapterID,
            [Parsed_Item_Count] = @ParsedItemCount,
            [Sender] = NULLIF(@Sender, N''),
            [Subject] = NULLIF(@Subject, N''),
            [Blob_Path] = NULLIF(@BlobPath, N''),
            [Parsed_Payload_Json] = @ParsedPayloadJson,
            [Downstream_Payload_Json] = @DownstreamPayloadJson,
            [Status] = CASE WHEN [Status] = N'processed' THEN [Status] ELSE N'staged' END,
            [Last_Error_Message] = CASE WHEN [Status] = N'processed' THEN [Last_Error_Message] ELSE NULL END,
            [Updated_At] = SYSUTCDATETIME()
        WHERE [Event_Key] = @NormalizedEventKey;
    END
    ELSE
    BEGIN
        INSERT INTO [membership].[clubexpress_parsed_events]
        (
            [Message_ID],
            [Event_Key],
            [Message_Type],
            [Event_Type],
            [Received_At],
            [Event_Date],
            [AGAID],
            [ChapterID],
            [Parsed_Item_Count],
            [Sender],
            [Subject],
            [Blob_Path],
            [Parsed_Payload_Json],
            [Downstream_Payload_Json],
            [Status]
        )
        VALUES
        (
            @NormalizedMessageId,
            @NormalizedEventKey,
            @NormalizedMessageType,
            @NormalizedEventType,
            @ReceivedAt,
            @EventDate,
            @AGAID,
            @ChapterID,
            @ParsedItemCount,
            NULLIF(@Sender, N''),
            NULLIF(@Subject, N''),
            NULLIF(@BlobPath, N''),
            @ParsedPayloadJson,
            @DownstreamPayloadJson,
            N'staged'
        );
    END;
END;
GO
