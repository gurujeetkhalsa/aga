-- Copyright 2026, American Go Association, All rights reserved

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'membership') IS NULL
    EXEC(N'CREATE SCHEMA [membership]');
GO

IF OBJECT_ID(N'membership.clubexpress_parsed_events', N'U') IS NULL
BEGIN
    CREATE TABLE [membership].[clubexpress_parsed_events]
    (
        [Parsed_Event_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Message_ID] nvarchar(256) NOT NULL,
        [Event_Key] nvarchar(512) NOT NULL,
        [Message_Type] nvarchar(64) NOT NULL,
        [Event_Type] nvarchar(64) NOT NULL,
        [Received_At] datetime2(0) NOT NULL,
        [Event_Date] date NULL,
        [AGAID] int NULL,
        [ChapterID] int NULL,
        [Parsed_Item_Count] int NOT NULL CONSTRAINT [DF_clubexpress_parsed_events_Item_Count] DEFAULT 1,
        [Sender] nvarchar(512) NULL,
        [Subject] nvarchar(512) NULL,
        [Blob_Path] nvarchar(1024) NULL,
        [Parsed_Payload_Json] nvarchar(max) NOT NULL,
        [Downstream_Payload_Json] nvarchar(max) NULL,
        [Status] nvarchar(32) NOT NULL CONSTRAINT [DF_clubexpress_parsed_events_Status] DEFAULT N'staged',
        [Attempt_Count] int NOT NULL CONSTRAINT [DF_clubexpress_parsed_events_Attempt_Count] DEFAULT 0,
        [Last_Processed_At] datetime2(0) NULL,
        [Last_Error_Message] nvarchar(max) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_clubexpress_parsed_events_Created_At] DEFAULT SYSUTCDATETIME(),
        [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_clubexpress_parsed_events_Updated_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_clubexpress_parsed_events] PRIMARY KEY CLUSTERED ([Parsed_Event_ID]),
        CONSTRAINT [UQ_clubexpress_parsed_events_Event_Key] UNIQUE ([Event_Key]),
        CONSTRAINT [CK_clubexpress_parsed_events_Status] CHECK ([Status] IN (N'staged', N'processing', N'processed', N'error', N'skipped')),
        CONSTRAINT [CK_clubexpress_parsed_events_Item_Count] CHECK ([Parsed_Item_Count] >= 0),
        CONSTRAINT [CK_clubexpress_parsed_events_Attempt_Count] CHECK ([Attempt_Count] >= 0)
    );
END;
GO

IF OBJECT_ID(N'membership.clubexpress_parsed_event_attempts', N'U') IS NULL
BEGIN
    CREATE TABLE [membership].[clubexpress_parsed_event_attempts]
    (
        [Attempt_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Parsed_Event_ID] bigint NOT NULL,
        [Event_Key] nvarchar(512) NOT NULL,
        [Status] nvarchar(32) NOT NULL,
        [Error_Message] nvarchar(max) NULL,
        [Result_Payload_Json] nvarchar(max) NULL,
        [Recorded_At] datetime2(0) NOT NULL CONSTRAINT [DF_clubexpress_parsed_event_attempts_Recorded_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_clubexpress_parsed_event_attempts] PRIMARY KEY CLUSTERED ([Attempt_ID]),
        CONSTRAINT [FK_clubexpress_parsed_event_attempts_Event_ID]
            FOREIGN KEY ([Parsed_Event_ID]) REFERENCES [membership].[clubexpress_parsed_events] ([Parsed_Event_ID]),
        CONSTRAINT [CK_clubexpress_parsed_event_attempts_Status] CHECK ([Status] IN (N'staged', N'processing', N'processed', N'error', N'skipped'))
    );
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_clubexpress_parsed_events_Message'
      AND [object_id] = OBJECT_ID(N'membership.clubexpress_parsed_events')
)
BEGIN
    CREATE INDEX [IX_clubexpress_parsed_events_Message]
        ON [membership].[clubexpress_parsed_events] ([Message_ID], [Event_Type])
        INCLUDE ([Status], [Received_At], [AGAID], [ChapterID]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_clubexpress_parsed_events_Status'
      AND [object_id] = OBJECT_ID(N'membership.clubexpress_parsed_events')
)
BEGIN
    CREATE INDEX [IX_clubexpress_parsed_events_Status]
        ON [membership].[clubexpress_parsed_events] ([Status], [Received_At], [Event_Type])
        INCLUDE ([Message_ID], [Event_Key], [AGAID], [ChapterID]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_clubexpress_parsed_event_attempts_Event'
      AND [object_id] = OBJECT_ID(N'membership.clubexpress_parsed_event_attempts')
)
BEGIN
    CREATE INDEX [IX_clubexpress_parsed_event_attempts_Event]
        ON [membership].[clubexpress_parsed_event_attempts] ([Parsed_Event_ID], [Recorded_At]);
END;
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

CREATE OR ALTER PROCEDURE [membership].[sp_update_clubexpress_parsed_event_status]
    @EventKey nvarchar(512),
    @Status nvarchar(32),
    @ErrorMessage nvarchar(max) = NULL,
    @ResultPayloadJson nvarchar(max) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'membership.clubexpress_parsed_events', N'U') IS NULL
       OR OBJECT_ID(N'membership.clubexpress_parsed_event_attempts', N'U') IS NULL
        THROW 54010, N'ClubExpress parsed-event staging tables do not exist. Apply clubexpress_parsed_event_staging.sql first.', 1;

    DECLARE @NormalizedEventKey nvarchar(512) = NULLIF(LTRIM(RTRIM(COALESCE(@EventKey, N''))), N'');
    DECLARE @NormalizedStatus nvarchar(32) = LTRIM(RTRIM(COALESCE(@Status, N'')));

    IF @NormalizedEventKey IS NULL
        THROW 54011, N'EventKey is required.', 1;

    IF @NormalizedStatus NOT IN (N'staged', N'processing', N'processed', N'error', N'skipped')
        THROW 54012, N'Unsupported ClubExpress parsed-event status.', 1;

    IF @ResultPayloadJson IS NOT NULL AND ISJSON(@ResultPayloadJson) <> 1
        THROW 54013, N'ResultPayloadJson must be valid JSON when supplied.', 1;

    DECLARE @ParsedEventID bigint;

    SELECT @ParsedEventID = [Parsed_Event_ID]
    FROM [membership].[clubexpress_parsed_events] WITH (UPDLOCK, HOLDLOCK)
    WHERE [Event_Key] = @NormalizedEventKey;

    IF @ParsedEventID IS NULL
        THROW 54014, N'ClubExpress parsed event was not found for the supplied EventKey.', 1;

    UPDATE [membership].[clubexpress_parsed_events]
    SET
        [Status] = @NormalizedStatus,
        [Attempt_Count] =
            [Attempt_Count] +
            CASE WHEN @NormalizedStatus IN (N'processed', N'error', N'skipped') THEN 1 ELSE 0 END,
        [Last_Processed_At] =
            CASE
                WHEN @NormalizedStatus IN (N'processed', N'error', N'skipped') THEN SYSUTCDATETIME()
                ELSE [Last_Processed_At]
            END,
        [Last_Error_Message] = CASE WHEN @NormalizedStatus = N'error' THEN @ErrorMessage ELSE NULL END,
        [Updated_At] = SYSUTCDATETIME()
    WHERE [Parsed_Event_ID] = @ParsedEventID;

    INSERT INTO [membership].[clubexpress_parsed_event_attempts]
    (
        [Parsed_Event_ID],
        [Event_Key],
        [Status],
        [Error_Message],
        [Result_Payload_Json]
    )
    VALUES
    (
        @ParsedEventID,
        @NormalizedEventKey,
        @NormalizedStatus,
        CASE WHEN @NormalizedStatus = N'error' THEN @ErrorMessage ELSE NULL END,
        @ResultPayloadJson
    );
END;
GO
