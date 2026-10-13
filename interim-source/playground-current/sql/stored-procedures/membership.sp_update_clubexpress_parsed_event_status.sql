-- Copyright 2026, American Go Association, All rights reserved

-- Live Azure SQL stored procedure export.
-- Source object: [membership].[sp_update_clubexpress_parsed_event_status].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
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
