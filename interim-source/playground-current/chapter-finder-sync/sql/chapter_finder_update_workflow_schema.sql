-- Copyright 2026, American Go Association, All rights reserved

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'clubexpress') IS NULL
    EXEC(N'CREATE SCHEMA [clubexpress]');
GO

IF OBJECT_ID(N'clubexpress.chapter_finder_update_requests', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[chapter_finder_update_requests]
    (
        [Chapter_Finder_Update_Request_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Request_Key] uniqueidentifier NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Request_Key] DEFAULT NEWID(),
        [Chapter_Finder_Entry_ID] bigint NOT NULL,
        [Club_ID] int NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Club_ID] DEFAULT 454497,
        [Chapter_Finder_ID] int NOT NULL,
        [ClubExpress_Chapter_ID] int NULL,
        [Chapter_Name_At_Submission] nvarchar(256) NOT NULL,
        [Request_Source] nvarchar(64) NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Request_Source] DEFAULT N'chapter_update_form',
        [Status] nvarchar(32) NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Status] DEFAULT N'submitted',
        [Submitted_By_Name] nvarchar(256) NULL,
        [Submitted_By_Email] nvarchar(320) NULL,
        [Submitted_By_Role] nvarchar(128) NULL,
        [Submitter_Notes] nvarchar(max) NULL,
        [Base_Content_Sha256] char(64) NOT NULL,
        [Proposed_Content_Sha256] char(64) NOT NULL,
        [Proposed_Json] nvarchar(max) NOT NULL,
        [Normalized_Proposed_Json] nvarchar(max) NOT NULL,
        [Field_Diff_Json] nvarchar(max) NOT NULL,
        [Changed_Field_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Changed_Field_Count] DEFAULT 0,
        [Reviewed_By] nvarchar(256) NULL,
        [Reviewed_At] datetime2(0) NULL,
        [Review_Notes] nvarchar(max) NULL,
        [Approved_At] datetime2(0) NULL,
        [Approved_By] nvarchar(256) NULL,
        [Rejected_At] datetime2(0) NULL,
        [Rejected_By] nvarchar(256) NULL,
        [Pushed_At] datetime2(0) NULL,
        [Push_Status] nvarchar(32) NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Push_Status] DEFAULT N'not_ready',
        [Push_Attempt_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Push_Attempt_Count] DEFAULT 0,
        [Last_Push_Attempt_At] datetime2(0) NULL,
        [Last_Push_Error] nvarchar(max) NULL,
        [Last_Push_Result_Json] nvarchar(max) NULL,
        [Verification_Run_ID] bigint NULL,
        [Verified_At] datetime2(0) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Created_At] DEFAULT SYSUTCDATETIME(),
        [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Updated_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_chapter_finder_update_requests] PRIMARY KEY CLUSTERED ([Chapter_Finder_Update_Request_ID]),
        CONSTRAINT [UQ_chapter_finder_update_requests_Request_Key] UNIQUE ([Request_Key]),
        CONSTRAINT [CK_chapter_finder_update_requests_Status] CHECK ([Status] IN
            (N'draft', N'submitted', N'needs_review', N'approved', N'pending_push', N'pushing', N'pushed', N'verified', N'rejected', N'cancelled', N'failed')),
        CONSTRAINT [CK_chapter_finder_update_requests_Push_Status] CHECK ([Push_Status] IN
            (N'not_ready', N'pending', N'pushing', N'pushed', N'verified', N'failed', N'skipped')),
        CONSTRAINT [CK_chapter_finder_update_requests_Proposed_Json] CHECK (ISJSON([Proposed_Json]) = 1),
        CONSTRAINT [CK_chapter_finder_update_requests_Normalized_Proposed_Json] CHECK (ISJSON([Normalized_Proposed_Json]) = 1),
        CONSTRAINT [CK_chapter_finder_update_requests_Field_Diff_Json] CHECK (ISJSON([Field_Diff_Json]) = 1),
        CONSTRAINT [CK_chapter_finder_update_requests_Last_Push_Result_Json] CHECK ([Last_Push_Result_Json] IS NULL OR ISJSON([Last_Push_Result_Json]) = 1),
        CONSTRAINT [CK_chapter_finder_update_requests_Changed_Field_Count] CHECK ([Changed_Field_Count] >= 0),
        CONSTRAINT [CK_chapter_finder_update_requests_Push_Attempt_Count] CHECK ([Push_Attempt_Count] >= 0),
        CONSTRAINT [FK_chapter_finder_update_requests_Entry] FOREIGN KEY ([Chapter_Finder_Entry_ID]) REFERENCES [clubexpress].[chapter_finder_entries] ([Chapter_Finder_Entry_ID]),
        CONSTRAINT [FK_chapter_finder_update_requests_Verification_Run] FOREIGN KEY ([Verification_Run_ID]) REFERENCES [clubexpress].[chapter_finder_sync_runs] ([Chapter_Finder_Sync_Run_ID])
    );
END;
GO

IF OBJECT_ID(N'clubexpress.chapter_finder_update_request_fields', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[chapter_finder_update_request_fields]
    (
        [Chapter_Finder_Update_Request_Field_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Chapter_Finder_Update_Request_ID] bigint NOT NULL,
        [Field_Name] nvarchar(128) NOT NULL,
        [Current_Value] nvarchar(max) NULL,
        [Proposed_Value] nvarchar(max) NULL,
        [Change_Type] nvarchar(32) NOT NULL,
        [Display_Order] int NOT NULL CONSTRAINT [DF_chapter_finder_update_request_fields_Display_Order] DEFAULT 0,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_update_request_fields_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_chapter_finder_update_request_fields] PRIMARY KEY CLUSTERED ([Chapter_Finder_Update_Request_Field_ID]),
        CONSTRAINT [UQ_chapter_finder_update_request_fields_Request_Field] UNIQUE ([Chapter_Finder_Update_Request_ID], [Field_Name]),
        CONSTRAINT [CK_chapter_finder_update_request_fields_Change_Type] CHECK ([Change_Type] IN (N'added', N'changed', N'removed')),
        CONSTRAINT [FK_chapter_finder_update_request_fields_Request] FOREIGN KEY ([Chapter_Finder_Update_Request_ID]) REFERENCES [clubexpress].[chapter_finder_update_requests] ([Chapter_Finder_Update_Request_ID]) ON DELETE CASCADE
    );
END;
GO

IF OBJECT_ID(N'clubexpress.chapter_finder_update_push_attempts', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[chapter_finder_update_push_attempts]
    (
        [Chapter_Finder_Update_Push_Attempt_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Chapter_Finder_Update_Request_ID] bigint NOT NULL,
        [Attempt_Number] int NOT NULL,
        [Started_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_update_push_attempts_Started_At] DEFAULT SYSUTCDATETIME(),
        [Completed_At] datetime2(0) NULL,
        [Status] nvarchar(32) NOT NULL CONSTRAINT [DF_chapter_finder_update_push_attempts_Status] DEFAULT N'running',
        [Operator] nvarchar(256) NULL,
        [ClubExpress_Edit_Url] nvarchar(1024) NULL,
        [Payload_Json] nvarchar(max) NOT NULL,
        [Result_Json] nvarchar(max) NULL,
        [Error_Message] nvarchar(max) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_update_push_attempts_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_chapter_finder_update_push_attempts] PRIMARY KEY CLUSTERED ([Chapter_Finder_Update_Push_Attempt_ID]),
        CONSTRAINT [UQ_chapter_finder_update_push_attempts_Request_Attempt] UNIQUE ([Chapter_Finder_Update_Request_ID], [Attempt_Number]),
        CONSTRAINT [CK_chapter_finder_update_push_attempts_Status] CHECK ([Status] IN (N'running', N'succeeded', N'failed', N'cancelled')),
        CONSTRAINT [CK_chapter_finder_update_push_attempts_Payload_Json] CHECK (ISJSON([Payload_Json]) = 1),
        CONSTRAINT [CK_chapter_finder_update_push_attempts_Result_Json] CHECK ([Result_Json] IS NULL OR ISJSON([Result_Json]) = 1),
        CONSTRAINT [FK_chapter_finder_update_push_attempts_Request] FOREIGN KEY ([Chapter_Finder_Update_Request_ID]) REFERENCES [clubexpress].[chapter_finder_update_requests] ([Chapter_Finder_Update_Request_ID])
    );
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_chapter_finder_update_requests_Status_Push'
      AND [object_id] = OBJECT_ID(N'clubexpress.chapter_finder_update_requests')
)
BEGIN
    CREATE INDEX [IX_chapter_finder_update_requests_Status_Push]
        ON [clubexpress].[chapter_finder_update_requests] ([Status], [Push_Status], [Created_At])
        INCLUDE ([Chapter_Finder_Entry_ID], [Chapter_Finder_ID], [ClubExpress_Chapter_ID], [Chapter_Name_At_Submission]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_chapter_finder_update_requests_Entry_Created'
      AND [object_id] = OBJECT_ID(N'clubexpress.chapter_finder_update_requests')
)
BEGIN
    CREATE INDEX [IX_chapter_finder_update_requests_Entry_Created]
        ON [clubexpress].[chapter_finder_update_requests] ([Chapter_Finder_Entry_ID], [Created_At] DESC);
END;
GO

CREATE OR ALTER VIEW [clubexpress].[v_chapter_finder_update_review]
AS
SELECT
    r.[Chapter_Finder_Update_Request_ID],
    r.[Request_Key],
    r.[Status],
    r.[Push_Status],
    r.[Chapter_Finder_Entry_ID],
    r.[Chapter_Finder_ID],
    r.[ClubExpress_Chapter_ID],
    r.[Chapter_Name_At_Submission],
    e.[Chapter_Name] AS [Current_Chapter_Name],
    e.[Contact_Name] AS [Current_Contact_Name],
    e.[Contact_Email] AS [Current_Contact_Email],
    e.[Website_Url] AS [Current_Website_Url],
    e.[Description_Text] AS [Current_Description_Text],
    r.[Submitted_By_Name],
    r.[Submitted_By_Email],
    r.[Submitted_By_Role],
    r.[Changed_Field_Count],
    r.[Field_Diff_Json],
    r.[Submitter_Notes],
    r.[Reviewed_By],
    r.[Reviewed_At],
    r.[Review_Notes],
    r.[Created_At],
    r.[Updated_At]
FROM [clubexpress].[chapter_finder_update_requests] AS r
INNER JOIN [clubexpress].[chapter_finder_entries] AS e
    ON e.[Chapter_Finder_Entry_ID] = r.[Chapter_Finder_Entry_ID];
GO

CREATE OR ALTER VIEW [clubexpress].[v_chapter_finder_update_push_queue]
AS
SELECT
    r.[Chapter_Finder_Update_Request_ID],
    r.[Request_Key],
    r.[Chapter_Finder_Entry_ID],
    r.[Chapter_Finder_ID],
    r.[ClubExpress_Chapter_ID],
    r.[Chapter_Name_At_Submission],
    r.[Status],
    r.[Push_Status],
    r.[Push_Attempt_Count],
    r.[Approved_At],
    r.[Approved_By],
    r.[Normalized_Proposed_Json],
    r.[Field_Diff_Json],
    e.[Content_Sha256] AS [Current_Public_Content_Sha256],
    r.[Base_Content_Sha256],
    r.[Proposed_Content_Sha256],
    r.[Last_Push_Attempt_At],
    r.[Last_Push_Error]
FROM [clubexpress].[chapter_finder_update_requests] AS r
INNER JOIN [clubexpress].[chapter_finder_entries] AS e
    ON e.[Chapter_Finder_Entry_ID] = r.[Chapter_Finder_Entry_ID]
WHERE r.[Status] = N'pending_push'
  AND r.[Push_Status] = N'pending';
GO

CREATE OR ALTER PROCEDURE [clubexpress].[sp_approve_chapter_finder_update_request]
    @ChapterFinderUpdateRequestID bigint,
    @ApprovedBy nvarchar(256),
    @ReviewNotes nvarchar(max) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    UPDATE [clubexpress].[chapter_finder_update_requests]
    SET
        [Status] = N'pending_push',
        [Push_Status] = N'pending',
        [Reviewed_By] = @ApprovedBy,
        [Reviewed_At] = SYSUTCDATETIME(),
        [Review_Notes] = @ReviewNotes,
        [Approved_By] = @ApprovedBy,
        [Approved_At] = SYSUTCDATETIME(),
        [Updated_At] = SYSUTCDATETIME()
    WHERE [Chapter_Finder_Update_Request_ID] = @ChapterFinderUpdateRequestID
      AND [Status] IN (N'submitted', N'needs_review', N'approved');

    IF @@ROWCOUNT = 0
        THROW 50201, 'Chapter Finder update request was not approvable.', 1;

    SELECT *
    FROM [clubexpress].[v_chapter_finder_update_push_queue]
    WHERE [Chapter_Finder_Update_Request_ID] = @ChapterFinderUpdateRequestID;
END;
GO

CREATE OR ALTER PROCEDURE [clubexpress].[sp_reject_chapter_finder_update_request]
    @ChapterFinderUpdateRequestID bigint,
    @RejectedBy nvarchar(256),
    @ReviewNotes nvarchar(max) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    UPDATE [clubexpress].[chapter_finder_update_requests]
    SET
        [Status] = N'rejected',
        [Push_Status] = N'skipped',
        [Reviewed_By] = @RejectedBy,
        [Reviewed_At] = SYSUTCDATETIME(),
        [Review_Notes] = @ReviewNotes,
        [Rejected_By] = @RejectedBy,
        [Rejected_At] = SYSUTCDATETIME(),
        [Updated_At] = SYSUTCDATETIME()
    WHERE [Chapter_Finder_Update_Request_ID] = @ChapterFinderUpdateRequestID
      AND [Status] IN (N'submitted', N'needs_review', N'approved', N'pending_push');

    IF @@ROWCOUNT = 0
        THROW 50202, 'Chapter Finder update request was not rejectable.', 1;
END;
GO

CREATE OR ALTER PROCEDURE [clubexpress].[sp_start_chapter_finder_update_push_attempt]
    @ChapterFinderUpdateRequestID bigint,
    @Operator nvarchar(256) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @AttemptNumber int;
    DECLARE @PayloadJson nvarchar(max);
    DECLARE @Attempt table
    (
        [Chapter_Finder_Update_Push_Attempt_ID] bigint NOT NULL,
        [Attempt_Number] int NOT NULL
    );

    BEGIN TRANSACTION;

    SELECT
        @AttemptNumber = [Push_Attempt_Count] + 1,
        @PayloadJson = [Normalized_Proposed_Json]
    FROM [clubexpress].[chapter_finder_update_requests] WITH (UPDLOCK, HOLDLOCK)
    WHERE [Chapter_Finder_Update_Request_ID] = @ChapterFinderUpdateRequestID
      AND [Status] = N'pending_push'
      AND [Push_Status] = N'pending';

    IF @AttemptNumber IS NULL
    BEGIN
        ROLLBACK TRANSACTION;
        THROW 50203, 'Chapter Finder update request is not pending push.', 1;
    END;

    UPDATE [clubexpress].[chapter_finder_update_requests]
    SET
        [Status] = N'pushing',
        [Push_Status] = N'pushing',
        [Push_Attempt_Count] = @AttemptNumber,
        [Last_Push_Attempt_At] = SYSUTCDATETIME(),
        [Last_Push_Error] = NULL,
        [Updated_At] = SYSUTCDATETIME()
    WHERE [Chapter_Finder_Update_Request_ID] = @ChapterFinderUpdateRequestID;

    INSERT INTO [clubexpress].[chapter_finder_update_push_attempts]
    (
        [Chapter_Finder_Update_Request_ID],
        [Attempt_Number],
        [Operator],
        [Payload_Json]
    )
    OUTPUT inserted.[Chapter_Finder_Update_Push_Attempt_ID], inserted.[Attempt_Number]
    INTO @Attempt
    VALUES
    (
        @ChapterFinderUpdateRequestID,
        @AttemptNumber,
        @Operator,
        @PayloadJson
    );

    COMMIT TRANSACTION;

    SELECT
        attempt.[Chapter_Finder_Update_Push_Attempt_ID],
        attempt.[Attempt_Number],
        request.[Chapter_Finder_Update_Request_ID],
        request.[Request_Key],
        request.[Chapter_Finder_ID],
        request.[ClubExpress_Chapter_ID],
        request.[Chapter_Name_At_Submission],
        request.[Normalized_Proposed_Json],
        request.[Field_Diff_Json]
    FROM @Attempt AS attempt
    CROSS JOIN [clubexpress].[chapter_finder_update_requests] AS request
    WHERE request.[Chapter_Finder_Update_Request_ID] = @ChapterFinderUpdateRequestID;
END;
GO

CREATE OR ALTER PROCEDURE [clubexpress].[sp_complete_chapter_finder_update_push_attempt]
    @ChapterFinderUpdatePushAttemptID bigint,
    @Succeeded bit,
    @ResultJson nvarchar(max) = NULL,
    @ErrorMessage nvarchar(max) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @ResultJson IS NOT NULL AND ISJSON(@ResultJson) <> 1
        THROW 50204, 'ResultJson must be valid JSON.', 1;

    DECLARE @RequestID bigint;
    SELECT @RequestID = [Chapter_Finder_Update_Request_ID]
    FROM [clubexpress].[chapter_finder_update_push_attempts]
    WHERE [Chapter_Finder_Update_Push_Attempt_ID] = @ChapterFinderUpdatePushAttemptID;

    IF @RequestID IS NULL
        THROW 50205, 'Chapter Finder update push attempt was not found.', 1;

    BEGIN TRANSACTION;

    UPDATE [clubexpress].[chapter_finder_update_push_attempts]
    SET
        [Completed_At] = SYSUTCDATETIME(),
        [Status] = CASE WHEN @Succeeded = 1 THEN N'succeeded' ELSE N'failed' END,
        [Result_Json] = @ResultJson,
        [Error_Message] = @ErrorMessage
    WHERE [Chapter_Finder_Update_Push_Attempt_ID] = @ChapterFinderUpdatePushAttemptID;

    UPDATE [clubexpress].[chapter_finder_update_requests]
    SET
        [Status] = CASE WHEN @Succeeded = 1 THEN N'pushed' ELSE N'failed' END,
        [Push_Status] = CASE WHEN @Succeeded = 1 THEN N'pushed' ELSE N'failed' END,
        [Pushed_At] = CASE WHEN @Succeeded = 1 THEN SYSUTCDATETIME() ELSE [Pushed_At] END,
        [Last_Push_Result_Json] = @ResultJson,
        [Last_Push_Error] = @ErrorMessage,
        [Updated_At] = SYSUTCDATETIME()
    WHERE [Chapter_Finder_Update_Request_ID] = @RequestID;

    COMMIT TRANSACTION;
END;
GO

