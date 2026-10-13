-- Live Azure SQL metadata; database aga-membership; SQL_STORED_PROCEDURE
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   PROCEDURE [clubexpress].[sp_start_chapter_finder_update_push_attempt]
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
