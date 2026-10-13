-- Live Azure SQL metadata; database aga-membership; SQL_STORED_PROCEDURE
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   PROCEDURE [clubexpress].[sp_complete_chapter_finder_update_push_attempt]
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
