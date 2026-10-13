-- Live Azure SQL metadata; database aga-membership; SQL_STORED_PROCEDURE
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   PROCEDURE [clubexpress].[sp_approve_chapter_finder_update_request]
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
