-- Live Azure SQL metadata; database aga-membership; SQL_STORED_PROCEDURE
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   PROCEDURE [clubexpress].[sp_reject_chapter_finder_update_request]
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
