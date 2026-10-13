-- Live Azure SQL metadata; database aga-membership; VIEW
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   VIEW [clubexpress].[v_chapter_finder_update_push_queue]
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
