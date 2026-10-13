-- Live Azure SQL metadata; database aga-membership; VIEW
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   VIEW [clubexpress].[v_chapter_finder_update_portal_review]
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
    r.[Submitted_By_Name],
    r.[Submitted_By_Email],
    r.[Submitted_By_Role],
    r.[Submitter_Notes],
    r.[Changed_Field_Count],
    r.[Field_Diff_Json],
    r.[Normalized_Proposed_Json],
    r.[Base_Administrator_Json],
    r.[Proposed_Administrator_Json],
    r.[Reviewed_By],
    r.[Reviewed_At],
    r.[Review_Notes],
    r.[Approved_By],
    r.[Approved_At],
    r.[Rejected_By],
    r.[Rejected_At],
    r.[Pushed_At],
    r.[Last_Push_Error],
    r.[Created_At],
    r.[Updated_At]
FROM [clubexpress].[chapter_finder_update_requests] AS r
INNER JOIN [clubexpress].[chapter_finder_entries] AS e
    ON e.[Chapter_Finder_Entry_ID] = r.[Chapter_Finder_Entry_ID];
GO
