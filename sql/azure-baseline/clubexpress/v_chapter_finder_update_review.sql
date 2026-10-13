-- Live Azure SQL metadata; database aga-membership; VIEW
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   VIEW [clubexpress].[v_chapter_finder_update_review]
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
