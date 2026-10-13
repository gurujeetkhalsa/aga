-- Live Azure SQL metadata; database aga-membership; SQL_STORED_PROCEDURE
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   PROCEDURE [rewards].[sp_get_pending_chapter_renewals]
    @AsOfDate date = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @EffectiveAsOfDate date = COALESCE(@AsOfDate, CONVERT(date, SYSUTCDATETIME()));

    SELECT
        [NoticeID],
        [Message_ID],
        [ChapterID],
        [Chapter_Code],
        [Chapter_Name],
        [Notice_Date],
        [Received_At],
        [Points_Required],
        [Available_Points],
        [Decision],
        [RedemptionID],
        [TransactionID],
        [RunID],
        DATEDIFF(day, [Notice_Date], @EffectiveAsOfDate) AS [Pending_Days],
        [Created_At]
    FROM [rewards].[chapter_renewal_notice_results]
    WHERE [Decision] IN (N'posted', N'already_posted')
      AND [TransactionID] IS NOT NULL
      AND [ClubExpress_Renewal_Message_ID] IS NULL
    ORDER BY [Notice_Date], [Chapter_Code], [ChapterID], [NoticeID];
END;
GO
