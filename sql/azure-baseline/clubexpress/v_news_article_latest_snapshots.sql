-- Live Azure SQL metadata; database aga-membership; VIEW
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   VIEW [clubexpress].[v_news_article_latest_snapshots]
AS
WITH RankedSnapshots AS
(
    SELECT
        s.*,
        ROW_NUMBER() OVER
        (
            PARTITION BY s.[News_Article_ID]
            ORDER BY s.[Created_At] DESC, s.[News_Article_Snapshot_ID] DESC
        ) AS [Snapshot_Rank]
    FROM [clubexpress].[news_article_snapshots] AS s
)
SELECT
    a.[News_Article_ID],
    a.[Club_ID],
    a.[Item_ID],
    a.[Headline],
    a.[Author],
    a.[Category],
    a.[Current_Status],
    a.[Visibility],
    a.[Active_Date],
    a.[Expires_Date],
    a.[View_Url],
    r.[News_Archive_Run_ID],
    r.[Run_Key],
    r.[Run_Type],
    r.[Source_Mode],
    s.[News_Article_Snapshot_ID],
    s.[Snapshot_Key],
    s.[Content_Sha256],
    s.[Search_Text],
    s.[Blob_Prefix],
    s.[Rendered_Text_Length],
    s.[Rendered_Article_Image_Count],
    s.[Managed_Image_Count],
    s.[Asset_Captured_Count],
    s.[Asset_Failure_Count],
    s.[Created_At] AS [Snapshot_Created_At]
FROM [clubexpress].[news_articles] AS a
INNER JOIN RankedSnapshots AS s
    ON s.[News_Article_ID] = a.[News_Article_ID]
   AND s.[Snapshot_Rank] = 1
INNER JOIN [clubexpress].[news_archive_runs] AS r
    ON r.[News_Archive_Run_ID] = s.[News_Archive_Run_ID];
GO
