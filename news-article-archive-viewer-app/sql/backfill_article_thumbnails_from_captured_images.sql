-- Copyright 2026, American Go Association, All rights reserved

-- Select a stable thumbnail for archived articles from already captured article
-- images. This does not fetch new images and does not overwrite an explicit
-- thumbnail choice that may have been made later.

IF COL_LENGTH(N'clubexpress.news_article_snapshots', N'Thumbnail_Asset_ID') IS NULL
BEGIN
    ALTER TABLE [clubexpress].[news_article_snapshots]
        ADD [Thumbnail_Asset_ID] bigint NULL;
END;
GO

IF COL_LENGTH(N'clubexpress.news_article_snapshots', N'Thumbnail_Source') IS NULL
BEGIN
    ALTER TABLE [clubexpress].[news_article_snapshots]
        ADD [Thumbnail_Source] nvarchar(64) NULL;
END;
GO

IF COL_LENGTH(N'clubexpress.news_article_snapshots', N'Thumbnail_Selected_At') IS NULL
BEGIN
    ALTER TABLE [clubexpress].[news_article_snapshots]
        ADD [Thumbnail_Selected_At] datetime2(0) NULL;
END;
GO

IF OBJECT_ID(N'clubexpress.FK_news_article_snapshots_ThumbnailAsset', N'F') IS NULL
BEGIN
    ALTER TABLE [clubexpress].[news_article_snapshots] WITH NOCHECK
        ADD CONSTRAINT [FK_news_article_snapshots_ThumbnailAsset]
            FOREIGN KEY ([Thumbnail_Asset_ID])
            REFERENCES [clubexpress].[news_article_assets] ([News_Article_Asset_ID]);

    ALTER TABLE [clubexpress].[news_article_snapshots]
        CHECK CONSTRAINT [FK_news_article_snapshots_ThumbnailAsset];
END;
GO

;WITH CapturedImageCandidates AS
(
    SELECT
        snapshot.[News_Article_Snapshot_ID],
        asset.[News_Article_Asset_ID],
        ROW_NUMBER() OVER
        (
            PARTITION BY snapshot.[News_Article_Snapshot_ID]
            ORDER BY
                CASE
                    WHEN asset.[Asset_Class] = N'managed' THEN 0
                    WHEN asset.[Asset_Class] = N'external' THEN 1
                    WHEN asset.[Asset_Class] = N'legacy_external' THEN 2
                    ELSE 3
                END,
                CASE
                    WHEN COALESCE(asset.[Width], 0) >= 120
                     AND COALESCE(asset.[Height], 0) >= 90
                    THEN 0
                    ELSE 1
                END,
                asset.[News_Article_Asset_ID]
        ) AS [Candidate_Rank]
    FROM [clubexpress].[news_article_snapshots] AS snapshot
    INNER JOIN [clubexpress].[news_article_assets] AS asset
        ON asset.[News_Article_Snapshot_ID] = snapshot.[News_Article_Snapshot_ID]
    WHERE snapshot.[Thumbnail_Asset_ID] IS NULL
      AND asset.[Capture_Status] = N'captured'
      AND asset.[Blob_Path] IS NOT NULL
      AND
      (
          asset.[Content_Type] LIKE N'image/%'
          OR asset.[File_Name] LIKE N'%.jpg'
          OR asset.[File_Name] LIKE N'%.jpeg'
          OR asset.[File_Name] LIKE N'%.png'
          OR asset.[File_Name] LIKE N'%.gif'
          OR asset.[File_Name] LIKE N'%.webp'
      )
)
UPDATE snapshot
SET
    [Thumbnail_Asset_ID] = candidate.[News_Article_Asset_ID],
    [Thumbnail_Source] = N'captured_article_image',
    [Thumbnail_Selected_At] = SYSUTCDATETIME()
FROM [clubexpress].[news_article_snapshots] AS snapshot
INNER JOIN CapturedImageCandidates AS candidate
    ON candidate.[News_Article_Snapshot_ID] = snapshot.[News_Article_Snapshot_ID]
   AND candidate.[Candidate_Rank] = 1
WHERE snapshot.[Thumbnail_Asset_ID] IS NULL;
GO
