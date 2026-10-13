-- Copyright 2026, American Go Association, All rights reserved

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'clubexpress') IS NULL
    EXEC(N'CREATE SCHEMA [clubexpress]');
GO

IF OBJECT_ID(N'clubexpress.news_archive_runs', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[news_archive_runs]
    (
        [News_Archive_Run_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Run_Key] nvarchar(256) NOT NULL,
        [Run_Type] nvarchar(32) NOT NULL CONSTRAINT [DF_news_archive_runs_Run_Type] DEFAULT N'manual',
        [Source_Mode] nvarchar(64) NOT NULL,
        [Source_Description] nvarchar(512) NULL,
        [Started_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_archive_runs_Started_At] DEFAULT SYSUTCDATETIME(),
        [Completed_At] datetime2(0) NULL,
        [Status] nvarchar(32) NOT NULL CONSTRAINT [DF_news_archive_runs_Status] DEFAULT N'running',
        [Article_Count] int NOT NULL CONSTRAINT [DF_news_archive_runs_Article_Count] DEFAULT 0,
        [Asset_Count] int NOT NULL CONSTRAINT [DF_news_archive_runs_Asset_Count] DEFAULT 0,
        [Blob_Account] nvarchar(128) NULL,
        [Blob_Container] nvarchar(128) NULL,
        [Blob_Prefix] nvarchar(1024) NULL,
        [Manifest_Blob_Path] nvarchar(1024) NULL,
        [Summary_Json] nvarchar(max) NULL,
        [Error_Message] nvarchar(max) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_archive_runs_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_news_archive_runs] PRIMARY KEY CLUSTERED ([News_Archive_Run_ID]),
        CONSTRAINT [UQ_news_archive_runs_Run_Key] UNIQUE ([Run_Key]),
        CONSTRAINT [CK_news_archive_runs_Run_Type] CHECK ([Run_Type] IN (N'sample', N'manual', N'backfill', N'scheduled')),
        CONSTRAINT [CK_news_archive_runs_Status] CHECK ([Status] IN (N'running', N'succeeded', N'partial', N'failed', N'cancelled')),
        CONSTRAINT [CK_news_archive_runs_Article_Count] CHECK ([Article_Count] >= 0),
        CONSTRAINT [CK_news_archive_runs_Asset_Count] CHECK ([Asset_Count] >= 0),
        CONSTRAINT [CK_news_archive_runs_Summary_Json] CHECK ([Summary_Json] IS NULL OR ISJSON([Summary_Json]) = 1)
    );
END;
GO

IF OBJECT_ID(N'clubexpress.news_articles', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[news_articles]
    (
        [News_Article_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Club_ID] int NOT NULL CONSTRAINT [DF_news_articles_Club_ID] DEFAULT 454497,
        [Item_ID] int NOT NULL,
        [Headline] nvarchar(512) NULL,
        [Author] nvarchar(256) NULL,
        [Subgroup] nvarchar(256) NULL,
        [Category] nvarchar(128) NULL,
        [Current_Status] nvarchar(32) NULL,
        [Visibility] nvarchar(64) NULL,
        [Active_Date] date NULL,
        [Expires_Date] date NULL,
        [View_Url] nvarchar(1024) NULL,
        [Configure_Url] nvarchar(1024) NULL,
        [Edit_Text_Url] nvarchar(1024) NULL,
        [First_Seen_Run_ID] bigint NULL,
        [Last_Seen_Run_ID] bigint NULL,
        [First_Seen_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_articles_First_Seen_At] DEFAULT SYSUTCDATETIME(),
        [Last_Seen_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_articles_Last_Seen_At] DEFAULT SYSUTCDATETIME(),
        [Is_Current] bit NOT NULL CONSTRAINT [DF_news_articles_Is_Current] DEFAULT 1,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_articles_Created_At] DEFAULT SYSUTCDATETIME(),
        [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_articles_Updated_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_news_articles] PRIMARY KEY CLUSTERED ([News_Article_ID]),
        CONSTRAINT [UQ_news_articles_Club_Item] UNIQUE ([Club_ID], [Item_ID]),
        CONSTRAINT [FK_news_articles_First_Run] FOREIGN KEY ([First_Seen_Run_ID]) REFERENCES [clubexpress].[news_archive_runs] ([News_Archive_Run_ID]),
        CONSTRAINT [FK_news_articles_Last_Run] FOREIGN KEY ([Last_Seen_Run_ID]) REFERENCES [clubexpress].[news_archive_runs] ([News_Archive_Run_ID])
    );
END;
GO

IF OBJECT_ID(N'clubexpress.news_article_snapshots', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[news_article_snapshots]
    (
        [News_Article_Snapshot_ID] bigint IDENTITY(1, 1) NOT NULL,
        [News_Article_ID] bigint NOT NULL,
        [News_Archive_Run_ID] bigint NOT NULL,
        [Snapshot_Key] nvarchar(256) NOT NULL,
        [Item_ID] int NOT NULL,
        [Headline] nvarchar(512) NULL,
        [Author] nvarchar(256) NULL,
        [Category] nvarchar(128) NULL,
        [Status] nvarchar(32) NULL,
        [Visibility] nvarchar(64) NULL,
        [Active_Date] date NULL,
        [Expires_Date] date NULL,
        [Summary] nvarchar(max) NULL,
        [Search_Text] nvarchar(max) NULL,
        [Tags_Input] nvarchar(1024) NULL,
        [Show_On_Main_News_Page] bit NULL,
        [Show_Active_Date_On_Story] bit NULL,
        [Show_Author_On_Story] bit NULL,
        [Rendered_Text_Length] int NOT NULL CONSTRAINT [DF_news_article_snapshots_Rendered_Text_Length] DEFAULT 0,
        [Rendered_Article_Image_Count] int NOT NULL CONSTRAINT [DF_news_article_snapshots_Rendered_Image_Count] DEFAULT 0,
        [Rendered_Article_Link_Count] int NOT NULL CONSTRAINT [DF_news_article_snapshots_Rendered_Link_Count] DEFAULT 0,
        [Managed_Image_Count] int NOT NULL CONSTRAINT [DF_news_article_snapshots_Managed_Image_Count] DEFAULT 0,
        [External_Image_Count] int NOT NULL CONSTRAINT [DF_news_article_snapshots_External_Image_Count] DEFAULT 0,
        [Asset_Captured_Count] int NOT NULL CONSTRAINT [DF_news_article_snapshots_Asset_Captured_Count] DEFAULT 0,
        [Asset_Failure_Count] int NOT NULL CONSTRAINT [DF_news_article_snapshots_Asset_Failure_Count] DEFAULT 0,
        [Manager_Row_Json] nvarchar(max) NULL,
        [Configure_Form_Json] nvarchar(max) NULL,
        [Rendered_Metadata_Json] nvarchar(max) NULL,
        [Asset_Manifest_Json] nvarchar(max) NULL,
        [Blob_Prefix] nvarchar(1024) NULL,
        [Manager_Row_Blob_Path] nvarchar(1024) NULL,
        [Configure_Form_Blob_Path] nvarchar(1024) NULL,
        [Editor_Source_Blob_Path] nvarchar(1024) NULL,
        [Editor_Source_Raw_Blob_Path] nvarchar(1024) NULL,
        [Rendered_Article_Blob_Path] nvarchar(1024) NULL,
        [Rendered_Container_Blob_Path] nvarchar(1024) NULL,
        [Asset_Manifest_Blob_Path] nvarchar(1024) NULL,
        [Editor_Source_Sha256] char(64) NULL,
        [Rendered_Article_Sha256] char(64) NULL,
        [Rendered_Container_Sha256] char(64) NULL,
        [Content_Sha256] char(64) NULL,
        [Thumbnail_Asset_ID] bigint NULL,
        [Thumbnail_Source] nvarchar(64) NULL,
        [Thumbnail_Selected_At] datetime2(0) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_article_snapshots_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_news_article_snapshots] PRIMARY KEY CLUSTERED ([News_Article_Snapshot_ID]),
        CONSTRAINT [UQ_news_article_snapshots_Run_Article] UNIQUE ([News_Archive_Run_ID], [News_Article_ID]),
        CONSTRAINT [UQ_news_article_snapshots_Snapshot_Key] UNIQUE ([Snapshot_Key]),
        CONSTRAINT [FK_news_article_snapshots_Article] FOREIGN KEY ([News_Article_ID]) REFERENCES [clubexpress].[news_articles] ([News_Article_ID]),
        CONSTRAINT [FK_news_article_snapshots_Run] FOREIGN KEY ([News_Archive_Run_ID]) REFERENCES [clubexpress].[news_archive_runs] ([News_Archive_Run_ID]),
        CONSTRAINT [CK_news_article_snapshots_Manager_Row_Json] CHECK ([Manager_Row_Json] IS NULL OR ISJSON([Manager_Row_Json]) = 1),
        CONSTRAINT [CK_news_article_snapshots_Configure_Form_Json] CHECK ([Configure_Form_Json] IS NULL OR ISJSON([Configure_Form_Json]) = 1),
        CONSTRAINT [CK_news_article_snapshots_Rendered_Metadata_Json] CHECK ([Rendered_Metadata_Json] IS NULL OR ISJSON([Rendered_Metadata_Json]) = 1),
        CONSTRAINT [CK_news_article_snapshots_Asset_Manifest_Json] CHECK ([Asset_Manifest_Json] IS NULL OR ISJSON([Asset_Manifest_Json]) = 1)
    );
END;
GO

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

IF COL_LENGTH(N'clubexpress.news_article_snapshots', N'Search_Text') IS NULL
BEGIN
    ALTER TABLE [clubexpress].[news_article_snapshots]
        ADD [Search_Text] nvarchar(max) NULL;
END;
GO

IF OBJECT_ID(N'clubexpress.news_article_assets', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[news_article_assets]
    (
        [News_Article_Asset_ID] bigint IDENTITY(1, 1) NOT NULL,
        [News_Article_Snapshot_ID] bigint NOT NULL,
        [News_Article_ID] bigint NOT NULL,
        [Source_Url] nvarchar(2048) NOT NULL,
        [Source_Url_Sha256] char(64) NULL,
        [Asset_Class] nvarchar(32) NOT NULL CONSTRAINT [DF_news_article_assets_Asset_Class] DEFAULT N'managed',
        [Capture_Status] nvarchar(32) NOT NULL CONSTRAINT [DF_news_article_assets_Capture_Status] DEFAULT N'captured',
        [Blob_Path] nvarchar(1024) NULL,
        [File_Name] nvarchar(512) NULL,
        [Content_Type] nvarchar(128) NULL,
        [Byte_Length] bigint NULL,
        [Sha256] char(64) NULL,
        [Width] int NULL,
        [Height] int NULL,
        [Alt_Text] nvarchar(512) NULL,
        [Title_Text] nvarchar(512) NULL,
        [Error_Message] nvarchar(max) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_article_assets_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_news_article_assets] PRIMARY KEY CLUSTERED ([News_Article_Asset_ID]),
        CONSTRAINT [FK_news_article_assets_Snapshot] FOREIGN KEY ([News_Article_Snapshot_ID]) REFERENCES [clubexpress].[news_article_snapshots] ([News_Article_Snapshot_ID]),
        CONSTRAINT [FK_news_article_assets_Article] FOREIGN KEY ([News_Article_ID]) REFERENCES [clubexpress].[news_articles] ([News_Article_ID]),
        CONSTRAINT [CK_news_article_assets_Asset_Class] CHECK ([Asset_Class] IN (N'managed', N'external', N'legacy_external', N'unknown')),
        CONSTRAINT [CK_news_article_assets_Capture_Status] CHECK ([Capture_Status] IN (N'captured', N'not_fetched', N'failed'))
    );
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

IF OBJECT_ID(N'clubexpress.news_article_links', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[news_article_links]
    (
        [News_Article_Link_ID] bigint IDENTITY(1, 1) NOT NULL,
        [News_Article_Snapshot_ID] bigint NOT NULL,
        [News_Article_ID] bigint NOT NULL,
        [Href] nvarchar(2048) NOT NULL,
        [Link_Text] nvarchar(1024) NULL,
        [Link_Source] nvarchar(32) NOT NULL CONSTRAINT [DF_news_article_links_Link_Source] DEFAULT N'rendered_article',
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_article_links_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_news_article_links] PRIMARY KEY CLUSTERED ([News_Article_Link_ID]),
        CONSTRAINT [FK_news_article_links_Snapshot] FOREIGN KEY ([News_Article_Snapshot_ID]) REFERENCES [clubexpress].[news_article_snapshots] ([News_Article_Snapshot_ID]),
        CONSTRAINT [FK_news_article_links_Article] FOREIGN KEY ([News_Article_ID]) REFERENCES [clubexpress].[news_articles] ([News_Article_ID])
    );
END;
GO

IF OBJECT_ID(N'clubexpress.external_news_translation_cache', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[external_news_translation_cache]
    (
        [External_News_Translation_Cache_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Source_ID] nvarchar(64) NOT NULL,
        [Source_Article_ID] nvarchar(512) NOT NULL,
        [Content_Sha256] char(64) NOT NULL,
        [Source_Language] nvarchar(32) NOT NULL,
        [Target_Language] nvarchar(16) NOT NULL CONSTRAINT [DF_external_news_translation_cache_Target_Language] DEFAULT N'en',
        [Title_En] nvarchar(1024) NULL,
        [Category_En] nvarchar(512) NULL,
        [Summary_En] nvarchar(max) NULL,
        [Enrichment_Status] nvarchar(64) NULL,
        [Enrichment_Model] nvarchar(128) NULL,
        [Hit_Count] int NOT NULL CONSTRAINT [DF_external_news_translation_cache_Hit_Count] DEFAULT 0,
        [Last_Used_At] datetime2(0) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_external_news_translation_cache_Created_At] DEFAULT SYSUTCDATETIME(),
        [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_external_news_translation_cache_Updated_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_external_news_translation_cache] PRIMARY KEY CLUSTERED ([External_News_Translation_Cache_ID]),
        CONSTRAINT [UQ_external_news_translation_cache_Item_Content] UNIQUE
        (
            [Source_ID],
            [Source_Article_ID],
            [Content_Sha256],
            [Target_Language]
        ),
        CONSTRAINT [CK_external_news_translation_cache_Hit_Count] CHECK ([Hit_Count] >= 0)
    );
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_news_articles_Active_Date'
      AND [object_id] = OBJECT_ID(N'clubexpress.news_articles')
)
BEGIN
    CREATE INDEX [IX_news_articles_Active_Date]
        ON [clubexpress].[news_articles] ([Active_Date] DESC, [Item_ID])
        INCLUDE ([Headline], [Author], [Current_Status], [Visibility]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_external_news_translation_cache_Source_Article'
      AND [object_id] = OBJECT_ID(N'clubexpress.external_news_translation_cache')
)
BEGIN
    CREATE INDEX [IX_external_news_translation_cache_Source_Article]
        ON [clubexpress].[external_news_translation_cache] ([Source_ID], [Source_Article_ID], [Target_Language])
        INCLUDE ([Content_Sha256], [Updated_At], [Hit_Count]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_news_article_snapshots_Article_Created'
      AND [object_id] = OBJECT_ID(N'clubexpress.news_article_snapshots')
)
BEGIN
    CREATE INDEX [IX_news_article_snapshots_Article_Created]
        ON [clubexpress].[news_article_snapshots] ([News_Article_ID], [Created_At] DESC)
        INCLUDE ([News_Archive_Run_ID], [Content_Sha256], [Status], [Visibility], [Blob_Prefix]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_news_article_snapshots_Run'
      AND [object_id] = OBJECT_ID(N'clubexpress.news_article_snapshots')
)
BEGIN
    CREATE INDEX [IX_news_article_snapshots_Run]
        ON [clubexpress].[news_article_snapshots] ([News_Archive_Run_ID], [Item_ID]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_news_article_snapshots_Title_Summary_List'
      AND [object_id] = OBJECT_ID(N'clubexpress.news_article_snapshots')
)
BEGIN
    CREATE INDEX [IX_news_article_snapshots_Title_Summary_List]
        ON [clubexpress].[news_article_snapshots] ([Active_Date] DESC, [Headline])
        INCLUDE ([News_Article_ID], [News_Archive_Run_ID], [Item_ID], [Author], [Category], [Status], [Visibility], [Summary]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_news_article_assets_Snapshot'
      AND [object_id] = OBJECT_ID(N'clubexpress.news_article_assets')
)
BEGIN
    CREATE INDEX [IX_news_article_assets_Snapshot]
        ON [clubexpress].[news_article_assets] ([News_Article_Snapshot_ID], [Capture_Status])
        INCLUDE ([Asset_Class], [Blob_Path], [Sha256], [Byte_Length]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_news_article_assets_Source_Url_Hash'
      AND [object_id] = OBJECT_ID(N'clubexpress.news_article_assets')
)
BEGIN
    CREATE INDEX [IX_news_article_assets_Source_Url_Hash]
        ON [clubexpress].[news_article_assets] ([Source_Url_Sha256], [Capture_Status]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_news_article_links_Snapshot'
      AND [object_id] = OBJECT_ID(N'clubexpress.news_article_links')
)
BEGIN
    CREATE INDEX [IX_news_article_links_Snapshot]
        ON [clubexpress].[news_article_links] ([News_Article_Snapshot_ID]);
END;
GO

IF FULLTEXTSERVICEPROPERTY(N'IsFullTextInstalled') = 1
   AND NOT EXISTS
   (
       SELECT 1
       FROM sys.fulltext_catalogs
       WHERE [name] = N'clubexpress_news_archive_search'
   )
BEGIN
    CREATE FULLTEXT CATALOG [clubexpress_news_archive_search] AS DEFAULT;
END;
GO

IF FULLTEXTSERVICEPROPERTY(N'IsFullTextInstalled') = 1
   AND NOT EXISTS
   (
       SELECT 1
       FROM sys.fulltext_indexes
       WHERE [object_id] = OBJECT_ID(N'clubexpress.news_article_snapshots')
   )
BEGIN
    CREATE FULLTEXT INDEX ON [clubexpress].[news_article_snapshots]
    (
        [Headline] LANGUAGE 1033,
        [Summary] LANGUAGE 1033,
        [Search_Text] LANGUAGE 1033
    )
    KEY INDEX [PK_news_article_snapshots]
    ON [clubexpress_news_archive_search]
    WITH CHANGE_TRACKING AUTO;
END;
GO

IF FULLTEXTSERVICEPROPERTY(N'IsFullTextInstalled') = 1
   AND EXISTS
   (
       SELECT 1
       FROM sys.fulltext_indexes
       WHERE [object_id] = OBJECT_ID(N'clubexpress.news_article_snapshots')
   )
   AND NOT EXISTS
   (
       SELECT 1
       FROM sys.fulltext_index_columns AS fic
       INNER JOIN sys.columns AS c
           ON c.[object_id] = fic.[object_id]
          AND c.[column_id] = fic.[column_id]
       WHERE fic.[object_id] = OBJECT_ID(N'clubexpress.news_article_snapshots')
         AND c.[name] = N'Headline'
   )
BEGIN
    ALTER FULLTEXT INDEX ON [clubexpress].[news_article_snapshots]
        ADD ([Headline] LANGUAGE 1033);
END;
GO

IF FULLTEXTSERVICEPROPERTY(N'IsFullTextInstalled') = 1
   AND EXISTS
   (
       SELECT 1
       FROM sys.fulltext_indexes
       WHERE [object_id] = OBJECT_ID(N'clubexpress.news_article_snapshots')
   )
   AND NOT EXISTS
   (
       SELECT 1
       FROM sys.fulltext_index_columns AS fic
       INNER JOIN sys.columns AS c
           ON c.[object_id] = fic.[object_id]
          AND c.[column_id] = fic.[column_id]
       WHERE fic.[object_id] = OBJECT_ID(N'clubexpress.news_article_snapshots')
         AND c.[name] = N'Summary'
   )
BEGIN
    ALTER FULLTEXT INDEX ON [clubexpress].[news_article_snapshots]
        ADD ([Summary] LANGUAGE 1033);
END;
GO

CREATE OR ALTER VIEW [clubexpress].[v_news_article_latest_snapshots]
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
