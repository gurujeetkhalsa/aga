-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[news_article_snapshots] (
    [News_Article_Snapshot_ID] bigint IDENTITY(1,1) NOT NULL,
    [News_Article_ID] bigint NOT NULL,
    [News_Archive_Run_ID] bigint NOT NULL,
    [Snapshot_Key] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Item_ID] int NOT NULL,
    [Headline] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Author] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Category] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Visibility] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Active_Date] date NULL,
    [Expires_Date] date NULL,
    [Summary] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Tags_Input] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Show_On_Main_News_Page] bit NULL,
    [Show_Active_Date_On_Story] bit NULL,
    [Show_Author_On_Story] bit NULL,
    [Rendered_Text_Length] int NOT NULL CONSTRAINT [DF_news_article_snapshots_Rendered_Text_Length] DEFAULT ((0)),
    [Rendered_Article_Image_Count] int NOT NULL CONSTRAINT [DF_news_article_snapshots_Rendered_Image_Count] DEFAULT ((0)),
    [Rendered_Article_Link_Count] int NOT NULL CONSTRAINT [DF_news_article_snapshots_Rendered_Link_Count] DEFAULT ((0)),
    [Managed_Image_Count] int NOT NULL CONSTRAINT [DF_news_article_snapshots_Managed_Image_Count] DEFAULT ((0)),
    [External_Image_Count] int NOT NULL CONSTRAINT [DF_news_article_snapshots_External_Image_Count] DEFAULT ((0)),
    [Asset_Captured_Count] int NOT NULL CONSTRAINT [DF_news_article_snapshots_Asset_Captured_Count] DEFAULT ((0)),
    [Asset_Failure_Count] int NOT NULL CONSTRAINT [DF_news_article_snapshots_Asset_Failure_Count] DEFAULT ((0)),
    [Manager_Row_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Configure_Form_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Rendered_Metadata_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Asset_Manifest_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Blob_Prefix] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Manager_Row_Blob_Path] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Configure_Form_Blob_Path] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Editor_Source_Blob_Path] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Editor_Source_Raw_Blob_Path] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Rendered_Article_Blob_Path] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Rendered_Container_Blob_Path] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Asset_Manifest_Blob_Path] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Editor_Source_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Rendered_Article_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Rendered_Container_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Content_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_article_snapshots_Created_At] DEFAULT (sysutcdatetime()),
    [Search_Text] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Thumbnail_Asset_ID] bigint NULL,
    [Thumbnail_Source] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Thumbnail_Selected_At] datetime2(0) NULL
 );
GO
ALTER TABLE [clubexpress].[news_article_snapshots] ADD CONSTRAINT [PK_news_article_snapshots] PRIMARY KEY CLUSTERED ([News_Article_Snapshot_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[news_article_snapshots] ADD CONSTRAINT [UQ_news_article_snapshots_Snapshot_Key] UNIQUE NONCLUSTERED ([Snapshot_Key] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[news_article_snapshots] ADD CONSTRAINT [UQ_news_article_snapshots_Run_Article] UNIQUE NONCLUSTERED ([News_Archive_Run_ID] ASC, [News_Article_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_news_article_snapshots_Article_Created] ON [clubexpress].[news_article_snapshots] ([News_Article_ID] ASC, [Created_At] DESC) INCLUDE ([News_Archive_Run_ID], [Content_Sha256], [Status], [Visibility], [Blob_Prefix]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_news_article_snapshots_Run] ON [clubexpress].[news_article_snapshots] ([News_Archive_Run_ID] ASC, [Item_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_news_article_snapshots_Title_Summary_List] ON [clubexpress].[news_article_snapshots] ([Active_Date] DESC, [Headline] ASC) INCLUDE ([News_Article_ID], [News_Archive_Run_ID], [Item_ID], [Author], [Category], [Status], [Visibility], [Summary]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[news_article_snapshots] WITH CHECK ADD CONSTRAINT [CK_news_article_snapshots_Manager_Row_Json] CHECK ([Manager_Row_Json] IS NULL OR isjson([Manager_Row_Json])=(1));
GO
ALTER TABLE [clubexpress].[news_article_snapshots] WITH CHECK ADD CONSTRAINT [CK_news_article_snapshots_Configure_Form_Json] CHECK ([Configure_Form_Json] IS NULL OR isjson([Configure_Form_Json])=(1));
GO
ALTER TABLE [clubexpress].[news_article_snapshots] WITH CHECK ADD CONSTRAINT [CK_news_article_snapshots_Rendered_Metadata_Json] CHECK ([Rendered_Metadata_Json] IS NULL OR isjson([Rendered_Metadata_Json])=(1));
GO
ALTER TABLE [clubexpress].[news_article_snapshots] WITH CHECK ADD CONSTRAINT [CK_news_article_snapshots_Asset_Manifest_Json] CHECK ([Asset_Manifest_Json] IS NULL OR isjson([Asset_Manifest_Json])=(1));
GO
ALTER TABLE [clubexpress].[news_article_snapshots] WITH CHECK ADD CONSTRAINT [FK_news_article_snapshots_Article] FOREIGN KEY ([News_Article_ID]) REFERENCES [clubexpress].[news_articles] ([News_Article_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [clubexpress].[news_article_snapshots] WITH CHECK ADD CONSTRAINT [FK_news_article_snapshots_Run] FOREIGN KEY ([News_Archive_Run_ID]) REFERENCES [clubexpress].[news_archive_runs] ([News_Archive_Run_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [clubexpress].[news_article_snapshots] WITH NOCHECK ADD CONSTRAINT [FK_news_article_snapshots_ThumbnailAsset] FOREIGN KEY ([Thumbnail_Asset_ID]) REFERENCES [clubexpress].[news_article_assets] ([News_Article_Asset_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
