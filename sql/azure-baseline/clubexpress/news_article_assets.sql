-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[news_article_assets] (
    [News_Article_Asset_ID] bigint IDENTITY(1,1) NOT NULL,
    [News_Article_Snapshot_ID] bigint NOT NULL,
    [News_Article_ID] bigint NOT NULL,
    [Source_Url] nvarchar(2048) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Source_Url_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Asset_Class] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_news_article_assets_Asset_Class] DEFAULT (N'managed'),
    [Capture_Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_news_article_assets_Capture_Status] DEFAULT (N'captured'),
    [Blob_Path] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [File_Name] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Content_Type] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Byte_Length] bigint NULL,
    [Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Width] int NULL,
    [Height] int NULL,
    [Alt_Text] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Title_Text] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Error_Message] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_article_assets_Created_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [clubexpress].[news_article_assets] ADD CONSTRAINT [PK_news_article_assets] PRIMARY KEY CLUSTERED ([News_Article_Asset_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_news_article_assets_Snapshot] ON [clubexpress].[news_article_assets] ([News_Article_Snapshot_ID] ASC, [Capture_Status] ASC) INCLUDE ([Asset_Class], [Blob_Path], [Sha256], [Byte_Length]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_news_article_assets_Source_Url_Hash] ON [clubexpress].[news_article_assets] ([Source_Url_Sha256] ASC, [Capture_Status] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[news_article_assets] WITH CHECK ADD CONSTRAINT [CK_news_article_assets_Asset_Class] CHECK ([Asset_Class]=N'unknown' OR [Asset_Class]=N'legacy_external' OR [Asset_Class]=N'external' OR [Asset_Class]=N'managed');
GO
ALTER TABLE [clubexpress].[news_article_assets] WITH CHECK ADD CONSTRAINT [CK_news_article_assets_Capture_Status] CHECK ([Capture_Status]=N'failed' OR [Capture_Status]=N'not_fetched' OR [Capture_Status]=N'captured');
GO
ALTER TABLE [clubexpress].[news_article_assets] WITH CHECK ADD CONSTRAINT [FK_news_article_assets_Snapshot] FOREIGN KEY ([News_Article_Snapshot_ID]) REFERENCES [clubexpress].[news_article_snapshots] ([News_Article_Snapshot_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [clubexpress].[news_article_assets] WITH CHECK ADD CONSTRAINT [FK_news_article_assets_Article] FOREIGN KEY ([News_Article_ID]) REFERENCES [clubexpress].[news_articles] ([News_Article_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
