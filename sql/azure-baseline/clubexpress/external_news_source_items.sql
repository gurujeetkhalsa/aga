-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[external_news_source_items] (
    [External_News_Source_Item_ID] bigint IDENTITY(1,1) NOT NULL,
    [Source_ID] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Source_Article_ID] nvarchar(2048) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Source_Article_Key_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Source_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Source_Home_Url] nvarchar(2048) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Source_Language] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Canonical_Url] nvarchar(2048) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Canonical_Url_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Title] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Author] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Category] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Published_Text] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Published_At] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Digest_Date] date NULL,
    [Summary] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Summary_Type] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Title_En] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Category_En] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Summary_En] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Enrichment_Status] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Enrichment_Model] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Image_Url] nvarchar(2048) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [First_Seen_At] datetime2(0) NOT NULL CONSTRAINT [DF_external_news_source_items_first_seen_at] DEFAULT (sysutcdatetime()),
    [Last_Seen_At] datetime2(0) NOT NULL CONSTRAINT [DF_external_news_source_items_last_seen_at] DEFAULT (sysutcdatetime()),
    [Last_Fetched_At] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Content_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Last_Run_Key] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Raw_Item_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_external_news_source_items_created_at] DEFAULT (sysutcdatetime()),
    [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_external_news_source_items_updated_at] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [clubexpress].[external_news_source_items] ADD CONSTRAINT [PK_external_news_source_items] PRIMARY KEY CLUSTERED ([External_News_Source_Item_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE UNIQUE NONCLUSTERED INDEX [UX_external_news_source_items_source_key] ON [clubexpress].[external_news_source_items] ([Source_ID] ASC, [Source_Article_Key_Sha256] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_external_news_source_items_digest_date] ON [clubexpress].[external_news_source_items] ([Digest_Date] DESC, [Source_ID] ASC) INCLUDE ([Source_Article_Key_Sha256], [Published_At], [Title]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_external_news_source_items_last_seen] ON [clubexpress].[external_news_source_items] ([Last_Seen_At] DESC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_external_news_source_items_content_sha] ON [clubexpress].[external_news_source_items] ([Content_Sha256] ASC) WHERE ([Content_Sha256] IS NOT NULL) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
