-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[external_news_translation_cache] (
    [External_News_Translation_Cache_ID] bigint IDENTITY(1,1) NOT NULL,
    [Source_ID] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Source_Article_ID] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Content_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Source_Language] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Target_Language] nvarchar(16) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_external_news_translation_cache_Target_Language] DEFAULT (N'en'),
    [Title_En] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Category_En] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Summary_En] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Enrichment_Status] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Enrichment_Model] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Hit_Count] int NOT NULL CONSTRAINT [DF_external_news_translation_cache_Hit_Count] DEFAULT ((0)),
    [Last_Used_At] datetime2(0) NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_external_news_translation_cache_Created_At] DEFAULT (sysutcdatetime()),
    [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_external_news_translation_cache_Updated_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [clubexpress].[external_news_translation_cache] ADD CONSTRAINT [PK_external_news_translation_cache] PRIMARY KEY CLUSTERED ([External_News_Translation_Cache_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[external_news_translation_cache] ADD CONSTRAINT [UQ_external_news_translation_cache_Item_Content] UNIQUE NONCLUSTERED ([Source_ID] ASC, [Source_Article_ID] ASC, [Content_Sha256] ASC, [Target_Language] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_external_news_translation_cache_Source_Article] ON [clubexpress].[external_news_translation_cache] ([Source_ID] ASC, [Source_Article_ID] ASC, [Target_Language] ASC) INCLUDE ([Content_Sha256], [Updated_At], [Hit_Count]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[external_news_translation_cache] WITH CHECK ADD CONSTRAINT [CK_external_news_translation_cache_Hit_Count] CHECK ([Hit_Count]>=(0));
GO
