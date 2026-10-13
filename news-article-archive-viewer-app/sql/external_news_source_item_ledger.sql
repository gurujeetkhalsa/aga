-- Copyright 2026, American Go Association, All rights reserved
--
-- Persistent ledger for external Go news source items.
-- Run this before enabling the ledger-backed external digest sync.

IF SCHEMA_ID(N'clubexpress') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA [clubexpress]');
END;

IF OBJECT_ID(N'clubexpress.external_news_source_items', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[external_news_source_items]
    (
        [External_News_Source_Item_ID] bigint IDENTITY(1,1) NOT NULL
            CONSTRAINT [PK_external_news_source_items] PRIMARY KEY,
        [Source_ID] nvarchar(64) NOT NULL,
        [Source_Article_ID] nvarchar(2048) NOT NULL,
        [Source_Article_Key_Sha256] char(64) NOT NULL,
        [Source_Name] nvarchar(256) NULL,
        [Source_Home_Url] nvarchar(2048) NULL,
        [Source_Language] nvarchar(32) NULL,
        [Canonical_Url] nvarchar(2048) NULL,
        [Canonical_Url_Sha256] char(64) NULL,
        [Title] nvarchar(1024) NULL,
        [Author] nvarchar(512) NULL,
        [Category] nvarchar(512) NULL,
        [Published_Text] nvarchar(512) NULL,
        [Published_At] nvarchar(64) NULL,
        [Digest_Date] date NULL,
        [Summary] nvarchar(max) NULL,
        [Summary_Type] nvarchar(64) NULL,
        [Title_En] nvarchar(1024) NULL,
        [Category_En] nvarchar(512) NULL,
        [Summary_En] nvarchar(max) NULL,
        [Enrichment_Status] nvarchar(64) NULL,
        [Enrichment_Model] nvarchar(128) NULL,
        [Image_Url] nvarchar(2048) NULL,
        [First_Seen_At] datetime2(0) NOT NULL
            CONSTRAINT [DF_external_news_source_items_first_seen_at] DEFAULT SYSUTCDATETIME(),
        [Last_Seen_At] datetime2(0) NOT NULL
            CONSTRAINT [DF_external_news_source_items_last_seen_at] DEFAULT SYSUTCDATETIME(),
        [Last_Fetched_At] nvarchar(64) NULL,
        [Content_Sha256] char(64) NULL,
        [Last_Run_Key] nvarchar(128) NULL,
        [Raw_Item_Json] nvarchar(max) NULL,
        [Created_At] datetime2(0) NOT NULL
            CONSTRAINT [DF_external_news_source_items_created_at] DEFAULT SYSUTCDATETIME(),
        [Updated_At] datetime2(0) NOT NULL
            CONSTRAINT [DF_external_news_source_items_updated_at] DEFAULT SYSUTCDATETIME()
    );
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'UX_external_news_source_items_source_key'
      AND [object_id] = OBJECT_ID(N'clubexpress.external_news_source_items', N'U')
)
BEGIN
    CREATE UNIQUE INDEX [UX_external_news_source_items_source_key]
        ON [clubexpress].[external_news_source_items] ([Source_ID], [Source_Article_Key_Sha256]);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_external_news_source_items_digest_date'
      AND [object_id] = OBJECT_ID(N'clubexpress.external_news_source_items', N'U')
)
BEGIN
    CREATE INDEX [IX_external_news_source_items_digest_date]
        ON [clubexpress].[external_news_source_items] ([Digest_Date] DESC, [Source_ID])
        INCLUDE ([Source_Article_Key_Sha256], [Published_At], [Title]);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_external_news_source_items_last_seen'
      AND [object_id] = OBJECT_ID(N'clubexpress.external_news_source_items', N'U')
)
BEGIN
    CREATE INDEX [IX_external_news_source_items_last_seen]
        ON [clubexpress].[external_news_source_items] ([Last_Seen_At] DESC);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_external_news_source_items_content_sha'
      AND [object_id] = OBJECT_ID(N'clubexpress.external_news_source_items', N'U')
)
BEGIN
    CREATE INDEX [IX_external_news_source_items_content_sha]
        ON [clubexpress].[external_news_source_items] ([Content_Sha256])
        WHERE [Content_Sha256] IS NOT NULL;
END;
