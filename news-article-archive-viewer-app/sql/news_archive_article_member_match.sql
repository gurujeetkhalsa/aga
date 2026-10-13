-- Copyright 2026, American Go Association, All rights reserved
--
-- Archive-backed player/article matches for Ratings Explorer context links.

IF SCHEMA_ID(N'integration') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA [integration]');
END;

IF OBJECT_ID(N'integration.news_archive_article_member_match', N'U') IS NULL
BEGIN
    CREATE TABLE [integration].[news_archive_article_member_match]
    (
        [NewsArchiveArticleMemberMatchID] bigint IDENTITY(1,1) NOT NULL
            CONSTRAINT [PK_news_archive_article_member_match] PRIMARY KEY,
        [News_Article_Snapshot_ID] bigint NOT NULL,
        [News_Article_ID] bigint NOT NULL,
        [Item_ID] int NOT NULL,
        [AGAID] int NOT NULL,
        [MatchedName] nvarchar(200) NOT NULL,
        [ArticleTitle] nvarchar(500) NOT NULL,
        [ArticleLink] nvarchar(1000) NOT NULL,
        [ArticleActiveDate] date NULL,
        [Refresh_Run_Key] nvarchar(128) NOT NULL,
        [Created_At] datetime2(0) NOT NULL
            CONSTRAINT [DF_news_archive_article_member_match_created_at] DEFAULT SYSUTCDATETIME(),
        [Updated_At] datetime2(0) NOT NULL
            CONSTRAINT [DF_news_archive_article_member_match_updated_at] DEFAULT SYSUTCDATETIME()
    );
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'UX_news_archive_article_member_match_snapshot_agaid_name'
      AND [object_id] = OBJECT_ID(N'integration.news_archive_article_member_match', N'U')
)
BEGIN
    CREATE UNIQUE INDEX [UX_news_archive_article_member_match_snapshot_agaid_name]
        ON [integration].[news_archive_article_member_match]
        (
            [News_Article_Snapshot_ID],
            [AGAID],
            [MatchedName]
        );
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_news_archive_article_member_match_agaid_date'
      AND [object_id] = OBJECT_ID(N'integration.news_archive_article_member_match', N'U')
)
BEGIN
    CREATE INDEX [IX_news_archive_article_member_match_agaid_date]
        ON [integration].[news_archive_article_member_match]
        (
            [AGAID],
            [ArticleActiveDate] DESC,
            [News_Article_Snapshot_ID] DESC
        )
        INCLUDE ([ArticleTitle], [ArticleLink]);
END;
