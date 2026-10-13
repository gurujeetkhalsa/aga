-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [integration].[news_archive_article_member_match] (
    [NewsArchiveArticleMemberMatchID] bigint IDENTITY(1,1) NOT NULL,
    [News_Article_Snapshot_ID] bigint NOT NULL,
    [News_Article_ID] bigint NOT NULL,
    [Item_ID] int NOT NULL,
    [AGAID] int NOT NULL,
    [MatchedName] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [ArticleTitle] nvarchar(500) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [ArticleLink] nvarchar(1000) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [ArticleActiveDate] date NULL,
    [Refresh_Run_Key] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_archive_article_member_match_created_at] DEFAULT (sysutcdatetime()),
    [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_archive_article_member_match_updated_at] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [integration].[news_archive_article_member_match] ADD CONSTRAINT [PK_news_archive_article_member_match] PRIMARY KEY CLUSTERED ([NewsArchiveArticleMemberMatchID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE UNIQUE NONCLUSTERED INDEX [UX_news_archive_article_member_match_snapshot_agaid_name] ON [integration].[news_archive_article_member_match] ([News_Article_Snapshot_ID] ASC, [AGAID] ASC, [MatchedName] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_news_archive_article_member_match_agaid_date] ON [integration].[news_archive_article_member_match] ([AGAID] ASC, [ArticleActiveDate] DESC, [News_Article_Snapshot_ID] DESC) INCLUDE ([ArticleTitle], [ArticleLink]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
