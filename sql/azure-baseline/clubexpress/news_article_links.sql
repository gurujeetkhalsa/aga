-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[news_article_links] (
    [News_Article_Link_ID] bigint IDENTITY(1,1) NOT NULL,
    [News_Article_Snapshot_ID] bigint NOT NULL,
    [News_Article_ID] bigint NOT NULL,
    [Href] nvarchar(2048) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Link_Text] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Link_Source] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_news_article_links_Link_Source] DEFAULT (N'rendered_article'),
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_article_links_Created_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [clubexpress].[news_article_links] ADD CONSTRAINT [PK_news_article_links] PRIMARY KEY CLUSTERED ([News_Article_Link_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_news_article_links_Snapshot] ON [clubexpress].[news_article_links] ([News_Article_Snapshot_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[news_article_links] WITH CHECK ADD CONSTRAINT [FK_news_article_links_Snapshot] FOREIGN KEY ([News_Article_Snapshot_ID]) REFERENCES [clubexpress].[news_article_snapshots] ([News_Article_Snapshot_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [clubexpress].[news_article_links] WITH CHECK ADD CONSTRAINT [FK_news_article_links_Article] FOREIGN KEY ([News_Article_ID]) REFERENCES [clubexpress].[news_articles] ([News_Article_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
