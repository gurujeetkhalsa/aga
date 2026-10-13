-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [integration].[journal_article_member_match] (
    [JournalArticleMemberMatchID] int IDENTITY(1,1) NOT NULL,
    [MessageId] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [JournalDate] date NOT NULL,
    [AGAID] int NOT NULL,
    [MatchedName] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [ArticleTitle] nvarchar(500) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [ArticleLink] nvarchar(1000) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [CreatedDate] datetime2(7) NOT NULL CONSTRAINT [DF_journal_article_member_match_CreatedDate] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [integration].[journal_article_member_match] ADD CONSTRAINT [PK_journal_article_member_match] PRIMARY KEY CLUSTERED ([JournalArticleMemberMatchID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [integration].[journal_article_member_match] WITH CHECK ADD CONSTRAINT [FK_journal_article_member_match_member] FOREIGN KEY ([AGAID]) REFERENCES [membership].[members] ([AGAID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
