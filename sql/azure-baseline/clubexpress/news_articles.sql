-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[news_articles] (
    [News_Article_ID] bigint IDENTITY(1,1) NOT NULL,
    [Club_ID] int NOT NULL CONSTRAINT [DF_news_articles_Club_ID] DEFAULT ((454497)),
    [Item_ID] int NOT NULL,
    [Headline] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Author] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Subgroup] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Category] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Current_Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Visibility] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Active_Date] date NULL,
    [Expires_Date] date NULL,
    [View_Url] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Configure_Url] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Edit_Text_Url] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [First_Seen_Run_ID] bigint NULL,
    [Last_Seen_Run_ID] bigint NULL,
    [First_Seen_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_articles_First_Seen_At] DEFAULT (sysutcdatetime()),
    [Last_Seen_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_articles_Last_Seen_At] DEFAULT (sysutcdatetime()),
    [Is_Current] bit NOT NULL CONSTRAINT [DF_news_articles_Is_Current] DEFAULT ((1)),
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_articles_Created_At] DEFAULT (sysutcdatetime()),
    [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_articles_Updated_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [clubexpress].[news_articles] ADD CONSTRAINT [PK_news_articles] PRIMARY KEY CLUSTERED ([News_Article_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[news_articles] ADD CONSTRAINT [UQ_news_articles_Club_Item] UNIQUE NONCLUSTERED ([Club_ID] ASC, [Item_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_news_articles_Active_Date] ON [clubexpress].[news_articles] ([Active_Date] DESC, [Item_ID] ASC) INCLUDE ([Headline], [Author], [Current_Status], [Visibility]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[news_articles] WITH CHECK ADD CONSTRAINT [FK_news_articles_First_Run] FOREIGN KEY ([First_Seen_Run_ID]) REFERENCES [clubexpress].[news_archive_runs] ([News_Archive_Run_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [clubexpress].[news_articles] WITH CHECK ADD CONSTRAINT [FK_news_articles_Last_Run] FOREIGN KEY ([Last_Seen_Run_ID]) REFERENCES [clubexpress].[news_archive_runs] ([News_Archive_Run_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
