-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [membership].[chapter_membership_history] (
    [HistoryID] int IDENTITY(1,1) NOT NULL,
    [AGAID] int NOT NULL,
    [ChapterID] int NOT NULL,
    [StartDate] date NOT NULL,
    [EndDate] date NULL,
    [Source] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [CreatedDate] datetime2(7) NULL CONSTRAINT [DF__chapter_m__Creat__7E37BEF6] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [membership].[chapter_membership_history] ADD CONSTRAINT [PK__chapter___4D7B4ADD9A066E77] PRIMARY KEY CLUSTERED ([HistoryID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_history_AGAID] ON [membership].[chapter_membership_history] ([AGAID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_history_ChapterID] ON [membership].[chapter_membership_history] ([ChapterID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [membership].[chapter_membership_history] WITH CHECK ADD CONSTRAINT [FK_chapter_membership_chapter] FOREIGN KEY ([ChapterID]) REFERENCES [membership].[chapters] ([ChapterID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [membership].[chapter_membership_history] WITH CHECK ADD CONSTRAINT [FK_chapter_membership_member] FOREIGN KEY ([AGAID]) REFERENCES [membership].[members] ([AGAID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
