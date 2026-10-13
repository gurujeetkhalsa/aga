-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [membership].[chapters] (
    [ChapterID] int NOT NULL,
    [ChapterCode] nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ChapterName] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [City] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [State] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ChapterRepID] int NULL,
    [CreatedDate] datetime NULL CONSTRAINT [DF__chapters__Create__70DDC3D8] DEFAULT (getdate()),
    [Status] nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 );
GO
ALTER TABLE [membership].[chapters] ADD CONSTRAINT [PK__chapters__0893A34A1CC044CA] PRIMARY KEY CLUSTERED ([ChapterID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapters_ChapterRepID] ON [membership].[chapters] ([ChapterRepID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [membership].[chapters] WITH CHECK ADD CONSTRAINT [FK_chapter_rep] FOREIGN KEY ([ChapterRepID]) REFERENCES [membership].[members] ([AGAID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
