-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [rewards].[chapter_daily_snapshot] (
    [Snapshot_Date] date NOT NULL,
    [ChapterID] int NOT NULL,
    [Chapter_Code] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Chapter_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Is_Current] bit NOT NULL CONSTRAINT [DF_chapter_daily_snapshot_Is_Current] DEFAULT ((1)),
    [Active_Member_Count] int NOT NULL,
    [Multiplier] tinyint NOT NULL,
    [Created_RunID] int NOT NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_daily_snapshot_Created_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [rewards].[chapter_daily_snapshot] ADD CONSTRAINT [PK_chapter_daily_snapshot] PRIMARY KEY CLUSTERED ([Snapshot_Date] ASC, [ChapterID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE UNIQUE NONCLUSTERED INDEX [IX_chapter_daily_snapshot_Code] ON [rewards].[chapter_daily_snapshot] ([Snapshot_Date] ASC, [Chapter_Code] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[chapter_daily_snapshot] WITH CHECK ADD CONSTRAINT [CK_chapter_daily_snapshot_Active_Member_Count] CHECK ([Active_Member_Count]>=(0));
GO
ALTER TABLE [rewards].[chapter_daily_snapshot] WITH CHECK ADD CONSTRAINT [CK_chapter_daily_snapshot_Multiplier] CHECK ([Multiplier]=(3) OR [Multiplier]=(2) OR [Multiplier]=(1));
GO
ALTER TABLE [rewards].[chapter_daily_snapshot] WITH CHECK ADD CONSTRAINT [FK_chapter_daily_snapshot_RunID] FOREIGN KEY ([Created_RunID]) REFERENCES [rewards].[reward_runs] ([RunID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
