-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [rewards].[member_daily_snapshot] (
    [Snapshot_Date] date NOT NULL,
    [AGAID] int NOT NULL,
    [Member_Type] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Expiration_Date] date NULL,
    [ChapterID] int NULL,
    [Chapter_Code] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Is_Active] bit NOT NULL,
    [Is_Tournament_Pass] bit NOT NULL,
    [Created_RunID] int NOT NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_member_daily_snapshot_Created_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [rewards].[member_daily_snapshot] ADD CONSTRAINT [PK_member_daily_snapshot] PRIMARY KEY CLUSTERED ([Snapshot_Date] ASC, [AGAID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_member_daily_snapshot_Chapter] ON [rewards].[member_daily_snapshot] ([Snapshot_Date] ASC, [ChapterID] ASC, [Is_Active] ASC, [Is_Tournament_Pass] ASC) INCLUDE ([Chapter_Code], [Member_Type], [Expiration_Date]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_member_daily_snapshot_AGAID_Date] ON [rewards].[member_daily_snapshot] ([AGAID] ASC, [Snapshot_Date] ASC) INCLUDE ([ChapterID], [Chapter_Code], [Is_Active], [Member_Type]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[member_daily_snapshot] WITH CHECK ADD CONSTRAINT [FK_member_daily_snapshot_RunID] FOREIGN KEY ([Created_RunID]) REFERENCES [rewards].[reward_runs] ([RunID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
