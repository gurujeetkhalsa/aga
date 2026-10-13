-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[chapter_finder_administrator_assignments] (
    [Chapter_Finder_Administrator_Assignment_ID] bigint IDENTITY(1,1) NOT NULL,
    [Chapter_Finder_Entry_ID] bigint NOT NULL,
    [Club_ID] int NOT NULL,
    [Chapter_Finder_ID] int NOT NULL,
    [Administrator_Display_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Normalized_Display_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Duplicate_Ordinal] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_Duplicate_Ordinal] DEFAULT ((1)),
    [Assignment_Key_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Membership_AGAID] int NULL,
    [Member_Match_Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Member_Match_Candidate_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_Member_Match_Candidate_Count] DEFAULT ((0)),
    [First_Seen_Run_ID] bigint NOT NULL,
    [Last_Seen_Run_ID] bigint NOT NULL,
    [First_Seen_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_First_Seen_At] DEFAULT (sysutcdatetime()),
    [Last_Seen_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_Last_Seen_At] DEFAULT (sysutcdatetime()),
    [Last_Changed_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_Last_Changed_At] DEFAULT (sysutcdatetime()),
    [Is_Current] bit NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_Is_Current] DEFAULT ((1)),
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_Created_At] DEFAULT (sysutcdatetime()),
    [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_Updated_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_assignments] ADD CONSTRAINT [PK_chapter_finder_administrator_assignments] PRIMARY KEY CLUSTERED ([Chapter_Finder_Administrator_Assignment_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_assignments] ADD CONSTRAINT [UQ_chapter_finder_administrator_assignments_Entry_Key] UNIQUE NONCLUSTERED ([Chapter_Finder_Entry_ID] ASC, [Assignment_Key_Sha256] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_finder_administrator_assignments_Current_Chapter] ON [clubexpress].[chapter_finder_administrator_assignments] ([Is_Current] ASC, [Chapter_Finder_ID] ASC) INCLUDE ([Administrator_Display_Name], [Membership_AGAID], [Member_Match_Status]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_finder_administrator_assignments_Membership_AGAID] ON [clubexpress].[chapter_finder_administrator_assignments] ([Membership_AGAID] ASC, [Is_Current] ASC) INCLUDE ([Chapter_Finder_ID], [Administrator_Display_Name]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_assignments] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_administrator_assignments_Duplicate_Ordinal] CHECK ([Duplicate_Ordinal]>=(1));
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_assignments] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_administrator_assignments_Member_Match_Status] CHECK ([Member_Match_Status]=N'unmatched' OR [Member_Match_Status]=N'ambiguous' OR [Member_Match_Status]=N'matched_unique');
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_assignments] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_administrator_assignments_Entry] FOREIGN KEY ([Chapter_Finder_Entry_ID]) REFERENCES [clubexpress].[chapter_finder_entries] ([Chapter_Finder_Entry_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_assignments] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_administrator_assignments_Member] FOREIGN KEY ([Membership_AGAID]) REFERENCES [membership].[members] ([AGAID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_assignments] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_administrator_assignments_First_Run] FOREIGN KEY ([First_Seen_Run_ID]) REFERENCES [clubexpress].[chapter_finder_administrator_sync_runs] ([Chapter_Finder_Administrator_Sync_Run_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_assignments] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_administrator_assignments_Last_Run] FOREIGN KEY ([Last_Seen_Run_ID]) REFERENCES [clubexpress].[chapter_finder_administrator_sync_runs] ([Chapter_Finder_Administrator_Sync_Run_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
