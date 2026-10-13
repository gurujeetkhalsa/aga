-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[chapter_finder_administrator_assignment_versions] (
    [Chapter_Finder_Administrator_Assignment_Version_ID] bigint IDENTITY(1,1) NOT NULL,
    [Chapter_Finder_Administrator_Assignment_ID] bigint NOT NULL,
    [Chapter_Finder_Administrator_Sync_Run_ID] bigint NOT NULL,
    [Change_Type] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Chapter_Finder_Entry_ID] bigint NOT NULL,
    [Club_ID] int NOT NULL,
    [Chapter_Finder_ID] int NOT NULL,
    [Administrator_Display_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Normalized_Display_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Duplicate_Ordinal] int NOT NULL,
    [Assignment_Key_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Membership_AGAID] int NULL,
    [Member_Match_Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Member_Match_Candidate_Count] int NOT NULL,
    [Is_Current] bit NOT NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignment_versions_Created_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_assignment_versions] ADD CONSTRAINT [PK_chapter_finder_administrator_assignment_versions] PRIMARY KEY CLUSTERED ([Chapter_Finder_Administrator_Assignment_Version_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_finder_administrator_assignment_versions_Assignment_Created] ON [clubexpress].[chapter_finder_administrator_assignment_versions] ([Chapter_Finder_Administrator_Assignment_ID] ASC, [Created_At] DESC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_assignment_versions] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_administrator_assignment_versions_Change_Type] CHECK ([Change_Type]=N'marked_inactive' OR [Change_Type]=N'resolution_changed' OR [Change_Type]=N'reactivated' OR [Change_Type]=N'inserted');
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_assignment_versions] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_administrator_assignment_versions_Member_Match_Status] CHECK ([Member_Match_Status]=N'unmatched' OR [Member_Match_Status]=N'ambiguous' OR [Member_Match_Status]=N'matched_unique');
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_assignment_versions] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_administrator_assignment_versions_Assignment] FOREIGN KEY ([Chapter_Finder_Administrator_Assignment_ID]) REFERENCES [clubexpress].[chapter_finder_administrator_assignments] ([Chapter_Finder_Administrator_Assignment_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_assignment_versions] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_administrator_assignment_versions_Run] FOREIGN KEY ([Chapter_Finder_Administrator_Sync_Run_ID]) REFERENCES [clubexpress].[chapter_finder_administrator_sync_runs] ([Chapter_Finder_Administrator_Sync_Run_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
