-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[chapter_finder_administrator_sync_runs] (
    [Chapter_Finder_Administrator_Sync_Run_ID] bigint IDENTITY(1,1) NOT NULL,
    [Run_Key] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Club_ID] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Club_ID] DEFAULT ((454497)),
    [Source_Url_Template] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Extracted_At] datetime2(0) NULL,
    [Started_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Started_At] DEFAULT (sysutcdatetime()),
    [Completed_At] datetime2(0) NULL,
    [Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Status] DEFAULT (N'running'),
    [Chapter_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Chapter_Count] DEFAULT ((0)),
    [Chapters_With_Administrators_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Chapters_With_Administrators_Count] DEFAULT ((0)),
    [Assignment_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Assignment_Count] DEFAULT ((0)),
    [Inserted_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Inserted_Count] DEFAULT ((0)),
    [Updated_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Updated_Count] DEFAULT ((0)),
    [Unchanged_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Unchanged_Count] DEFAULT ((0)),
    [Marked_Inactive_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Marked_Inactive_Count] DEFAULT ((0)),
    [Matched_Unique_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Matched_Unique_Count] DEFAULT ((0)),
    [Ambiguous_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Ambiguous_Count] DEFAULT ((0)),
    [Unmatched_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Unmatched_Count] DEFAULT ((0)),
    [Error_Message] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Created_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_sync_runs] ADD CONSTRAINT [PK_chapter_finder_administrator_sync_runs] PRIMARY KEY CLUSTERED ([Chapter_Finder_Administrator_Sync_Run_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_sync_runs] ADD CONSTRAINT [UQ_chapter_finder_administrator_sync_runs_Run_Key] UNIQUE NONCLUSTERED ([Run_Key] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_administrator_sync_runs] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_administrator_sync_runs_Status] CHECK ([Status]=N'cancelled' OR [Status]=N'failed' OR [Status]=N'partial' OR [Status]=N'succeeded' OR [Status]=N'running');
GO
