-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[chapter_finder_sync_runs] (
    [Chapter_Finder_Sync_Run_ID] bigint IDENTITY(1,1) NOT NULL,
    [Run_Key] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Run_Type] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Run_Type] DEFAULT (N'manual'),
    [Source_Page_Url] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Started_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Started_At] DEFAULT (sysutcdatetime()),
    [Completed_At] datetime2(0) NULL,
    [Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Status] DEFAULT (N'running'),
    [Point_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Point_Count] DEFAULT ((0)),
    [Detail_Success_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Detail_Success_Count] DEFAULT ((0)),
    [Detail_Failure_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Detail_Failure_Count] DEFAULT ((0)),
    [Inserted_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Inserted_Count] DEFAULT ((0)),
    [Updated_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Updated_Count] DEFAULT ((0)),
    [Unchanged_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Unchanged_Count] DEFAULT ((0)),
    [Marked_Inactive_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Marked_Inactive_Count] DEFAULT ((0)),
    [Summary_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Error_Message] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Created_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [clubexpress].[chapter_finder_sync_runs] ADD CONSTRAINT [PK_chapter_finder_sync_runs] PRIMARY KEY CLUSTERED ([Chapter_Finder_Sync_Run_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_sync_runs] ADD CONSTRAINT [UQ_chapter_finder_sync_runs_Run_Key] UNIQUE NONCLUSTERED ([Run_Key] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_sync_runs] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_sync_runs_Run_Type] CHECK ([Run_Type]=N'backfill' OR [Run_Type]=N'scheduled' OR [Run_Type]=N'manual');
GO
ALTER TABLE [clubexpress].[chapter_finder_sync_runs] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_sync_runs_Status] CHECK ([Status]=N'cancelled' OR [Status]=N'failed' OR [Status]=N'partial' OR [Status]=N'succeeded' OR [Status]=N'running');
GO
ALTER TABLE [clubexpress].[chapter_finder_sync_runs] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_sync_runs_Summary_Json] CHECK ([Summary_Json] IS NULL OR isjson([Summary_Json])=(1));
GO
