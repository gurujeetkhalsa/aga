-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[news_archive_runs] (
    [News_Archive_Run_ID] bigint IDENTITY(1,1) NOT NULL,
    [Run_Key] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Run_Type] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_news_archive_runs_Run_Type] DEFAULT (N'manual'),
    [Source_Mode] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Source_Description] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Started_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_archive_runs_Started_At] DEFAULT (sysutcdatetime()),
    [Completed_At] datetime2(0) NULL,
    [Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_news_archive_runs_Status] DEFAULT (N'running'),
    [Article_Count] int NOT NULL CONSTRAINT [DF_news_archive_runs_Article_Count] DEFAULT ((0)),
    [Asset_Count] int NOT NULL CONSTRAINT [DF_news_archive_runs_Asset_Count] DEFAULT ((0)),
    [Blob_Account] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Blob_Container] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Blob_Prefix] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Manifest_Blob_Path] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Summary_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Error_Message] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_news_archive_runs_Created_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [clubexpress].[news_archive_runs] ADD CONSTRAINT [PK_news_archive_runs] PRIMARY KEY CLUSTERED ([News_Archive_Run_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[news_archive_runs] ADD CONSTRAINT [UQ_news_archive_runs_Run_Key] UNIQUE NONCLUSTERED ([Run_Key] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[news_archive_runs] WITH CHECK ADD CONSTRAINT [CK_news_archive_runs_Run_Type] CHECK ([Run_Type]=N'scheduled' OR [Run_Type]=N'backfill' OR [Run_Type]=N'manual' OR [Run_Type]=N'sample');
GO
ALTER TABLE [clubexpress].[news_archive_runs] WITH CHECK ADD CONSTRAINT [CK_news_archive_runs_Status] CHECK ([Status]=N'cancelled' OR [Status]=N'failed' OR [Status]=N'partial' OR [Status]=N'succeeded' OR [Status]=N'running');
GO
ALTER TABLE [clubexpress].[news_archive_runs] WITH CHECK ADD CONSTRAINT [CK_news_archive_runs_Article_Count] CHECK ([Article_Count]>=(0));
GO
ALTER TABLE [clubexpress].[news_archive_runs] WITH CHECK ADD CONSTRAINT [CK_news_archive_runs_Asset_Count] CHECK ([Asset_Count]>=(0));
GO
ALTER TABLE [clubexpress].[news_archive_runs] WITH CHECK ADD CONSTRAINT [CK_news_archive_runs_Summary_Json] CHECK ([Summary_Json] IS NULL OR isjson([Summary_Json])=(1));
GO
