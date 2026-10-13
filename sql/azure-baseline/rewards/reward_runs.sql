-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [rewards].[reward_runs] (
    [RunID] int IDENTITY(1,1) NOT NULL,
    [Run_Type] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_reward_runs_Run_Type] DEFAULT (N'daily'),
    [Snapshot_Date] date NOT NULL,
    [Started_At] datetime2(0) NOT NULL CONSTRAINT [DF_reward_runs_Started_At] DEFAULT (sysutcdatetime()),
    [Completed_At] datetime2(0) NULL,
    [Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_reward_runs_Status] DEFAULT (N'running'),
    [SummaryJson] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Error_Message] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 );
GO
ALTER TABLE [rewards].[reward_runs] ADD CONSTRAINT [PK_reward_runs] PRIMARY KEY CLUSTERED ([RunID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_reward_runs_Snapshot_Date] ON [rewards].[reward_runs] ([Snapshot_Date] ASC, [Run_Type] ASC, [Status] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[reward_runs] WITH CHECK ADD CONSTRAINT [CK_reward_runs_Run_Type] CHECK ([Run_Type]=N'backfill' OR [Run_Type]=N'import' OR [Run_Type]=N'manual' OR [Run_Type]=N'daily');
GO
ALTER TABLE [rewards].[reward_runs] WITH CHECK ADD CONSTRAINT [CK_reward_runs_Status] CHECK ([Status]=N'cancelled' OR [Status]=N'failed' OR [Status]=N'succeeded' OR [Status]=N'running');
GO
