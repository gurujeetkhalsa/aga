-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[bayrate_runs] (
    [RunID] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_RunID] DEFAULT (NEXT VALUE FOR [ratings].[bayrate_run_id_seq]),
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Created_At] DEFAULT (sysutcdatetime()),
    [Last_Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Last_Updated_At] DEFAULT (sysutcdatetime()),
    [Created_By] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL CONSTRAINT [DF_bayrate_stage_runs_Created_By] DEFAULT (suser_sname()),
    [Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Status] DEFAULT (N'staged'),
    [Source_Report_Count] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Source_Report_Count] DEFAULT ((0)),
    [Source_Report_Names] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Tournament_Count] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Tournament_Count] DEFAULT ((0)),
    [Game_Count] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Game_Count] DEFAULT ((0)),
    [Validation_Error_Count] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Validation_Error_Count] DEFAULT ((0)),
    [Ready_Tournament_Count] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Ready_Tournament_Count] DEFAULT ((0)),
    [Needs_Review_Count] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Needs_Review_Count] DEFAULT ((0)),
    [Validation_Failed_Count] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Validation_Failed_Count] DEFAULT ((0)),
    [SummaryJson] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 );
GO
ALTER TABLE [ratings].[bayrate_runs] ADD CONSTRAINT [PK_bayrate_stage_runs] PRIMARY KEY CLUSTERED ([RunID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [ratings].[bayrate_runs] WITH CHECK ADD CONSTRAINT [CK_bayrate_stage_runs_Status] CHECK ([Status]=N'ready_for_rating' OR [Status]=N'needs_review' OR [Status]=N'validation_failed' OR [Status]=N'staged');
GO
