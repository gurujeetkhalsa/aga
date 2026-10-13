-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[bayrate_staged_tournaments] (
    [RunID] int NOT NULL,
    [Source_Report_Ordinal] int NOT NULL,
    [Source_Report_Name] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Source_Report_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Tournament_Code] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Original_Tournament_Code] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Tournament_Code_Source] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Tournament_Descr] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Normalized_Title] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Tournament_Date] date NULL,
    [City] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [State_Code] nvarchar(16) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Country_Code] nvarchar(16) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Rounds] int NULL,
    [Total_Players] int NULL,
    [Wallist] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Elab_Date] date NULL,
    [Validation_Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_bayrate_staged_tournaments_Validation_Status] DEFAULT (N'staged'),
    [Validation_Errors] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Parser_Warnings] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Duplicate_Candidate_Code] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Duplicate_Score] decimal(5,4) NULL,
    [Review_Reason] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [MetadataJson] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_bayrate_staged_tournaments_Created_At] DEFAULT (sysutcdatetime()),
    [Host_ChapterID] int NULL,
    [Host_ChapterCode] nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Host_ChapterName] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Reward_Event_Key] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Reward_Event_Name] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Reward_Is_State_Championship] bit NOT NULL CONSTRAINT [DF_bayrate_staged_tournaments_Reward_Is_State_Championship] DEFAULT ((0))
 );
GO
ALTER TABLE [ratings].[bayrate_staged_tournaments] ADD CONSTRAINT [PK_bayrate_staged_tournaments] PRIMARY KEY CLUSTERED ([RunID] ASC, [Source_Report_Ordinal] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_bayrate_staged_tournaments_Status] ON [ratings].[bayrate_staged_tournaments] ([Validation_Status] ASC, [Tournament_Date] ASC, [Tournament_Code] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_bayrate_staged_tournaments_Host_Chapter] ON [ratings].[bayrate_staged_tournaments] ([Host_ChapterID] ASC, [Reward_Event_Key] ASC, [Tournament_Date] ASC) WHERE ([Host_ChapterID] IS NOT NULL) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [ratings].[bayrate_staged_tournaments] WITH CHECK ADD CONSTRAINT [CK_bayrate_staged_tournaments_Validation_Status] CHECK ([Validation_Status]=N'ready_for_rating' OR [Validation_Status]=N'needs_review' OR [Validation_Status]=N'validation_failed' OR [Validation_Status]=N'staged');
GO
ALTER TABLE [ratings].[bayrate_staged_tournaments] WITH CHECK ADD CONSTRAINT [CK_bayrate_staged_tournaments_Code_Source] CHECK ([Tournament_Code_Source]=N'parser' OR [Tournament_Code_Source]=N'reused' OR [Tournament_Code_Source]=N'generated');
GO
ALTER TABLE [ratings].[bayrate_staged_tournaments] WITH CHECK ADD CONSTRAINT [FK_bayrate_staged_tournaments_RunID] FOREIGN KEY ([RunID]) REFERENCES [ratings].[bayrate_runs] ([RunID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
