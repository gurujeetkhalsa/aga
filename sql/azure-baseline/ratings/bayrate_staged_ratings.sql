-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[bayrate_staged_ratings] (
    [RunID] int NOT NULL,
    [Event_Ordinal] int NOT NULL,
    [Player_Ordinal] int NOT NULL,
    [Event_Source] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Event_Key] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Tournament_Code] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Staged_Tournament_Code] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Replaced_Production_Code] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Source_Report_Ordinal] int NULL,
    [Pin_Player] int NOT NULL,
    [Rating] float NULL,
    [Sigma] float NULL,
    [Elab_Date] date NULL,
    [Rank_Seed] float NULL,
    [Seed_Before_Closing_Boundary] float NULL,
    [Prior_Rating] float NULL,
    [Prior_Sigma] float NULL,
    [Planned_Rating_Row_ID] int NULL,
    [Production_Rating_Row_ID] int NULL,
    [Rating_Delta] float NULL,
    [Sigma_Delta] float NULL,
    [MetadataJson] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_bayrate_staged_ratings_Created_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [ratings].[bayrate_staged_ratings] ADD CONSTRAINT [PK_bayrate_staged_ratings] PRIMARY KEY CLUSTERED ([RunID] ASC, [Event_Ordinal] ASC, [Pin_Player] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_bayrate_staged_ratings_Tournament] ON [ratings].[bayrate_staged_ratings] ([RunID] ASC, [Tournament_Code] ASC, [Event_Ordinal] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_bayrate_staged_ratings_Player] ON [ratings].[bayrate_staged_ratings] ([RunID] ASC, [Pin_Player] ASC, [Event_Ordinal] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [ratings].[bayrate_staged_ratings] WITH CHECK ADD CONSTRAINT [CK_bayrate_staged_ratings_Event_Source] CHECK ([Event_Source]=N'production' OR [Event_Source]=N'staged');
GO
ALTER TABLE [ratings].[bayrate_staged_ratings] WITH CHECK ADD CONSTRAINT [FK_bayrate_staged_ratings_RunID] FOREIGN KEY ([RunID]) REFERENCES [ratings].[bayrate_runs] ([RunID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
