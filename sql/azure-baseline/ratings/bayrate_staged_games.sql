-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[bayrate_staged_games] (
    [RunID] int NOT NULL,
    [Source_Report_Ordinal] int NOT NULL,
    [Source_Game_Ordinal] int NOT NULL,
    [Source_Report_Name] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Game_ID] int NULL,
    [Tournament_Code] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Game_Date] date NULL,
    [Round] int NULL,
    [Pin_Player_1] int NULL,
    [Color_1] nvarchar(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Rank_1] nvarchar(16) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Pin_Player_2] int NULL,
    [Color_2] nvarchar(1) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Rank_2] nvarchar(16) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Handicap] int NULL,
    [Komi] decimal(5,1) NULL,
    [Result] nvarchar(8) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Sgf_Code] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Online] bit NULL,
    [Exclude] bit NULL,
    [Rated] bit NULL,
    [Elab_Date] date NULL,
    [Validation_Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_bayrate_staged_games_Validation_Status] DEFAULT (N'staged'),
    [Validation_Errors] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_bayrate_staged_games_Created_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [ratings].[bayrate_staged_games] ADD CONSTRAINT [PK_bayrate_staged_games] PRIMARY KEY CLUSTERED ([RunID] ASC, [Source_Report_Ordinal] ASC, [Source_Game_Ordinal] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_bayrate_staged_games_Tournament] ON [ratings].[bayrate_staged_games] ([RunID] ASC, [Tournament_Code] ASC, [Game_Date] ASC, [Round] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [ratings].[bayrate_staged_games] WITH CHECK ADD CONSTRAINT [CK_bayrate_staged_games_Validation_Status] CHECK ([Validation_Status]=N'ready_for_rating' OR [Validation_Status]=N'needs_review' OR [Validation_Status]=N'validation_failed' OR [Validation_Status]=N'staged');
GO
ALTER TABLE [ratings].[bayrate_staged_games] WITH CHECK ADD CONSTRAINT [FK_bayrate_staged_games_Tournament] FOREIGN KEY ([RunID], [Source_Report_Ordinal]) REFERENCES [ratings].[bayrate_staged_tournaments] ([RunID], [Source_Report_Ordinal]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
