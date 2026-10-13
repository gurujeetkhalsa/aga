-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[zz_20260325_bayrate_event_game_results] (
    [ModelLabel] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [RunID] bigint NOT NULL,
    [EventOrdinal] int NOT NULL,
    [EventKey] nvarchar(300) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [EventDate] date NOT NULL,
    [Tournament_Code] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [SourceGameID] int NOT NULL,
    [WhiteAGAID] int NOT NULL,
    [BlackAGAID] int NOT NULL,
    [Handicap] int NOT NULL,
    [Komi] float NOT NULL,
    [WhiteWins] bit NOT NULL,
    [WhiteSeedBefore] float NOT NULL,
    [BlackSeedBefore] float NOT NULL,
    [PreEventExpectedWhite] float NOT NULL,
    [PostEventExpectedWhite] float NOT NULL,
    [CreatedDate] datetime2(7) NOT NULL CONSTRAINT [DF_bayrate_event_game_results_CreatedDate] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [ratings].[zz_20260325_bayrate_event_game_results] ADD CONSTRAINT [PK_bayrate_event_game_results] PRIMARY KEY CLUSTERED ([ModelLabel] ASC, [SourceGameID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [ratings].[zz_20260325_bayrate_event_game_results] WITH CHECK ADD CONSTRAINT [FK_bayrate_event_game_results_RunID] FOREIGN KEY ([RunID]) REFERENCES [ratings].[zz_20260325_bayrate_runs] ([RunID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
