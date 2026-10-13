-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [competition].[games_clean] (
    [SourceGameID] int NOT NULL,
    [CanonicalTournamentCode] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [SourceTournamentCode] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Game_Date] date NULL,
    [Round] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Pin_Player_1] int NULL,
    [Color_1] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Rank_1] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Pin_Player_2] int NULL,
    [Color_2] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Rank_2] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Handicap] int NULL,
    [Komi] int NULL,
    [Result] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Sgf_Code] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Online] int NULL,
    [Exclude] int NULL,
    [Rated] int NULL,
    [Elab_Date] date NULL,
    [HasPendingExactDuplicateReview] bit NOT NULL,
    [HasPendingPairDuplicateReview] bit NOT NULL,
    [CreatedDate] datetime2(7) NOT NULL CONSTRAINT [DF_games_clean_CreatedDate] DEFAULT (sysdatetime()),
    [LastRefreshed] datetime2(7) NOT NULL CONSTRAINT [DF_games_clean_LastRefreshed] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [competition].[games_clean] ADD CONSTRAINT [PK_games_clean] PRIMARY KEY CLUSTERED ([SourceGameID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_games_clean_CanonicalTournamentCode] ON [competition].[games_clean] ([CanonicalTournamentCode] ASC, [Game_Date] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
