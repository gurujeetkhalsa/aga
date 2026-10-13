-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[games] (
    [Game_ID] int NOT NULL,
    [Tournament_Code] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Game_Date] date NULL,
    [Round] int NULL,
    [Pin_Player_1] int NULL,
    [Color_1] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Rank_1] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Pin_Player_2] int NULL,
    [Color_2] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Rank_2] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Handicap] int NULL,
    [Komi] int NULL,
    [Result] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Sgf_Code] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Online] int NULL,
    [Exclude] int NULL,
    [Rated] int NULL,
    [Elab_Date] date NULL
 );
GO
CREATE UNIQUE NONCLUSTERED INDEX [UX_games_Game_ID] ON [ratings].[games] ([Game_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_games_Tournament_Date_Round] ON [ratings].[games] ([Tournament_Code] ASC, [Game_Date] ASC, [Round] ASC, [Game_ID] ASC) INCLUDE ([Result], [Handicap], [Komi], [Color_1], [Color_2], [Sgf_Code], [Rank_1], [Rank_2], [Pin_Player_1], [Pin_Player_2]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_games_Pin_Player_1_Date] ON [ratings].[games] ([Pin_Player_1] ASC, [Game_Date] DESC, [Tournament_Code] ASC) INCLUDE ([Game_ID], [Round], [Result], [Color_1], [Handicap], [Sgf_Code], [Rank_1], [Rank_2], [Pin_Player_2]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_games_Pin_Player_2_Date] ON [ratings].[games] ([Pin_Player_2] ASC, [Game_Date] DESC, [Tournament_Code] ASC) INCLUDE ([Game_ID], [Round], [Result], [Color_2], [Handicap], [Sgf_Code], [Rank_1], [Rank_2], [Pin_Player_1]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_games_Tournament_Match] ON [ratings].[games] ([Tournament_Code] ASC, [Round] ASC, [Pin_Player_1] ASC, [Pin_Player_2] ASC, [Game_Date] ASC) INCLUDE ([Game_ID], [Sgf_Code]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
