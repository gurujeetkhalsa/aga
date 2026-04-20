/*
Ratings Explorer SQL cleanup applied to production on 2026-04-20.

Purpose:
- make core ratings columns indexable
- convert ratings.games.Round from text to int
- add indexes used by Ratings Explorer SQL fallback and snapshot refresh paths

Run only after validating:
- ratings.games.Game_ID has no nulls and is unique
- ratings.games.Round values all convert to int
- ratings.tournaments.Tournament_Code has no blanks and is unique
- ratings.ratings.id has no nulls and is unique
*/

ALTER TABLE ratings.games ALTER COLUMN Game_ID int NOT NULL;
ALTER TABLE ratings.games ALTER COLUMN Tournament_Code nvarchar(32) NOT NULL;
ALTER TABLE ratings.games ALTER COLUMN [Round] int NULL;
ALTER TABLE ratings.games ALTER COLUMN Sgf_Code nvarchar(128) NULL;

IF EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'ratings.ratings')
      AND name = N'IX_ratings_Current_ByPlayer'
)
BEGIN
    DROP INDEX IX_ratings_Current_ByPlayer ON ratings.ratings;
END;

ALTER TABLE ratings.ratings ALTER COLUMN id int NOT NULL;
ALTER TABLE ratings.ratings ALTER COLUMN Tournament_Code nvarchar(32) NULL;

ALTER TABLE ratings.tournaments ALTER COLUMN Tournament_Code nvarchar(32) NOT NULL;
ALTER TABLE ratings.tournaments ALTER COLUMN Tournament_Descr nvarchar(128) NULL;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'ratings.games')
      AND name = N'UX_games_Game_ID'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_games_Game_ID
    ON ratings.games(Game_ID);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'ratings.tournaments')
      AND name = N'UX_tournaments_Tournament_Code'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_tournaments_Tournament_Code
    ON ratings.tournaments(Tournament_Code);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'ratings.ratings')
      AND name = N'UX_ratings_id'
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX UX_ratings_id
    ON ratings.ratings(id);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'ratings.ratings')
      AND name = N'IX_ratings_Current_ByPlayer'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_ratings_Current_ByPlayer
    ON ratings.ratings(Pin_Player, Elab_Date DESC, id DESC)
    INCLUDE (Rating, Sigma, Tournament_Code);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'ratings.games')
      AND name = N'IX_games_Tournament_Date_Round'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_games_Tournament_Date_Round
    ON ratings.games(Tournament_Code, Game_Date, [Round], Game_ID)
    INCLUDE (
        Result,
        Handicap,
        Komi,
        Color_1,
        Color_2,
        Sgf_Code,
        Rank_1,
        Rank_2,
        Pin_Player_1,
        Pin_Player_2
    );
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'ratings.games')
      AND name = N'IX_games_Pin_Player_1_Date'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_games_Pin_Player_1_Date
    ON ratings.games(Pin_Player_1, Game_Date DESC, Tournament_Code)
    INCLUDE (
        Game_ID,
        [Round],
        Result,
        Color_1,
        Handicap,
        Sgf_Code,
        Rank_1,
        Rank_2,
        Pin_Player_2
    );
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'ratings.games')
      AND name = N'IX_games_Pin_Player_2_Date'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_games_Pin_Player_2_Date
    ON ratings.games(Pin_Player_2, Game_Date DESC, Tournament_Code)
    INCLUDE (
        Game_ID,
        [Round],
        Result,
        Color_2,
        Handicap,
        Sgf_Code,
        Rank_1,
        Rank_2,
        Pin_Player_1
    );
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'ratings.games')
      AND name = N'IX_games_Tournament_Match'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_games_Tournament_Match
    ON ratings.games(Tournament_Code, [Round], Pin_Player_1, Pin_Player_2, Game_Date)
    INCLUDE (Game_ID, Sgf_Code);
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'ratings.tournaments')
      AND name = N'IX_tournaments_Date_Code'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_tournaments_Date_Code
    ON ratings.tournaments(Tournament_Date DESC, Tournament_Code)
    INCLUDE (
        Tournament_Descr,
        City,
        State_Code,
        Country_Code,
        Rounds,
        Total_Players
    );
END;
