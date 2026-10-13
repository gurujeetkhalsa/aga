-- Copyright 2026, American Go Association, All rights reserved

-- Live Azure SQL stored procedure export.
-- Source object: [competition].[sp_refresh_clean_competition].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [competition].[sp_refresh_clean_competition]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Now DATETIME2(7) = SYSDATETIME();

    IF OBJECT_ID('tempdb..#TournamentMap') IS NOT NULL DROP TABLE #TournamentMap;
    IF OBJECT_ID('tempdb..#PendingTournamentCodes') IS NOT NULL DROP TABLE #PendingTournamentCodes;
    IF OBJECT_ID('tempdb..#ResolvedExactReview') IS NOT NULL DROP TABLE #ResolvedExactReview;
    IF OBJECT_ID('tempdb..#PendingExactReview') IS NOT NULL DROP TABLE #PendingExactReview;
    IF OBJECT_ID('tempdb..#PendingPairReview') IS NOT NULL DROP TABLE #PendingPairReview;

    CREATE TABLE #TournamentMap
    (
        SourceTournamentCode NVARCHAR(255) NOT NULL PRIMARY KEY,
        CanonicalTournamentCode NVARCHAR(255) NOT NULL
    );

    INSERT INTO #TournamentMap (SourceTournamentCode, CanonicalTournamentCode)
    SELECT
        t.Tournament_Code,
        COALESCE(mapped.CanonicalTournamentCode, t.Tournament_Code) AS CanonicalTournamentCode
    FROM [ratingsync].[tournaments] AS t
    OUTER APPLY
    (
        SELECT TOP (1)
            r.CanonicalTournamentCode
        FROM [competition].[tournament_duplicate_review] AS r
        CROSS APPLY STRING_SPLIT(r.TournamentCodes, ',') AS s
        WHERE r.[Status] = N'resolved'
          AND r.CanonicalTournamentCode IS NOT NULL
          AND LTRIM(RTRIM(s.value)) = t.Tournament_Code
        ORDER BY r.ReviewID
    ) AS mapped;

    CREATE TABLE #PendingTournamentCodes
    (
        SourceTournamentCode NVARCHAR(255) NOT NULL PRIMARY KEY
    );

    INSERT INTO #PendingTournamentCodes (SourceTournamentCode)
    SELECT DISTINCT LTRIM(RTRIM(s.value))
    FROM [competition].[tournament_duplicate_review] AS r
    CROSS APPLY STRING_SPLIT(r.TournamentCodes, ',') AS s
    WHERE r.[Status] <> N'resolved'
      AND LTRIM(RTRIM(s.value)) <> N'';

    CREATE TABLE #ResolvedExactReview
    (
        ReviewID BIGINT NOT NULL PRIMARY KEY,
        CanonicalGameID INT NOT NULL,
        GameDate DATE NOT NULL,
        TournamentCode NVARCHAR(255) NULL,
        Pin_Player_1 INT NULL,
        Color_1 NVARCHAR(20) NULL,
        Pin_Player_2 INT NULL,
        Color_2 NVARCHAR(20) NULL,
        Handicap INT NULL,
        Komi INT NULL,
        Result NVARCHAR(20) NULL
    );

    INSERT INTO #ResolvedExactReview
    (
        ReviewID, CanonicalGameID, GameDate, TournamentCode, Pin_Player_1, Color_1, Pin_Player_2, Color_2, Handicap, Komi, Result
    )
    SELECT
        ReviewID, CanonicalGameID, GameDate, TournamentCode, Pin_Player_1, Color_1, Pin_Player_2, Color_2, Handicap, Komi, Result
    FROM [competition].[game_duplicate_review]
    WHERE [ReviewType] = N'exact_early'
      AND [Status] = N'resolved'
      AND CanonicalGameID IS NOT NULL;

    CREATE TABLE #PendingExactReview
    (
        ReviewID BIGINT NOT NULL PRIMARY KEY,
        GameDate DATE NOT NULL,
        TournamentCode NVARCHAR(255) NULL,
        Pin_Player_1 INT NULL,
        Color_1 NVARCHAR(20) NULL,
        Pin_Player_2 INT NULL,
        Color_2 NVARCHAR(20) NULL,
        Handicap INT NULL,
        Komi INT NULL,
        Result NVARCHAR(20) NULL
    );

    INSERT INTO #PendingExactReview
    (
        ReviewID, GameDate, TournamentCode, Pin_Player_1, Color_1, Pin_Player_2, Color_2, Handicap, Komi, Result
    )
    SELECT
        ReviewID, GameDate, TournamentCode, Pin_Player_1, Color_1, Pin_Player_2, Color_2, Handicap, Komi, Result
    FROM [competition].[game_duplicate_review]
    WHERE [ReviewType] = N'exact_early'
      AND [Status] <> N'resolved';

    CREATE TABLE #PendingPairReview
    (
        ReviewID BIGINT NOT NULL PRIMARY KEY,
        GameDate DATE NOT NULL,
        TournamentCode NVARCHAR(255) NULL,
        PlayerLowAGAID INT NULL,
        PlayerHighAGAID INT NULL,
        Handicap INT NULL,
        Komi INT NULL,
        Result NVARCHAR(20) NULL
    );

    INSERT INTO #PendingPairReview
    (
        ReviewID, GameDate, TournamentCode, PlayerLowAGAID, PlayerHighAGAID, Handicap, Komi, Result
    )
    SELECT
        ReviewID, GameDate, TournamentCode, PlayerLowAGAID, PlayerHighAGAID, Handicap, Komi, Result
    FROM [competition].[game_duplicate_review]
    WHERE [ReviewType] = N'pair_early'
      AND [Status] <> N'resolved';

    TRUNCATE TABLE [competition].[games_clean];
    TRUNCATE TABLE [competition].[tournaments_clean];

    WITH tournament_base AS
    (
        SELECT
            m.CanonicalTournamentCode,
            t.Tournament_Code,
            t.Tournament_Descr,
            t.Tournament_Date,
            t.City,
            t.State_Code,
            t.Country_Code,
            t.Rounds,
            t.Total_Players,
            t.Wallist,
            t.Elab_Date,
            t.[status],
            ROW_NUMBER() OVER
            (
                PARTITION BY m.CanonicalTournamentCode
                ORDER BY CASE WHEN t.Tournament_Code = m.CanonicalTournamentCode THEN 0 ELSE 1 END,
                         t.Tournament_Code
            ) AS rn
        FROM [ratingsync].[tournaments] AS t
        INNER JOIN #TournamentMap AS m
            ON m.SourceTournamentCode = t.Tournament_Code
    ),
    tournament_codes AS
    (
        SELECT
            m.CanonicalTournamentCode,
            STRING_AGG(m.SourceTournamentCode, ', ') WITHIN GROUP (ORDER BY m.SourceTournamentCode) AS SourceTournamentCodes,
            COUNT(*) AS SourceTournamentCount,
            MAX(CASE WHEN p.SourceTournamentCode IS NOT NULL THEN 1 ELSE 0 END) AS HasPendingTournamentReview
        FROM #TournamentMap AS m
        LEFT JOIN #PendingTournamentCodes AS p
            ON p.SourceTournamentCode = m.SourceTournamentCode
        GROUP BY m.CanonicalTournamentCode
    )
    INSERT INTO [competition].[tournaments_clean]
    (
        [CanonicalTournamentCode], [RepresentativeTournamentCode], [Tournament_Descr], [Tournament_Date], [City],
        [State_Code], [Country_Code], [Rounds], [Total_Players], [Wallist], [Elab_Date], [SourceStatus],
        [SourceTournamentCodes], [SourceTournamentCount], [HasPendingTournamentReview], [LastRefreshed]
    )
    SELECT
        b.CanonicalTournamentCode,
        b.Tournament_Code,
        b.Tournament_Descr,
        b.Tournament_Date,
        b.City,
        b.State_Code,
        b.Country_Code,
        b.Rounds,
        b.Total_Players,
        b.Wallist,
        b.Elab_Date,
        b.[status],
        c.SourceTournamentCodes,
        c.SourceTournamentCount,
        CAST(c.HasPendingTournamentReview AS BIT),
        @Now
    FROM tournament_base AS b
    INNER JOIN tournament_codes AS c
        ON c.CanonicalTournamentCode = b.CanonicalTournamentCode
    WHERE b.rn = 1;

    WITH raw_games AS
    (
        SELECT
            g.Game_ID,
            g.Tournament_Code,
            tm.CanonicalTournamentCode,
            g.Game_Date,
            g.[Round],
            g.Pin_Player_1,
            g.Color_1,
            g.Rank_1,
            g.Pin_Player_2,
            g.Color_2,
            g.Rank_2,
            g.Handicap,
            g.Komi,
            g.Result,
            g.Sgf_Code,
            g.Online,
            g.[Exclude],
            g.Rated,
            g.Elab_Date,
            CASE
                WHEN EXISTS
                (
                    SELECT 1
                    FROM #PendingExactReview AS r
                    WHERE r.GameDate = g.Game_Date
                      AND ISNULL(r.TournamentCode, N'<NULL>') = ISNULL(g.Tournament_Code, N'<NULL>')
                      AND ISNULL(r.Pin_Player_1, -1) = ISNULL(g.Pin_Player_1, -1)
                      AND ISNULL(r.Color_1, N'<NULL>') = ISNULL(g.Color_1, N'<NULL>')
                      AND ISNULL(r.Pin_Player_2, -1) = ISNULL(g.Pin_Player_2, -1)
                      AND ISNULL(r.Color_2, N'<NULL>') = ISNULL(g.Color_2, N'<NULL>')
                      AND ISNULL(r.Handicap, -999) = ISNULL(g.Handicap, -999)
                      AND ISNULL(r.Komi, -999) = ISNULL(g.Komi, -999)
                      AND ISNULL(r.Result, N'<NULL>') = ISNULL(g.Result, N'<NULL>')
                ) THEN 1 ELSE 0 END AS HasPendingExactDuplicateReview,
            CASE
                WHEN EXISTS
                (
                    SELECT 1
                    FROM #PendingPairReview AS r
                    WHERE r.GameDate = g.Game_Date
                      AND ISNULL(r.TournamentCode, N'<NULL>') = ISNULL(g.Tournament_Code, N'<NULL>')
                      AND ISNULL(r.PlayerLowAGAID, -1) = ISNULL(CASE WHEN g.Pin_Player_1 < g.Pin_Player_2 THEN g.Pin_Player_1 ELSE g.Pin_Player_2 END, -1)
                      AND ISNULL(r.PlayerHighAGAID, -1) = ISNULL(CASE WHEN g.Pin_Player_1 < g.Pin_Player_2 THEN g.Pin_Player_2 ELSE g.Pin_Player_1 END, -1)
                      AND ISNULL(r.Handicap, -999) = ISNULL(g.Handicap, -999)
                      AND ISNULL(r.Komi, -999) = ISNULL(g.Komi, -999)
                      AND ISNULL(r.Result, N'<NULL>') = ISNULL(g.Result, N'<NULL>')
                ) THEN 1 ELSE 0 END AS HasPendingPairDuplicateReview
        FROM [ratingsync].[games] AS g
        LEFT JOIN #TournamentMap AS tm
            ON tm.SourceTournamentCode = g.Tournament_Code
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM #ResolvedExactReview AS r
            WHERE r.GameDate = g.Game_Date
              AND ISNULL(r.TournamentCode, N'<NULL>') = ISNULL(g.Tournament_Code, N'<NULL>')
              AND ISNULL(r.Pin_Player_1, -1) = ISNULL(g.Pin_Player_1, -1)
              AND ISNULL(r.Color_1, N'<NULL>') = ISNULL(g.Color_1, N'<NULL>')
              AND ISNULL(r.Pin_Player_2, -1) = ISNULL(g.Pin_Player_2, -1)
              AND ISNULL(r.Color_2, N'<NULL>') = ISNULL(g.Color_2, N'<NULL>')
              AND ISNULL(r.Handicap, -999) = ISNULL(g.Handicap, -999)
              AND ISNULL(r.Komi, -999) = ISNULL(g.Komi, -999)
              AND ISNULL(r.Result, N'<NULL>') = ISNULL(g.Result, N'<NULL>')
              AND g.Game_ID <> r.CanonicalGameID
        )
    )
    INSERT INTO [competition].[games_clean]
    (
        [SourceGameID], [CanonicalTournamentCode], [SourceTournamentCode], [Game_Date], [Round], [Pin_Player_1], [Color_1],
        [Rank_1], [Pin_Player_2], [Color_2], [Rank_2], [Handicap], [Komi], [Result], [Sgf_Code], [Online],
        [Exclude], [Rated], [Elab_Date], [HasPendingExactDuplicateReview], [HasPendingPairDuplicateReview], [LastRefreshed]
    )
    SELECT
        Game_ID,
        CanonicalTournamentCode,
        Tournament_Code,
        Game_Date,
        [Round],
        Pin_Player_1,
        Color_1,
        Rank_1,
        Pin_Player_2,
        Color_2,
        Rank_2,
        Handicap,
        Komi,
        Result,
        Sgf_Code,
        Online,
        [Exclude],
        Rated,
        Elab_Date,
        CAST(HasPendingExactDuplicateReview AS BIT),
        CAST(HasPendingPairDuplicateReview AS BIT),
        @Now
    FROM raw_games;
END;
GO
