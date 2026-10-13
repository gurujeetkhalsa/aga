-- Live Azure SQL metadata; database aga-membership; VIEW
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   VIEW [competition].[vw_rated_games_analysis]
AS
SELECT
    g.[SourceGameID],
    g.[CanonicalTournamentCode],
    t.[Tournament_Date],
    t.[Tournament_Descr],
    t.[City],
    t.[State_Code],
    t.[Country_Code],
    t.[SourceTournamentCount],
    t.[HasPendingTournamentReview],
    g.[SourceTournamentCode],
    g.[Game_Date],
    g.[Round],
    g.[Handicap],
    g.[Komi],
    g.[Result],
    g.[Online],
    g.[Elab_Date],
    WhiteAGAID = CASE WHEN g.[Color_1] = N'W' THEN g.[Pin_Player_1] ELSE g.[Pin_Player_2] END,
    WhiteRank = CASE WHEN g.[Color_1] = N'W' THEN g.[Rank_1] ELSE g.[Rank_2] END,
    BlackAGAID = CASE WHEN g.[Color_1] = N'B' THEN g.[Pin_Player_1] ELSE g.[Pin_Player_2] END,
    BlackRank = CASE WHEN g.[Color_1] = N'B' THEN g.[Rank_1] ELSE g.[Rank_2] END,
    WinnerAGAID = CASE g.[Result]
        WHEN N'W' THEN CASE WHEN g.[Color_1] = N'W' THEN g.[Pin_Player_1] ELSE g.[Pin_Player_2] END
        WHEN N'B' THEN CASE WHEN g.[Color_1] = N'B' THEN g.[Pin_Player_1] ELSE g.[Pin_Player_2] END
        ELSE NULL
    END,
    LoserAGAID = CASE g.[Result]
        WHEN N'W' THEN CASE WHEN g.[Color_1] = N'B' THEN g.[Pin_Player_1] ELSE g.[Pin_Player_2] END
        WHEN N'B' THEN CASE WHEN g.[Color_1] = N'W' THEN g.[Pin_Player_1] ELSE g.[Pin_Player_2] END
        ELSE NULL
    END,
    WinnerColor = g.[Result],
    IsRated = CAST(1 AS BIT),
    IsExcluded = CAST(0 AS BIT),
    g.[HasPendingExactDuplicateReview],
    g.[HasPendingPairDuplicateReview],
    HasAnyPendingReview = CAST(
        CASE
            WHEN g.[HasPendingExactDuplicateReview] = 1
              OR g.[HasPendingPairDuplicateReview] = 1
              OR t.[HasPendingTournamentReview] = 1
            THEN 1 ELSE 0
        END AS BIT
    )
FROM [competition].[games_clean] AS g
INNER JOIN [competition].[tournaments_clean] AS t
    ON t.[CanonicalTournamentCode] = g.[CanonicalTournamentCode]
WHERE g.[Rated] = 1
  AND ISNULL(g.[Exclude], 0) = 0;
GO
