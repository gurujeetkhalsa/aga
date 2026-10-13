-- Live Azure SQL metadata; database aga-membership; VIEW
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   VIEW [ratings].[vw_legacy_bayrate_games_source]
AS
SELECT g.[Game_ID], g.[Tournament_Code], CAST(g.[Game_Date] AS date) AS [Game_Date], g.[Round], g.[Pin_Player_1], g.[Color_1], g.[Rank_1],
       g.[Pin_Player_2], g.[Color_2], g.[Rank_2], g.[Handicap], g.[Komi], g.[Result], g.[Online], g.[Exclude], g.[Rated]
FROM [ratings].[games] AS g;

GO
