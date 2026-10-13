IF OBJECT_ID(N'[ratings].[vw_glicko2_seeded_event_ratings_latest]', N'V') IS NOT NULL
    DROP VIEW [ratings].[vw_glicko2_seeded_event_ratings_latest];
GO

IF OBJECT_ID(N'[ratings].[vw_glicko2_latest_player_state]', N'V') IS NOT NULL
    DROP VIEW [ratings].[vw_glicko2_latest_player_state];
GO

IF OBJECT_ID(N'[competition].[vw_rated_games_glicko2_input]', N'V') IS NOT NULL
    DROP VIEW [competition].[vw_rated_games_glicko2_input];
GO

IF OBJECT_ID(N'[ratings].[glicko2_seeded_event_ratings]', N'U') IS NOT NULL
    DROP TABLE [ratings].[glicko2_seeded_event_ratings];
GO

IF OBJECT_ID(N'[ratings].[glicko2_run_metrics]', N'U') IS NOT NULL
    DROP TABLE [ratings].[glicko2_run_metrics];
GO

IF OBJECT_ID(N'[ratings].[glicko2_player_state]', N'U') IS NOT NULL
    DROP TABLE [ratings].[glicko2_player_state];
GO

IF OBJECT_ID(N'[ratings].[glicko2_game_results]', N'U') IS NOT NULL
    DROP TABLE [ratings].[glicko2_game_results];
GO

IF OBJECT_ID(N'[ratings].[glicko2_run_parameters]', N'U') IS NOT NULL
    DROP TABLE [ratings].[glicko2_run_parameters];
GO

IF OBJECT_ID(N'[ratings].[glicko2_runs]', N'U') IS NOT NULL
    DROP TABLE [ratings].[glicko2_runs];
GO
