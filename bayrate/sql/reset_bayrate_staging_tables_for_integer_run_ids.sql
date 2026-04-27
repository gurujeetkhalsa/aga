IF OBJECT_ID(N'ratings.bayrate_staged_ratings', N'U') IS NOT NULL
    DROP TABLE [ratings].[bayrate_staged_ratings];

IF OBJECT_ID(N'ratings.bayrate_staged_games', N'U') IS NOT NULL
    DROP TABLE [ratings].[bayrate_staged_games];

IF OBJECT_ID(N'ratings.bayrate_staged_tournaments', N'U') IS NOT NULL
    DROP TABLE [ratings].[bayrate_staged_tournaments];

IF OBJECT_ID(N'ratings.bayrate_runs', N'U') IS NOT NULL
    DROP TABLE [ratings].[bayrate_runs];

IF OBJECT_ID(N'ratings.bayrate_run_id_seq', N'SO') IS NOT NULL
    DROP SEQUENCE [ratings].[bayrate_run_id_seq];
