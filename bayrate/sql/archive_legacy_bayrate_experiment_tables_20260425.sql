/*
Archive the March 25 BayRate experiment tables before installing the report
staging schema.

This script renames tables only. It does not delete data. Run the inventory
queries below first if you want row counts or column snapshots for your notes.
*/

SELECT
    s.name AS schema_name,
    t.name AS table_name,
    SUM(p.rows) AS row_count
FROM sys.tables AS t
JOIN sys.schemas AS s
    ON s.schema_id = t.schema_id
LEFT JOIN sys.partitions AS p
    ON p.object_id = t.object_id
   AND p.index_id IN (0, 1)
WHERE s.name = N'ratings'
  AND t.name IN
  (
      N'bayrate_control',
      N'bayrate_event_game_results',
      N'bayrate_event_index',
      N'bayrate_event_player_ratings',
      N'bayrate_player_checkpoint',
      N'bayrate_run_metrics',
      N'bayrate_runs'
  )
GROUP BY s.name, t.name
ORDER BY s.name, t.name;

IF EXISTS
(
    SELECT 1
    FROM
    (
        VALUES
            (N'bayrate_control', N'zz_20260325_bayrate_control'),
            (N'bayrate_event_game_results', N'zz_20260325_bayrate_event_game_results'),
            (N'bayrate_event_index', N'zz_20260325_bayrate_event_index'),
            (N'bayrate_event_player_ratings', N'zz_20260325_bayrate_event_player_ratings'),
            (N'bayrate_player_checkpoint', N'zz_20260325_bayrate_player_checkpoint'),
            (N'bayrate_run_metrics', N'zz_20260325_bayrate_run_metrics'),
            (N'bayrate_runs', N'zz_20260325_bayrate_runs')
    ) AS names(old_name, archive_name)
    WHERE OBJECT_ID(N'ratings.' + names.old_name, N'U') IS NOT NULL
      AND OBJECT_ID(N'ratings.' + names.archive_name, N'U') IS NOT NULL
)
BEGIN
    THROW 51001, N'Cannot archive legacy BayRate tables because at least one target archive table already exists.', 1;
END;

IF OBJECT_ID(N'ratings.bayrate_control', N'U') IS NOT NULL
   AND OBJECT_ID(N'ratings.zz_20260325_bayrate_control', N'U') IS NULL
    EXEC sp_rename N'ratings.bayrate_control', N'zz_20260325_bayrate_control';

IF OBJECT_ID(N'ratings.bayrate_event_game_results', N'U') IS NOT NULL
   AND OBJECT_ID(N'ratings.zz_20260325_bayrate_event_game_results', N'U') IS NULL
    EXEC sp_rename N'ratings.bayrate_event_game_results', N'zz_20260325_bayrate_event_game_results';

IF OBJECT_ID(N'ratings.bayrate_event_index', N'U') IS NOT NULL
   AND OBJECT_ID(N'ratings.zz_20260325_bayrate_event_index', N'U') IS NULL
    EXEC sp_rename N'ratings.bayrate_event_index', N'zz_20260325_bayrate_event_index';

IF OBJECT_ID(N'ratings.bayrate_event_player_ratings', N'U') IS NOT NULL
   AND OBJECT_ID(N'ratings.zz_20260325_bayrate_event_player_ratings', N'U') IS NULL
    EXEC sp_rename N'ratings.bayrate_event_player_ratings', N'zz_20260325_bayrate_event_player_ratings';

IF OBJECT_ID(N'ratings.bayrate_player_checkpoint', N'U') IS NOT NULL
   AND OBJECT_ID(N'ratings.zz_20260325_bayrate_player_checkpoint', N'U') IS NULL
    EXEC sp_rename N'ratings.bayrate_player_checkpoint', N'zz_20260325_bayrate_player_checkpoint';

IF OBJECT_ID(N'ratings.bayrate_run_metrics', N'U') IS NOT NULL
   AND OBJECT_ID(N'ratings.zz_20260325_bayrate_run_metrics', N'U') IS NULL
    EXEC sp_rename N'ratings.bayrate_run_metrics', N'zz_20260325_bayrate_run_metrics';

IF OBJECT_ID(N'ratings.bayrate_runs', N'U') IS NOT NULL
   AND OBJECT_ID(N'ratings.zz_20260325_bayrate_runs', N'U') IS NULL
    EXEC sp_rename N'ratings.bayrate_runs', N'zz_20260325_bayrate_runs';

SELECT
    s.name AS schema_name,
    t.name AS table_name,
    t.create_date,
    t.modify_date
FROM sys.tables AS t
JOIN sys.schemas AS s
    ON s.schema_id = t.schema_id
WHERE s.name = N'ratings'
  AND t.name LIKE N'zz_20260325_bayrate%'
ORDER BY s.name, t.name;
