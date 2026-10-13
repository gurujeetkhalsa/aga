IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'ratings')
BEGIN
    EXEC(N'CREATE SCHEMA [ratings]');
END;
GO

IF OBJECT_ID(N'[ratings].[bayrate_runs]', N'U') IS NULL
BEGIN
    CREATE TABLE [ratings].[bayrate_runs]
    (
        [RunID] BIGINT IDENTITY(1,1) NOT NULL,
        [ModelLabel] NVARCHAR(200) NOT NULL,
        [ConfigHash] VARBINARY(32) NOT NULL,
        [SourceVersion] NVARCHAR(200) NULL,
        [Status] NVARCHAR(30) NOT NULL,
        [ReplayFromEventOrdinal] INT NOT NULL,
        [ReplayFromEventKey] NVARCHAR(300) NOT NULL,
        [ReplayFromEventDate] DATE NOT NULL,
        [CompletedThroughEventOrdinal] INT NULL,
        [CompletedThroughEventKey] NVARCHAR(300) NULL,
        [CompletedThroughEventDate] DATE NULL,
        [EventCount] INT NULL,
        [PlayerCount] INT NULL,
        [GameCount] INT NULL,
        [ErrorMessage] NVARCHAR(MAX) NULL,
        [StartedAt] DATETIME2(7) NOT NULL CONSTRAINT [DF_bayrate_runs_StartedAt] DEFAULT (SYSDATETIME()),
        [CompletedAt] DATETIME2(7) NULL,
        [CreatedDate] DATETIME2(7) NOT NULL CONSTRAINT [DF_bayrate_runs_CreatedDate] DEFAULT (SYSDATETIME()),
        [LastUpdated] DATETIME2(7) NOT NULL CONSTRAINT [DF_bayrate_runs_LastUpdated] DEFAULT (SYSDATETIME()),
        CONSTRAINT [PK_bayrate_runs] PRIMARY KEY CLUSTERED ([RunID] ASC)
    );
END;
GO

IF OBJECT_ID(N'[ratings].[bayrate_event_index]', N'U') IS NULL
BEGIN
    CREATE TABLE [ratings].[bayrate_event_index]
    (
        [ModelLabel] NVARCHAR(200) NOT NULL,
        [EventOrdinal] INT NOT NULL,
        [EventKey] NVARCHAR(300) NOT NULL,
        [EventDate] DATE NOT NULL,
        [Tournament_Code] NVARCHAR(255) NULL,
        [SourceHash] VARBINARY(32) NOT NULL,
        [Dirty] BIT NOT NULL CONSTRAINT [DF_bayrate_event_index_Dirty] DEFAULT (0),
        [DirtyReason] NVARCHAR(200) NULL,
        [LastComputedRunID] BIGINT NULL,
        [LastComputedAt] DATETIME2(7) NULL,
        [CreatedDate] DATETIME2(7) NOT NULL CONSTRAINT [DF_bayrate_event_index_CreatedDate] DEFAULT (SYSDATETIME()),
        [LastUpdated] DATETIME2(7) NOT NULL CONSTRAINT [DF_bayrate_event_index_LastUpdated] DEFAULT (SYSDATETIME()),
        CONSTRAINT [PK_bayrate_event_index] PRIMARY KEY CLUSTERED ([ModelLabel] ASC, [EventOrdinal] ASC),
        CONSTRAINT [UX_bayrate_event_index_Model_EventKey] UNIQUE NONCLUSTERED ([ModelLabel] ASC, [EventKey] ASC)
    );
END;
GO

IF OBJECT_ID(N'[ratings].[bayrate_event_player_ratings]', N'U') IS NULL
BEGIN
    CREATE TABLE [ratings].[bayrate_event_player_ratings]
    (
        [ModelLabel] NVARCHAR(200) NOT NULL,
        [RunID] BIGINT NOT NULL,
        [EventOrdinal] INT NOT NULL,
        [EventKey] NVARCHAR(300) NOT NULL,
        [EventDate] DATE NOT NULL,
        [Tournament_Code] NVARCHAR(255) NULL,
        [AGAID] INT NOT NULL,
        [RankSeed] FLOAT NOT NULL,
        [SeedBefore] FLOAT NOT NULL,
        [PriorRating] FLOAT NULL,
        [PriorSigma] FLOAT NULL,
        [RatingAfter] FLOAT NOT NULL,
        [SigmaAfter] FLOAT NOT NULL,
        [CreatedDate] DATETIME2(7) NOT NULL CONSTRAINT [DF_bayrate_event_player_ratings_CreatedDate] DEFAULT (SYSDATETIME()),
        CONSTRAINT [PK_bayrate_event_player_ratings] PRIMARY KEY CLUSTERED ([ModelLabel] ASC, [AGAID] ASC, [EventOrdinal] ASC),
        CONSTRAINT [FK_bayrate_event_player_ratings_RunID] FOREIGN KEY ([RunID]) REFERENCES [ratings].[bayrate_runs] ([RunID])
    );
END;
GO

IF OBJECT_ID(N'[ratings].[bayrate_event_game_results]', N'U') IS NULL
BEGIN
    CREATE TABLE [ratings].[bayrate_event_game_results]
    (
        [ModelLabel] NVARCHAR(200) NOT NULL,
        [RunID] BIGINT NOT NULL,
        [EventOrdinal] INT NOT NULL,
        [EventKey] NVARCHAR(300) NOT NULL,
        [EventDate] DATE NOT NULL,
        [Tournament_Code] NVARCHAR(255) NULL,
        [SourceGameID] INT NOT NULL,
        [WhiteAGAID] INT NOT NULL,
        [BlackAGAID] INT NOT NULL,
        [Handicap] INT NOT NULL,
        [Komi] FLOAT NOT NULL,
        [WhiteWins] BIT NOT NULL,
        [WhiteSeedBefore] FLOAT NOT NULL,
        [BlackSeedBefore] FLOAT NOT NULL,
        [PreEventExpectedWhite] FLOAT NOT NULL,
        [PostEventExpectedWhite] FLOAT NOT NULL,
        [CreatedDate] DATETIME2(7) NOT NULL CONSTRAINT [DF_bayrate_event_game_results_CreatedDate] DEFAULT (SYSDATETIME()),
        CONSTRAINT [PK_bayrate_event_game_results] PRIMARY KEY CLUSTERED ([ModelLabel] ASC, [SourceGameID] ASC),
        CONSTRAINT [FK_bayrate_event_game_results_RunID] FOREIGN KEY ([RunID]) REFERENCES [ratings].[bayrate_runs] ([RunID])
    );
END;
GO
IF OBJECT_ID(N'[ratings].[bayrate_player_checkpoint]', N'U') IS NULL
BEGIN
    CREATE TABLE [ratings].[bayrate_player_checkpoint]
    (
        [ModelLabel] NVARCHAR(200) NOT NULL,
        [AGAID] INT NOT NULL,
        [EventOrdinal] INT NOT NULL,
        [EventKey] NVARCHAR(300) NOT NULL,
        [EventDate] DATE NOT NULL,
        [Rating] FLOAT NOT NULL,
        [Sigma] FLOAT NOT NULL,
        [LastRatingDate] DATE NOT NULL,
        [MomentumStreak] INT NOT NULL CONSTRAINT [DF_bayrate_player_checkpoint_MomentumStreak] DEFAULT (0),
        [RunID] BIGINT NOT NULL,
        [CreatedDate] DATETIME2(7) NOT NULL CONSTRAINT [DF_bayrate_player_checkpoint_CreatedDate] DEFAULT (SYSDATETIME()),
        CONSTRAINT [PK_bayrate_player_checkpoint] PRIMARY KEY CLUSTERED ([ModelLabel] ASC, [AGAID] ASC, [EventOrdinal] ASC),
        CONSTRAINT [FK_bayrate_player_checkpoint_RunID] FOREIGN KEY ([RunID]) REFERENCES [ratings].[bayrate_runs] ([RunID])
    );
END;
GO

IF OBJECT_ID(N'[ratings].[bayrate_run_metrics]', N'U') IS NULL
BEGIN
    CREATE TABLE [ratings].[bayrate_run_metrics]
    (
        [RunID] BIGINT NOT NULL,
        [MetricName] NVARCHAR(100) NOT NULL,
        [MetricValue] FLOAT NOT NULL,
        [MetricText] NVARCHAR(400) NULL,
        [CreatedDate] DATETIME2(7) NOT NULL CONSTRAINT [DF_bayrate_run_metrics_CreatedDate] DEFAULT (SYSDATETIME()),
        CONSTRAINT [PK_bayrate_run_metrics] PRIMARY KEY CLUSTERED ([RunID] ASC, [MetricName] ASC),
        CONSTRAINT [FK_bayrate_run_metrics_RunID] FOREIGN KEY ([RunID]) REFERENCES [ratings].[bayrate_runs] ([RunID])
    );
END;
GO

IF OBJECT_ID(N'[ratings].[bayrate_control]', N'U') IS NULL
BEGIN
    CREATE TABLE [ratings].[bayrate_control]
    (
        [ModelLabel] NVARCHAR(200) NOT NULL,
        [CutoverEventOrdinal] INT NULL,
        [SnapshotSeedEnabled] BIT NOT NULL CONSTRAINT [DF_bayrate_control_SnapshotSeedEnabled] DEFAULT (0),
        [FreezeBeforeCutover] BIT NOT NULL CONSTRAINT [DF_bayrate_control_FreezeBeforeCutover] DEFAULT (0),
        [CreatedDate] DATETIME2(7) NOT NULL CONSTRAINT [DF_bayrate_control_CreatedDate] DEFAULT (SYSDATETIME()),
        [LastUpdated] DATETIME2(7) NOT NULL CONSTRAINT [DF_bayrate_control_LastUpdated] DEFAULT (SYSDATETIME()),
        CONSTRAINT [PK_bayrate_control] PRIMARY KEY CLUSTERED ([ModelLabel] ASC)
    );
END;
GO

CREATE OR ALTER VIEW [ratings].[vw_bayrate_games_source]
AS
SELECT g.[Game_ID], g.[Tournament_Code], CAST(g.[Game_Date] AS date) AS [Game_Date], g.[Round], g.[Pin_Player_1], g.[Color_1], g.[Rank_1],
       g.[Pin_Player_2], g.[Color_2], g.[Rank_2], g.[Handicap], g.[Komi], g.[Result], g.[Online], g.[Exclude], g.[Rated]
FROM [ratings].[games] AS g;
GO

CREATE OR ALTER VIEW [ratings].[vw_bayrate_snapshots_source]
AS
SELECT r.[Pin_Player] AS [Player_ID], r.[Rating], r.[Sigma], CAST(r.[Elab_Date] AS date) AS [Elab_Date], r.[Tournament_Code], r.[id] AS [Row_ID]
FROM [ratings].[ratings] AS r;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_refresh_event_index]
    @ModelLabel NVARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;
    WITH event_rows AS
    (
        SELECT
            CASE WHEN NULLIF(LTRIM(RTRIM(g.[Tournament_Code])), N'') IS NOT NULL THEN CONCAT(N'code:', LTRIM(RTRIM(g.[Tournament_Code])))
                 ELSE CONCAT(N'date:', CONVERT(nvarchar(10), CAST(g.[Game_Date] AS date), 23)) END AS [EventKey],
            CAST(g.[Game_Date] AS date) AS [EventDate],
            NULLIF(LTRIM(RTRIM(g.[Tournament_Code])), N'') AS [Tournament_Code],
            HASHBYTES('SHA2_256', STRING_AGG(CONCAT_WS(N'|', CONVERT(nvarchar(30), g.[Game_ID]), CONVERT(nvarchar(10), CAST(g.[Game_Date] AS date), 23), COALESCE(g.[Tournament_Code], N''), COALESCE(CONVERT(nvarchar(30), g.[Round]), N''), CONVERT(nvarchar(30), g.[Pin_Player_1]), COALESCE(g.[Color_1], N''), COALESCE(g.[Rank_1], N''), CONVERT(nvarchar(30), g.[Pin_Player_2]), COALESCE(g.[Color_2], N''), COALESCE(g.[Rank_2], N''), COALESCE(CONVERT(nvarchar(30), g.[Handicap]), N''), COALESCE(CONVERT(nvarchar(50), g.[Komi]), N''), COALESCE(g.[Result], N''), COALESCE(CONVERT(nvarchar(10), g.[Online]), N''), COALESCE(CONVERT(nvarchar(10), g.[Exclude]), N''), COALESCE(CONVERT(nvarchar(10), g.[Rated]), N'')), N';') WITHIN GROUP (ORDER BY g.[Game_Date], g.[Tournament_Code], g.[Round], g.[Game_ID])) AS [SourceHash]
        FROM [ratings].[vw_bayrate_games_source] AS g
        GROUP BY CASE WHEN NULLIF(LTRIM(RTRIM(g.[Tournament_Code])), N'') IS NOT NULL THEN CONCAT(N'code:', LTRIM(RTRIM(g.[Tournament_Code]))) ELSE CONCAT(N'date:', CONVERT(nvarchar(10), CAST(g.[Game_Date] AS date), 23)) END,
                 CAST(g.[Game_Date] AS date), NULLIF(LTRIM(RTRIM(g.[Tournament_Code])), N'')
    ),
    numbered AS
    (
        SELECT @ModelLabel AS [ModelLabel], ROW_NUMBER() OVER (ORDER BY [EventDate], COALESCE([Tournament_Code], N''), [EventKey]) AS [EventOrdinal], [EventKey], [EventDate], [Tournament_Code], [SourceHash]
        FROM event_rows
    )
    MERGE [ratings].[bayrate_event_index] AS tgt
    USING numbered AS src
      ON tgt.[ModelLabel] = src.[ModelLabel]
     AND tgt.[EventKey] = src.[EventKey]
    WHEN MATCHED THEN
        UPDATE SET
            tgt.[EventOrdinal] = src.[EventOrdinal],
            tgt.[EventDate] = src.[EventDate],
            tgt.[Tournament_Code] = src.[Tournament_Code],
            tgt.[SourceHash] = src.[SourceHash],
            tgt.[Dirty] = CASE WHEN tgt.[SourceHash] <> src.[SourceHash] THEN 1 ELSE tgt.[Dirty] END,
            tgt.[DirtyReason] = CASE WHEN tgt.[SourceHash] <> src.[SourceHash] THEN N'source_hash_changed' ELSE tgt.[DirtyReason] END,
            tgt.[LastUpdated] = SYSDATETIME()
    WHEN NOT MATCHED THEN
        INSERT ([ModelLabel], [EventOrdinal], [EventKey], [EventDate], [Tournament_Code], [SourceHash], [Dirty], [DirtyReason])
        VALUES (src.[ModelLabel], src.[EventOrdinal], src.[EventKey], src.[EventDate], src.[Tournament_Code], src.[SourceHash], 1, N'new_event');
END;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_mark_dirty_from_event]
    @ModelLabel NVARCHAR(200),
    @EventKey NVARCHAR(300),
    @Reason NVARCHAR(200) = N'manual'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @EventOrdinal INT;
    SELECT @EventOrdinal = [EventOrdinal] FROM [ratings].[bayrate_event_index] WHERE [ModelLabel] = @ModelLabel AND [EventKey] = @EventKey;
    IF @EventOrdinal IS NULL THROW 50001, 'EventKey not found for model label.', 1;
    UPDATE [ratings].[bayrate_event_index]
    SET [Dirty] = 1, [DirtyReason] = @Reason, [LastUpdated] = SYSDATETIME()
    WHERE [ModelLabel] = @ModelLabel AND [EventOrdinal] >= @EventOrdinal;
END;
GO
CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_mark_dirty_from_tournament]
    @ModelLabel NVARCHAR(200),
    @TournamentCode NVARCHAR(255),
    @Reason NVARCHAR(200) = N'tournament_changed'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @EventKey NVARCHAR(300) = CONCAT(N'code:', @TournamentCode);
    EXEC [ratings].[sp_bayrate_mark_dirty_from_event]
        @ModelLabel = @ModelLabel,
        @EventKey = @EventKey,
        @Reason = @Reason;
END;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_get_replay_boundary]
    @ModelLabel NVARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT TOP (1) [EventOrdinal], [EventKey], [EventDate], [Tournament_Code]
    FROM [ratings].[bayrate_event_index]
    WHERE [ModelLabel] = @ModelLabel AND [Dirty] = 1
    ORDER BY [EventOrdinal];
END;
GO


CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_get_cutover_control]
    @ModelLabel NVARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT c.[ModelLabel], c.[CutoverEventOrdinal], ei.[EventKey] AS [CutoverEventKey], ei.[EventDate] AS [CutoverEventDate],
           c.[SnapshotSeedEnabled], c.[FreezeBeforeCutover]
    FROM [ratings].[bayrate_control] AS c
    LEFT JOIN [ratings].[bayrate_event_index] AS ei
      ON ei.[ModelLabel] = c.[ModelLabel]
     AND ei.[EventOrdinal] = c.[CutoverEventOrdinal]
    WHERE c.[ModelLabel] = @ModelLabel;
END;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_set_cutover_control]
    @ModelLabel NVARCHAR(200),
    @CutoverEventOrdinal INT,
    @SnapshotSeedEnabled BIT = 1,
    @FreezeBeforeCutover BIT = 1,
    @MarkPreCutoverClean BIT = 1
AS
BEGIN
    SET NOCOUNT ON;

    MERGE [ratings].[bayrate_control] AS tgt
    USING (
        SELECT @ModelLabel AS [ModelLabel],
               @CutoverEventOrdinal AS [CutoverEventOrdinal],
               @SnapshotSeedEnabled AS [SnapshotSeedEnabled],
               @FreezeBeforeCutover AS [FreezeBeforeCutover]
    ) AS src
      ON tgt.[ModelLabel] = src.[ModelLabel]
    WHEN MATCHED THEN
        UPDATE SET [CutoverEventOrdinal] = src.[CutoverEventOrdinal],
                   [SnapshotSeedEnabled] = src.[SnapshotSeedEnabled],
                   [FreezeBeforeCutover] = src.[FreezeBeforeCutover],
                   [LastUpdated] = SYSDATETIME()
    WHEN NOT MATCHED THEN
        INSERT ([ModelLabel], [CutoverEventOrdinal], [SnapshotSeedEnabled], [FreezeBeforeCutover])
        VALUES (src.[ModelLabel], src.[CutoverEventOrdinal], src.[SnapshotSeedEnabled], src.[FreezeBeforeCutover]);

    IF @MarkPreCutoverClean = 1
    BEGIN
        UPDATE [ratings].[bayrate_event_index]
        SET [Dirty] = 0,
            [DirtyReason] = NULL,
            [LastUpdated] = SYSDATETIME()
        WHERE [ModelLabel] = @ModelLabel
          AND [EventOrdinal] < @CutoverEventOrdinal;
    END;
END;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_get_snapshot_seed_before_event]
    @ModelLabel NVARCHAR(200),
    @EventOrdinal INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @EventDate DATE;
    SELECT @EventDate = [EventDate]
    FROM [ratings].[bayrate_event_index]
    WHERE [ModelLabel] = @ModelLabel AND [EventOrdinal] = @EventOrdinal;

    IF @EventDate IS NULL
        THROW 50002, 'EventOrdinal not found for model label.', 1;

    WITH ranked AS
    (
        SELECT r.[Pin_Player] AS [AGAID],
               r.[Rating],
               r.[Sigma],
               r.[Elab_Date] AS [LastRatingDate],
               ROW_NUMBER() OVER (PARTITION BY r.[Pin_Player] ORDER BY r.[Elab_Date] DESC, r.[id] DESC) AS rn
        FROM [ratings].[ratings] AS r
        WHERE r.[Elab_Date] < @EventDate
    )
    SELECT [AGAID], [Rating], [Sigma], [LastRatingDate]
    FROM ranked
    WHERE rn = 1
    ORDER BY [AGAID];
END;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_get_checkpoint_before_event]
    @ModelLabel NVARCHAR(200),
    @EventOrdinal INT
AS
BEGIN
    SET NOCOUNT ON;
    WITH ranked AS
    (
        SELECT c.[AGAID], c.[EventOrdinal], c.[EventKey], c.[EventDate], c.[Rating], c.[Sigma], c.[LastRatingDate], c.[MomentumStreak], ROW_NUMBER() OVER (PARTITION BY c.[AGAID] ORDER BY c.[EventOrdinal] DESC) AS rn
        FROM [ratings].[bayrate_player_checkpoint] AS c
        WHERE c.[ModelLabel] = @ModelLabel AND c.[EventOrdinal] < @EventOrdinal
    )
    SELECT [AGAID], [EventOrdinal], [EventKey], [EventDate], [Rating], [Sigma], [LastRatingDate], [MomentumStreak]
    FROM ranked
    WHERE rn = 1
    ORDER BY [AGAID];
END;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_purge_from_event]
    @ModelLabel NVARCHAR(200),
    @EventOrdinal INT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRAN;
    DELETE FROM [ratings].[bayrate_event_game_results] WHERE [ModelLabel] = @ModelLabel AND [EventOrdinal] >= @EventOrdinal;
    DELETE FROM [ratings].[bayrate_event_player_ratings] WHERE [ModelLabel] = @ModelLabel AND [EventOrdinal] >= @EventOrdinal;
    DELETE FROM [ratings].[bayrate_player_checkpoint] WHERE [ModelLabel] = @ModelLabel AND [EventOrdinal] >= @EventOrdinal;
    UPDATE [ratings].[bayrate_event_index]
    SET [LastComputedRunID] = NULL, [LastComputedAt] = NULL, [LastUpdated] = SYSDATETIME()
    WHERE [ModelLabel] = @ModelLabel AND [EventOrdinal] >= @EventOrdinal;
    COMMIT;
END;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_mark_event_computed]
    @ModelLabel NVARCHAR(200),
    @EventOrdinal INT,
    @RunID BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [ratings].[bayrate_event_index]
    SET [Dirty] = 0, [DirtyReason] = NULL, [LastComputedRunID] = @RunID, [LastComputedAt] = SYSDATETIME(), [LastUpdated] = SYSDATETIME()
    WHERE [ModelLabel] = @ModelLabel AND [EventOrdinal] = @EventOrdinal;
    UPDATE r
    SET [CompletedThroughEventOrdinal] = ei.[EventOrdinal], [CompletedThroughEventKey] = ei.[EventKey], [CompletedThroughEventDate] = ei.[EventDate], [LastUpdated] = SYSDATETIME()
    FROM [ratings].[bayrate_runs] AS r
    JOIN [ratings].[bayrate_event_index] AS ei ON ei.[ModelLabel] = r.[ModelLabel] AND ei.[EventOrdinal] = @EventOrdinal
    WHERE r.[RunID] = @RunID;
END;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_start_run]
    @ModelLabel NVARCHAR(200),
    @ConfigHash VARBINARY(32),
    @SourceVersion NVARCHAR(200) = NULL,
    @ReplayFromEventOrdinal INT,
    @ReplayFromEventKey NVARCHAR(300),
    @ReplayFromEventDate DATE
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [ratings].[bayrate_runs]
    ([ModelLabel], [ConfigHash], [SourceVersion], [Status], [ReplayFromEventOrdinal], [ReplayFromEventKey], [ReplayFromEventDate])
    VALUES (@ModelLabel, @ConfigHash, @SourceVersion, N'running', @ReplayFromEventOrdinal, @ReplayFromEventKey, @ReplayFromEventDate);
    SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [RunID];
END;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_complete_run]
    @RunID BIGINT,
    @EventCount INT,
    @PlayerCount INT,
    @GameCount INT
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [ratings].[bayrate_runs]
    SET [Status] = N'completed', [EventCount] = @EventCount, [PlayerCount] = @PlayerCount, [GameCount] = @GameCount, [CompletedAt] = SYSDATETIME(), [LastUpdated] = SYSDATETIME()
    WHERE [RunID] = @RunID;
END;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_fail_run]
    @RunID BIGINT,
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [ratings].[bayrate_runs]
    SET [Status] = N'failed', [ErrorMessage] = @ErrorMessage, [CompletedAt] = SYSDATETIME(), [LastUpdated] = SYSDATETIME()
    WHERE [RunID] = @RunID;
END;
GO
