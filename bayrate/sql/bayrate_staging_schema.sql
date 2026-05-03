IF SCHEMA_ID(N'ratings') IS NULL
    EXEC(N'CREATE SCHEMA [ratings]');

IF OBJECT_ID(N'ratings.bayrate_staged_games', N'U') IS NOT NULL
    PRINT N'ratings.bayrate_staged_games already exists.';

IF OBJECT_ID(N'ratings.bayrate_staged_ratings', N'U') IS NOT NULL
    PRINT N'ratings.bayrate_staged_ratings already exists.';

IF OBJECT_ID(N'ratings.bayrate_staged_tournaments', N'U') IS NOT NULL
    PRINT N'ratings.bayrate_staged_tournaments already exists.';

IF OBJECT_ID(N'ratings.bayrate_runs', N'U') IS NOT NULL
    PRINT N'ratings.bayrate_runs already exists.';

IF OBJECT_ID(N'ratings.bayrate_run_id_seq', N'SO') IS NULL
    EXEC(N'CREATE SEQUENCE [ratings].[bayrate_run_id_seq] AS int START WITH 1 INCREMENT BY 1;');

IF OBJECT_ID(N'ratings.bayrate_admins', N'U') IS NULL
BEGIN
    CREATE TABLE [ratings].[bayrate_admins]
    (
        [AdminID] int IDENTITY(1, 1) NOT NULL,
        [Principal_Name] nvarchar(256) NOT NULL,
        [Principal_Id] nvarchar(128) NULL,
        [Display_Name] nvarchar(256) NULL,
        [Is_Active] bit NOT NULL CONSTRAINT [DF_bayrate_admins_Is_Active] DEFAULT 1,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_bayrate_admins_Created_At] DEFAULT SYSUTCDATETIME(),
        [Created_By] nvarchar(128) NULL CONSTRAINT [DF_bayrate_admins_Created_By] DEFAULT SUSER_SNAME(),
        [Revoked_At] datetime2(0) NULL,
        [Revoked_By] nvarchar(128) NULL,
        CONSTRAINT [PK_bayrate_admins] PRIMARY KEY CLUSTERED ([AdminID]),
        CONSTRAINT [UQ_bayrate_admins_Principal_Name] UNIQUE ([Principal_Name])
    );
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_bayrate_admins_Principal_Id'
      AND [object_id] = OBJECT_ID(N'ratings.bayrate_admins')
)
BEGIN
    CREATE INDEX [IX_bayrate_admins_Principal_Id]
        ON [ratings].[bayrate_admins] ([Principal_Id])
        WHERE [Principal_Id] IS NOT NULL;
END;

IF OBJECT_ID(N'ratings.bayrate_runs', N'U') IS NOT NULL
   AND COL_LENGTH(N'ratings.bayrate_runs', N'Source_Report_Count') IS NULL
BEGIN
    THROW 51000, N'ratings.bayrate_runs already exists but does not look like the BayRate report staging table. Archive or rename the old table before applying this schema.', 1;
END;

IF OBJECT_ID(N'ratings.bayrate_runs', N'U') IS NOT NULL
   AND EXISTS
   (
       SELECT 1
       FROM sys.columns
       WHERE [object_id] = OBJECT_ID(N'ratings.bayrate_runs')
         AND [name] = N'RunID'
         AND system_type_id <> TYPE_ID(N'int')
   )
BEGIN
    THROW 51001, N'ratings.bayrate_runs uses the old GUID RunID schema. Drop/recreate BayRate staging tables before applying the integer RunID schema.', 1;
END;

IF OBJECT_ID(N'ratings.bayrate_runs', N'U') IS NULL
BEGIN
    CREATE TABLE [ratings].[bayrate_runs]
    (
        [RunID] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_RunID] DEFAULT (NEXT VALUE FOR [ratings].[bayrate_run_id_seq]),
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Created_At] DEFAULT SYSUTCDATETIME(),
        [Last_Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Last_Updated_At] DEFAULT SYSUTCDATETIME(),
        [Created_By] nvarchar(128) NULL CONSTRAINT [DF_bayrate_stage_runs_Created_By] DEFAULT SUSER_SNAME(),
        [Status] nvarchar(32) NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Status] DEFAULT N'staged',
        [Source_Report_Count] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Source_Report_Count] DEFAULT 0,
        [Source_Report_Names] nvarchar(max) NULL,
        [Tournament_Count] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Tournament_Count] DEFAULT 0,
        [Game_Count] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Game_Count] DEFAULT 0,
        [Validation_Error_Count] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Validation_Error_Count] DEFAULT 0,
        [Ready_Tournament_Count] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Ready_Tournament_Count] DEFAULT 0,
        [Needs_Review_Count] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Needs_Review_Count] DEFAULT 0,
        [Validation_Failed_Count] int NOT NULL CONSTRAINT [DF_bayrate_stage_runs_Validation_Failed_Count] DEFAULT 0,
        [SummaryJson] nvarchar(max) NULL,
        CONSTRAINT [PK_bayrate_stage_runs] PRIMARY KEY CLUSTERED ([RunID]),
        CONSTRAINT [CK_bayrate_stage_runs_Status] CHECK ([Status] IN (N'staged', N'validation_failed', N'needs_review', N'ready_for_rating'))
    );
END;

IF OBJECT_ID(N'ratings.bayrate_staged_tournaments', N'U') IS NULL
BEGIN
    CREATE TABLE [ratings].[bayrate_staged_tournaments]
    (
        [RunID] int NOT NULL,
        [Source_Report_Ordinal] int NOT NULL,
        [Source_Report_Name] nvarchar(512) NOT NULL,
        [Source_Report_Sha256] char(64) NOT NULL,
        [Tournament_Code] nvarchar(32) NOT NULL,
        [Original_Tournament_Code] nvarchar(32) NULL,
        [Tournament_Code_Source] nvarchar(32) NOT NULL,
        [Tournament_Descr] nvarchar(255) NULL,
        [Normalized_Title] nvarchar(255) NULL,
        [Tournament_Date] date NULL,
        [City] nvarchar(128) NULL,
        [State_Code] nvarchar(16) NULL,
        [Country_Code] nvarchar(16) NULL,
        [Host_ChapterID] int NULL,
        [Host_ChapterCode] nvarchar(20) NULL,
        [Host_ChapterName] nvarchar(200) NULL,
        [Reward_Event_Key] nvarchar(128) NULL,
        [Reward_Event_Name] nvarchar(255) NULL,
        [Reward_Is_State_Championship] bit NOT NULL CONSTRAINT [DF_bayrate_staged_tournaments_Reward_Is_State_Championship] DEFAULT 0,
        [Rounds] int NULL,
        [Total_Players] int NULL,
        [Wallist] nvarchar(255) NULL,
        [Elab_Date] date NULL,
        [Validation_Status] nvarchar(32) NOT NULL CONSTRAINT [DF_bayrate_staged_tournaments_Validation_Status] DEFAULT N'staged',
        [Validation_Errors] nvarchar(max) NULL,
        [Parser_Warnings] nvarchar(max) NULL,
        [Duplicate_Candidate_Code] nvarchar(32) NULL,
        [Duplicate_Score] decimal(5, 4) NULL,
        [Review_Reason] nvarchar(max) NULL,
        [MetadataJson] nvarchar(max) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_bayrate_staged_tournaments_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_bayrate_staged_tournaments] PRIMARY KEY CLUSTERED ([RunID], [Source_Report_Ordinal]),
        CONSTRAINT [FK_bayrate_staged_tournaments_RunID] FOREIGN KEY ([RunID]) REFERENCES [ratings].[bayrate_runs] ([RunID]),
        CONSTRAINT [CK_bayrate_staged_tournaments_Validation_Status] CHECK ([Validation_Status] IN (N'staged', N'validation_failed', N'needs_review', N'ready_for_rating')),
        CONSTRAINT [CK_bayrate_staged_tournaments_Code_Source] CHECK ([Tournament_Code_Source] IN (N'generated', N'reused', N'parser'))
    );
END;

IF OBJECT_ID(N'ratings.bayrate_staged_tournaments', N'U') IS NOT NULL
   AND COL_LENGTH(N'ratings.bayrate_staged_tournaments', N'Host_ChapterID') IS NULL
BEGIN
    ALTER TABLE [ratings].[bayrate_staged_tournaments]
        ADD [Host_ChapterID] int NULL;
END;

IF OBJECT_ID(N'ratings.bayrate_staged_tournaments', N'U') IS NOT NULL
   AND COL_LENGTH(N'ratings.bayrate_staged_tournaments', N'Host_ChapterCode') IS NULL
BEGIN
    ALTER TABLE [ratings].[bayrate_staged_tournaments]
        ADD [Host_ChapterCode] nvarchar(20) NULL;
END;

IF OBJECT_ID(N'ratings.bayrate_staged_tournaments', N'U') IS NOT NULL
   AND COL_LENGTH(N'ratings.bayrate_staged_tournaments', N'Host_ChapterName') IS NULL
BEGIN
    ALTER TABLE [ratings].[bayrate_staged_tournaments]
        ADD [Host_ChapterName] nvarchar(200) NULL;
END;

IF OBJECT_ID(N'ratings.bayrate_staged_tournaments', N'U') IS NOT NULL
   AND COL_LENGTH(N'ratings.bayrate_staged_tournaments', N'Reward_Event_Key') IS NULL
BEGIN
    ALTER TABLE [ratings].[bayrate_staged_tournaments]
        ADD [Reward_Event_Key] nvarchar(128) NULL;
END;

IF OBJECT_ID(N'ratings.bayrate_staged_tournaments', N'U') IS NOT NULL
   AND COL_LENGTH(N'ratings.bayrate_staged_tournaments', N'Reward_Event_Name') IS NULL
BEGIN
    ALTER TABLE [ratings].[bayrate_staged_tournaments]
        ADD [Reward_Event_Name] nvarchar(255) NULL;
END;

IF OBJECT_ID(N'ratings.bayrate_staged_tournaments', N'U') IS NOT NULL
   AND COL_LENGTH(N'ratings.bayrate_staged_tournaments', N'Reward_Is_State_Championship') IS NULL
BEGIN
    ALTER TABLE [ratings].[bayrate_staged_tournaments]
        ADD [Reward_Is_State_Championship] bit NOT NULL
            CONSTRAINT [DF_bayrate_staged_tournaments_Reward_Is_State_Championship] DEFAULT 0;
END;

IF OBJECT_ID(N'ratings.tournaments', N'U') IS NOT NULL
   AND COL_LENGTH(N'ratings.tournaments', N'Host_ChapterID') IS NULL
BEGIN
    ALTER TABLE [ratings].[tournaments]
        ADD [Host_ChapterID] int NULL;
END;

IF OBJECT_ID(N'ratings.tournaments', N'U') IS NOT NULL
   AND COL_LENGTH(N'ratings.tournaments', N'Host_ChapterCode') IS NULL
BEGIN
    ALTER TABLE [ratings].[tournaments]
        ADD [Host_ChapterCode] nvarchar(20) NULL;
END;

IF OBJECT_ID(N'ratings.tournaments', N'U') IS NOT NULL
   AND COL_LENGTH(N'ratings.tournaments', N'Host_ChapterName') IS NULL
BEGIN
    ALTER TABLE [ratings].[tournaments]
        ADD [Host_ChapterName] nvarchar(200) NULL;
END;

IF OBJECT_ID(N'ratings.tournaments', N'U') IS NOT NULL
   AND COL_LENGTH(N'ratings.tournaments', N'Reward_Event_Key') IS NULL
BEGIN
    ALTER TABLE [ratings].[tournaments]
        ADD [Reward_Event_Key] nvarchar(128) NULL;
END;

IF OBJECT_ID(N'ratings.tournaments', N'U') IS NOT NULL
   AND COL_LENGTH(N'ratings.tournaments', N'Reward_Event_Name') IS NULL
BEGIN
    ALTER TABLE [ratings].[tournaments]
        ADD [Reward_Event_Name] nvarchar(255) NULL;
END;

IF OBJECT_ID(N'ratings.tournaments', N'U') IS NOT NULL
   AND COL_LENGTH(N'ratings.tournaments', N'Reward_Is_State_Championship') IS NULL
BEGIN
    ALTER TABLE [ratings].[tournaments]
        ADD [Reward_Is_State_Championship] bit NOT NULL
            CONSTRAINT [DF_tournaments_Reward_Is_State_Championship] DEFAULT 0;
END;

IF OBJECT_ID(N'ratings.bayrate_staged_games', N'U') IS NULL
BEGIN
    CREATE TABLE [ratings].[bayrate_staged_games]
    (
        [RunID] int NOT NULL,
        [Source_Report_Ordinal] int NOT NULL,
        [Source_Game_Ordinal] int NOT NULL,
        [Source_Report_Name] nvarchar(512) NOT NULL,
        [Game_ID] int NULL,
        [Tournament_Code] nvarchar(32) NULL,
        [Game_Date] date NULL,
        [Round] int NULL,
        [Pin_Player_1] int NULL,
        [Color_1] nvarchar(1) NULL,
        [Rank_1] nvarchar(16) NULL,
        [Pin_Player_2] int NULL,
        [Color_2] nvarchar(1) NULL,
        [Rank_2] nvarchar(16) NULL,
        [Handicap] int NULL,
        [Komi] decimal(5, 1) NULL,
        [Result] nvarchar(8) NULL,
        [Sgf_Code] nvarchar(128) NULL,
        [Online] bit NULL,
        [Exclude] bit NULL,
        [Rated] bit NULL,
        [Elab_Date] date NULL,
        [Validation_Status] nvarchar(32) NOT NULL CONSTRAINT [DF_bayrate_staged_games_Validation_Status] DEFAULT N'staged',
        [Validation_Errors] nvarchar(max) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_bayrate_staged_games_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_bayrate_staged_games] PRIMARY KEY CLUSTERED ([RunID], [Source_Report_Ordinal], [Source_Game_Ordinal]),
        CONSTRAINT [FK_bayrate_staged_games_Tournament] FOREIGN KEY ([RunID], [Source_Report_Ordinal])
            REFERENCES [ratings].[bayrate_staged_tournaments] ([RunID], [Source_Report_Ordinal]),
        CONSTRAINT [CK_bayrate_staged_games_Validation_Status] CHECK ([Validation_Status] IN (N'staged', N'validation_failed', N'needs_review', N'ready_for_rating'))
    );
END;

IF OBJECT_ID(N'ratings.bayrate_staged_ratings', N'U') IS NULL
BEGIN
    CREATE TABLE [ratings].[bayrate_staged_ratings]
    (
        [RunID] int NOT NULL,
        [Event_Ordinal] int NOT NULL,
        [Player_Ordinal] int NOT NULL,
        [Event_Source] nvarchar(32) NOT NULL,
        [Event_Key] nvarchar(128) NULL,
        [Tournament_Code] nvarchar(32) NULL,
        [Staged_Tournament_Code] nvarchar(32) NULL,
        [Replaced_Production_Code] nvarchar(32) NULL,
        [Source_Report_Ordinal] int NULL,
        [Pin_Player] int NOT NULL,
        [Rating] float NULL,
        [Sigma] float NULL,
        [Elab_Date] date NULL,
        [Rank_Seed] float NULL,
        [Seed_Before_Closing_Boundary] float NULL,
        [Prior_Rating] float NULL,
        [Prior_Sigma] float NULL,
        [Planned_Rating_Row_ID] int NULL,
        [Production_Rating_Row_ID] int NULL,
        [Rating_Delta] float NULL,
        [Sigma_Delta] float NULL,
        [MetadataJson] nvarchar(max) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_bayrate_staged_ratings_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_bayrate_staged_ratings] PRIMARY KEY CLUSTERED ([RunID], [Event_Ordinal], [Pin_Player]),
        CONSTRAINT [FK_bayrate_staged_ratings_RunID] FOREIGN KEY ([RunID]) REFERENCES [ratings].[bayrate_runs] ([RunID]),
        CONSTRAINT [CK_bayrate_staged_ratings_Event_Source] CHECK ([Event_Source] IN (N'staged', N'production'))
    );
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_bayrate_staged_tournaments_Status'
      AND [object_id] = OBJECT_ID(N'ratings.bayrate_staged_tournaments')
)
BEGIN
    CREATE INDEX [IX_bayrate_staged_tournaments_Status]
        ON [ratings].[bayrate_staged_tournaments] ([Validation_Status], [Tournament_Date], [Tournament_Code]);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_bayrate_staged_tournaments_Host_Chapter'
      AND [object_id] = OBJECT_ID(N'ratings.bayrate_staged_tournaments')
)
BEGIN
    CREATE INDEX [IX_bayrate_staged_tournaments_Host_Chapter]
        ON [ratings].[bayrate_staged_tournaments] ([Host_ChapterID], [Reward_Event_Key], [Tournament_Date])
        WHERE [Host_ChapterID] IS NOT NULL;
END;

IF OBJECT_ID(N'ratings.tournaments', N'U') IS NOT NULL
   AND NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_tournaments_Host_Chapter'
      AND [object_id] = OBJECT_ID(N'ratings.tournaments')
)
BEGIN
    CREATE INDEX [IX_tournaments_Host_Chapter]
        ON [ratings].[tournaments] ([Host_ChapterID], [Reward_Event_Key], [Tournament_Date])
        WHERE [Host_ChapterID] IS NOT NULL;
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_bayrate_staged_games_Tournament'
      AND [object_id] = OBJECT_ID(N'ratings.bayrate_staged_games')
)
BEGIN
    CREATE INDEX [IX_bayrate_staged_games_Tournament]
        ON [ratings].[bayrate_staged_games] ([RunID], [Tournament_Code], [Game_Date], [Round]);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_bayrate_staged_ratings_Tournament'
      AND [object_id] = OBJECT_ID(N'ratings.bayrate_staged_ratings')
)
BEGIN
    CREATE INDEX [IX_bayrate_staged_ratings_Tournament]
        ON [ratings].[bayrate_staged_ratings] ([RunID], [Tournament_Code], [Event_Ordinal]);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_bayrate_staged_ratings_Player'
      AND [object_id] = OBJECT_ID(N'ratings.bayrate_staged_ratings')
)
BEGIN
    CREATE INDEX [IX_bayrate_staged_ratings_Player]
        ON [ratings].[bayrate_staged_ratings] ([RunID], [Pin_Player], [Event_Ordinal]);
END;
