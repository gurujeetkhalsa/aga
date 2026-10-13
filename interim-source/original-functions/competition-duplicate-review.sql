IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'competition')
BEGIN
    EXEC(N'CREATE SCHEMA [competition]');
END;
GO

IF OBJECT_ID(N'[competition].[tournament_duplicate_review]', N'U') IS NULL
BEGIN
    CREATE TABLE [competition].[tournament_duplicate_review]
    (
        [ReviewID] BIGINT IDENTITY(1,1) NOT NULL,
        [ReviewType] NVARCHAR(50) NOT NULL,
        [ReviewKey] NVARCHAR(400) NOT NULL,
        [CodeBase] NVARCHAR(255) NULL,
        [NormalizedName] NVARCHAR(400) NULL,
        [TournamentDate] DATE NULL,
        [VariantCount] INT NOT NULL,
        [TournamentCodes] NVARCHAR(MAX) NOT NULL,
        [TournamentDescriptions] NVARCHAR(MAX) NULL,
        [Locations] NVARCHAR(MAX) NULL,
        [Status] NVARCHAR(30) NOT NULL
            CONSTRAINT [DF_tournament_duplicate_review_Status] DEFAULT (N'pending'),
        [ReviewerNote] NVARCHAR(MAX) NULL,
        [CanonicalTournamentCode] NVARCHAR(255) NULL,
        [CreatedDate] DATETIME2(7) NOT NULL
            CONSTRAINT [DF_tournament_duplicate_review_CreatedDate] DEFAULT (SYSDATETIME()),
        [LastRefreshed] DATETIME2(7) NOT NULL
            CONSTRAINT [DF_tournament_duplicate_review_LastRefreshed] DEFAULT (SYSDATETIME()),
        CONSTRAINT [PK_tournament_duplicate_review] PRIMARY KEY CLUSTERED ([ReviewID] ASC),
        CONSTRAINT [UX_tournament_duplicate_review_ReviewType_ReviewKey]
            UNIQUE ([ReviewType], [ReviewKey])
    );

    CREATE NONCLUSTERED INDEX [IX_tournament_duplicate_review_Status]
        ON [competition].[tournament_duplicate_review] ([Status] ASC, [TournamentDate] ASC);
END;
GO

IF OBJECT_ID(N'[competition].[game_duplicate_review]', N'U') IS NULL
BEGIN
    CREATE TABLE [competition].[game_duplicate_review]
    (
        [ReviewID] BIGINT IDENTITY(1,1) NOT NULL,
        [ReviewType] NVARCHAR(50) NOT NULL,
        [ReviewKey] NVARCHAR(400) NOT NULL,
        [GameDate] DATE NOT NULL,
        [TournamentCode] NVARCHAR(255) NULL,
        [PlayerLowAGAID] INT NULL,
        [PlayerHighAGAID] INT NULL,
        [Pin_Player_1] INT NULL,
        [Color_1] NVARCHAR(20) NULL,
        [Pin_Player_2] INT NULL,
        [Color_2] NVARCHAR(20) NULL,
        [Handicap] INT NULL,
        [Komi] INT NULL,
        [Result] NVARCHAR(20) NULL,
        [DuplicateCount] INT NOT NULL,
        [ExcludedCount] INT NOT NULL,
        [MinGameID] INT NULL,
        [MaxGameID] INT NULL,
        [Status] NVARCHAR(30) NOT NULL
            CONSTRAINT [DF_game_duplicate_review_Status] DEFAULT (N'pending'),
        [ReviewerNote] NVARCHAR(MAX) NULL,
        [CanonicalGameID] INT NULL,
        [CreatedDate] DATETIME2(7) NOT NULL
            CONSTRAINT [DF_game_duplicate_review_CreatedDate] DEFAULT (SYSDATETIME()),
        [LastRefreshed] DATETIME2(7) NOT NULL
            CONSTRAINT [DF_game_duplicate_review_LastRefreshed] DEFAULT (SYSDATETIME()),
        CONSTRAINT [PK_game_duplicate_review] PRIMARY KEY CLUSTERED ([ReviewID] ASC),
        CONSTRAINT [UX_game_duplicate_review_ReviewType_ReviewKey]
            UNIQUE ([ReviewType], [ReviewKey])
    );

    CREATE NONCLUSTERED INDEX [IX_game_duplicate_review_Status]
        ON [competition].[game_duplicate_review] ([Status] ASC, [GameDate] ASC, [TournamentCode] ASC);
END;
GO

CREATE OR ALTER PROCEDURE [competition].[sp_refresh_tournament_duplicate_review]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Now DATETIME2(7) = SYSDATETIME();

    WITH source_rows AS
    (
        SELECT
            ReviewType = N'code_family',
            ReviewKey = CONCAT(N'code_family|', CodeBase),
            CodeBase,
            NormalizedName = CAST(NULL AS NVARCHAR(400)),
            TournamentDate,
            VariantCount,
            TournamentCodes,
            TournamentDescriptions,
            Locations = CAST(NULL AS NVARCHAR(MAX))
        FROM
        (
            SELECT
                CodeBase,
                MIN(Tournament_Date) AS TournamentDate,
                COUNT(*) AS VariantCount,
                STRING_AGG(Tournament_Code, ', ') WITHIN GROUP (ORDER BY Tournament_Code) AS TournamentCodes,
                STRING_AGG(COALESCE(Tournament_Descr, '<NULL>'), ' | ') WITHIN GROUP (ORDER BY Tournament_Code) AS TournamentDescriptions
            FROM
            (
                SELECT
                    Tournament_Code,
                    Tournament_Descr,
                    Tournament_Date,
                    CASE
                        WHEN Tournament_Code LIKE '%[a-z]' THEN LEFT(Tournament_Code, LEN(Tournament_Code) - 1)
                        ELSE Tournament_Code
                    END AS CodeBase
                FROM [ratings].[tournaments]
                WHERE Tournament_Date < '2000-01-01'
            ) AS family_source
            GROUP BY CodeBase
            HAVING COUNT(*) > 1
        ) AS code_family_rows

        UNION ALL

        SELECT
            ReviewType = N'normalized_name_date',
            ReviewKey = CONCAT(N'normalized_name_date|', NormalizedName, N'|', CONVERT(NVARCHAR(10), Tournament_Date, 23)),
            CodeBase = CAST(NULL AS NVARCHAR(255)),
            NormalizedName,
            Tournament_Date,
            TournamentCount,
            TournamentCodes,
            TournamentDescriptions = CAST(NULL AS NVARCHAR(MAX)),
            Locations
        FROM
        (
            SELECT
                NormalizedName,
                Tournament_Date,
                COUNT(*) AS TournamentCount,
                STRING_AGG(Tournament_Code, ', ') WITHIN GROUP (ORDER BY Tournament_Code) AS TournamentCodes,
                STRING_AGG(COALESCE(City, '<NULL>') + '/' + COALESCE(State_Code, '<NULL>'), ' | ') WITHIN GROUP (ORDER BY Tournament_Code) AS Locations
            FROM
            (
                SELECT
                    Tournament_Code,
                    Tournament_Date,
                    City,
                    State_Code,
                    UPPER(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    LTRIM(RTRIM(COALESCE(Tournament_Descr, ''))),
                                    ' ',
                                    ''
                                ),
                                '-',
                                ''
                            ),
                            '.',
                            ''
                        )
                    ) AS NormalizedName
                FROM [ratings].[tournaments]
                WHERE Tournament_Date < '2000-01-01'
            ) AS normalized_source
            WHERE NormalizedName <> ''
            GROUP BY NormalizedName, Tournament_Date
            HAVING COUNT(*) > 1
        ) AS normalized_rows
    )
    MERGE [competition].[tournament_duplicate_review] AS target
    USING source_rows AS source
        ON target.[ReviewType] = source.[ReviewType]
       AND target.[ReviewKey] = source.[ReviewKey]
    WHEN MATCHED THEN
        UPDATE SET
            [CodeBase] = source.[CodeBase],
            [NormalizedName] = source.[NormalizedName],
            [TournamentDate] = source.[TournamentDate],
            [VariantCount] = source.[VariantCount],
            [TournamentCodes] = source.[TournamentCodes],
            [TournamentDescriptions] = source.[TournamentDescriptions],
            [Locations] = source.[Locations],
            [LastRefreshed] = @Now
    WHEN NOT MATCHED THEN
        INSERT
        (
            [ReviewType], [ReviewKey], [CodeBase], [NormalizedName], [TournamentDate], [VariantCount],
            [TournamentCodes], [TournamentDescriptions], [Locations], [Status], [ReviewerNote],
            [CanonicalTournamentCode], [CreatedDate], [LastRefreshed]
        )
        VALUES
        (
            source.[ReviewType], source.[ReviewKey], source.[CodeBase], source.[NormalizedName], source.[TournamentDate], source.[VariantCount],
            source.[TournamentCodes], source.[TournamentDescriptions], source.[Locations], N'pending', NULL,
            NULL, @Now, @Now
        )
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
END;
GO

CREATE OR ALTER PROCEDURE [competition].[sp_refresh_game_duplicate_review]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Now DATETIME2(7) = SYSDATETIME();

    WITH source_rows AS
    (
        SELECT
            ReviewType = N'exact_early',
            ReviewKey = CONCAT(
                N'exact_early|',
                CONVERT(NVARCHAR(10), Game_Date, 23), N'|', COALESCE(Tournament_Code, '<NULL>'), N'|',
                COALESCE(CONVERT(NVARCHAR(20), Pin_Player_1), '<NULL>'), N'|', COALESCE(Color_1, '<NULL>'), N'|',
                COALESCE(CONVERT(NVARCHAR(20), Pin_Player_2), '<NULL>'), N'|', COALESCE(Color_2, '<NULL>'), N'|',
                COALESCE(CONVERT(NVARCHAR(20), Handicap), '<NULL>'), N'|', COALESCE(CONVERT(NVARCHAR(20), Komi), '<NULL>'), N'|', COALESCE(Result, '<NULL>')
            ),
            Game_Date,
            Tournament_Code,
            PlayerLowAGAID = CAST(NULL AS INT),
            PlayerHighAGAID = CAST(NULL AS INT),
            Pin_Player_1,
            Color_1,
            Pin_Player_2,
            Color_2,
            Handicap,
            Komi,
            Result,
            DuplicateCount,
            ExcludedCount,
            MinGameID,
            MaxGameID
        FROM
        (
            SELECT
                Game_Date,
                Tournament_Code,
                Pin_Player_1,
                Color_1,
                Pin_Player_2,
                Color_2,
                Handicap,
                Komi,
                Result,
                COUNT(*) AS DuplicateCount,
                SUM(CASE WHEN [Exclude] = 1 THEN 1 ELSE 0 END) AS ExcludedCount,
                MIN(Game_ID) AS MinGameID,
                MAX(Game_ID) AS MaxGameID
            FROM [ratings].[games]
            WHERE Game_Date < '2000-01-01'
            GROUP BY
                Game_Date,
                Tournament_Code,
                Pin_Player_1,
                Color_1,
                Pin_Player_2,
                Color_2,
                Handicap,
                Komi,
                Result
            HAVING COUNT(*) > 1
        ) AS exact_rows

        UNION ALL

        SELECT
            ReviewType = N'pair_early',
            ReviewKey = CONCAT(
                N'pair_early|',
                CONVERT(NVARCHAR(10), Game_Date, 23), N'|', COALESCE(Tournament_Code, '<NULL>'), N'|',
                COALESCE(CONVERT(NVARCHAR(20), PlayerLowAGAID), '<NULL>'), N'|', COALESCE(CONVERT(NVARCHAR(20), PlayerHighAGAID), '<NULL>'), N'|',
                COALESCE(CONVERT(NVARCHAR(20), Handicap), '<NULL>'), N'|', COALESCE(CONVERT(NVARCHAR(20), Komi), '<NULL>'), N'|', COALESCE(Result, '<NULL>')
            ),
            Game_Date,
            Tournament_Code,
            PlayerLowAGAID,
            PlayerHighAGAID,
            Pin_Player_1 = CAST(NULL AS INT),
            Color_1 = CAST(NULL AS NVARCHAR(20)),
            Pin_Player_2 = CAST(NULL AS INT),
            Color_2 = CAST(NULL AS NVARCHAR(20)),
            Handicap,
            Komi,
            Result,
            DuplicateCount,
            ExcludedCount,
            MinGameID = CAST(NULL AS INT),
            MaxGameID = CAST(NULL AS INT)
        FROM
        (
            SELECT
                Game_Date,
                Tournament_Code,
                CASE WHEN Pin_Player_1 < Pin_Player_2 THEN Pin_Player_1 ELSE Pin_Player_2 END AS PlayerLowAGAID,
                CASE WHEN Pin_Player_1 < Pin_Player_2 THEN Pin_Player_2 ELSE Pin_Player_1 END AS PlayerHighAGAID,
                Handicap,
                Komi,
                Result,
                COUNT(*) AS DuplicateCount,
                SUM(CASE WHEN [Exclude] = 1 THEN 1 ELSE 0 END) AS ExcludedCount
            FROM [ratings].[games]
            WHERE Game_Date < '2000-01-01'
            GROUP BY
                Game_Date,
                Tournament_Code,
                CASE WHEN Pin_Player_1 < Pin_Player_2 THEN Pin_Player_1 ELSE Pin_Player_2 END,
                CASE WHEN Pin_Player_1 < Pin_Player_2 THEN Pin_Player_2 ELSE Pin_Player_1 END,
                Handicap,
                Komi,
                Result
            HAVING COUNT(*) > 1
        ) AS pair_rows
    )
    MERGE [competition].[game_duplicate_review] AS target
    USING source_rows AS source
        ON target.[ReviewType] = source.[ReviewType]
       AND target.[ReviewKey] = source.[ReviewKey]
    WHEN MATCHED THEN
        UPDATE SET
            [GameDate] = source.[Game_Date],
            [TournamentCode] = source.[Tournament_Code],
            [PlayerLowAGAID] = source.[PlayerLowAGAID],
            [PlayerHighAGAID] = source.[PlayerHighAGAID],
            [Pin_Player_1] = source.[Pin_Player_1],
            [Color_1] = source.[Color_1],
            [Pin_Player_2] = source.[Pin_Player_2],
            [Color_2] = source.[Color_2],
            [Handicap] = source.[Handicap],
            [Komi] = source.[Komi],
            [Result] = source.[Result],
            [DuplicateCount] = source.[DuplicateCount],
            [ExcludedCount] = source.[ExcludedCount],
            [MinGameID] = source.[MinGameID],
            [MaxGameID] = source.[MaxGameID],
            [LastRefreshed] = @Now
    WHEN NOT MATCHED THEN
        INSERT
        (
            [ReviewType], [ReviewKey], [GameDate], [TournamentCode], [PlayerLowAGAID], [PlayerHighAGAID],
            [Pin_Player_1], [Color_1], [Pin_Player_2], [Color_2], [Handicap], [Komi], [Result],
            [DuplicateCount], [ExcludedCount], [MinGameID], [MaxGameID], [Status], [ReviewerNote],
            [CanonicalGameID], [CreatedDate], [LastRefreshed]
        )
        VALUES
        (
            source.[ReviewType], source.[ReviewKey], source.[Game_Date], source.[Tournament_Code], source.[PlayerLowAGAID], source.[PlayerHighAGAID],
            source.[Pin_Player_1], source.[Color_1], source.[Pin_Player_2], source.[Color_2], source.[Handicap], source.[Komi], source.[Result],
            source.[DuplicateCount], source.[ExcludedCount], source.[MinGameID], source.[MaxGameID], N'pending', NULL,
            NULL, @Now, @Now
        )
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
END;
GO

CREATE OR ALTER PROCEDURE [competition].[sp_refresh_duplicate_review]
AS
BEGIN
    SET NOCOUNT ON;
    EXEC [competition].[sp_refresh_tournament_duplicate_review];
    EXEC [competition].[sp_refresh_game_duplicate_review];
END;
GO

CREATE OR ALTER PROCEDURE [competition].[sp_mark_obvious_duplicate_review]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    UPDATE [competition].[game_duplicate_review]
    SET [Status] = N'likely_duplicate',
        [ReviewerNote] = N'Auto-marked: exact early duplicate group with duplicate count >= 5 and no excluded rows; highly likely repeated-import artifact.',
        [LastRefreshed] = SYSDATETIME()
    WHERE [ReviewType] = N'exact_early'
      AND [DuplicateCount] >= 5
      AND [ExcludedCount] = 0
      AND [Status] = N'pending';
END;
GO
