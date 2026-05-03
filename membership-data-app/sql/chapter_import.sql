IF SCHEMA_ID(N'staging') IS NULL
    EXEC(N'CREATE SCHEMA [staging]');
GO

IF OBJECT_ID(N'staging.chapters', N'U') IS NULL
BEGIN
    CREATE TABLE [staging].[chapters]
    (
        [ChapterID] int NULL,
        [ChapterCode] nvarchar(20) NULL,
        [ChapterName] nvarchar(200) NULL,
        [City] nvarchar(100) NULL,
        [State] nvarchar(50) NULL,
        [ChapterRepID] int NULL,
        [CreatedDate] datetime NULL,
        [Status] nvarchar(20) NULL
    );
END;
GO

CREATE OR ALTER PROCEDURE [membership].[sp_import_chapters]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Now datetime2(7) = SYSDATETIME();

    DECLARE @MergeResults table
    (
        [ActionType] nvarchar(10) NOT NULL,
        [ChapterID] int NOT NULL
    );

    BEGIN TRY
        IF OBJECT_ID(N'staging.chapters', N'U') IS NULL
        BEGIN
            THROW 50120, 'staging.chapters does not exist.', 1;
        END;

        IF EXISTS (SELECT 1 FROM [staging].[chapters] WHERE [ChapterID] IS NULL)
        BEGIN
            THROW 50121, 'staging.chapters contains rows with NULL ChapterID.', 1;
        END;

        IF EXISTS (SELECT 1 FROM [staging].[chapters] WHERE NULLIF(LTRIM(RTRIM([ChapterCode])), N'') IS NULL)
        BEGIN
            THROW 50123, 'staging.chapters contains rows with NULL or blank ChapterCode.', 1;
        END;

        IF EXISTS (SELECT 1 FROM [staging].[chapters] WHERE NULLIF(LTRIM(RTRIM([ChapterName])), N'') IS NULL)
        BEGIN
            THROW 50124, 'staging.chapters contains rows with NULL or blank ChapterName.', 1;
        END;

        IF EXISTS
        (
            SELECT 1
            FROM [staging].[chapters]
            GROUP BY [ChapterID]
            HAVING COUNT(*) > 1
        )
        BEGIN
            THROW 50122, 'staging.chapters contains duplicate ChapterID values.', 1;
        END;

        BEGIN TRANSACTION;

        ;WITH [source_rows] AS
        (
            SELECT
                s.[ChapterID],
                NULLIF(LTRIM(RTRIM(s.[ChapterCode])), N'') AS [ChapterCode],
                NULLIF(LTRIM(RTRIM(s.[ChapterName])), N'') AS [ChapterName],
                NULLIF(LTRIM(RTRIM(s.[City])), N'') AS [City],
                NULLIF(LTRIM(RTRIM(s.[State])), N'') AS [State],
                s.[ChapterRepID],
                CASE WHEN reps.[AGAID] IS NOT NULL THEN 1 ELSE 0 END AS [ChapterRepExists],
                s.[CreatedDate],
                NULLIF(LTRIM(RTRIM(s.[Status])), N'') AS [Status]
            FROM [staging].[chapters] AS s
            LEFT JOIN [membership].[members] AS reps
                ON reps.[AGAID] = s.[ChapterRepID]
        )
        MERGE [membership].[chapters] AS target
        USING [source_rows] AS source
            ON target.[ChapterID] = source.[ChapterID]
        WHEN MATCHED AND
        (
            (source.[ChapterCode] IS NOT NULL AND ISNULL(target.[ChapterCode], N'') <> source.[ChapterCode])
            OR (source.[ChapterName] IS NOT NULL AND ISNULL(target.[ChapterName], N'') <> source.[ChapterName])
            OR (source.[City] IS NOT NULL AND ISNULL(target.[City], N'') <> source.[City])
            OR (source.[State] IS NOT NULL AND ISNULL(target.[State], N'') <> source.[State])
            OR (source.[ChapterRepID] IS NOT NULL AND source.[ChapterRepExists] = 1 AND ISNULL(target.[ChapterRepID], -1) <> source.[ChapterRepID])
            OR (source.[CreatedDate] IS NOT NULL AND ISNULL(target.[CreatedDate], '19000101') <> source.[CreatedDate])
            OR (source.[Status] IS NOT NULL AND ISNULL(target.[Status], N'') <> source.[Status])
        )
        THEN UPDATE SET
            target.[ChapterCode] = COALESCE(source.[ChapterCode], target.[ChapterCode]),
            target.[ChapterName] = COALESCE(source.[ChapterName], target.[ChapterName]),
            target.[City] = COALESCE(source.[City], target.[City]),
            target.[State] = COALESCE(source.[State], target.[State]),
            target.[ChapterRepID] =
                CASE
                    WHEN source.[ChapterRepID] IS NOT NULL AND source.[ChapterRepExists] = 1 THEN source.[ChapterRepID]
                    ELSE target.[ChapterRepID]
                END,
            target.[CreatedDate] = COALESCE(source.[CreatedDate], target.[CreatedDate]),
            target.[Status] = COALESCE(source.[Status], target.[Status])
        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                [ChapterID],
                [ChapterCode],
                [ChapterName],
                [City],
                [State],
                [ChapterRepID],
                [CreatedDate],
                [Status]
            )
            VALUES
            (
                source.[ChapterID],
                source.[ChapterCode],
                source.[ChapterName],
                source.[City],
                source.[State],
                CASE WHEN source.[ChapterRepExists] = 1 THEN source.[ChapterRepID] ELSE NULL END,
                COALESCE(source.[CreatedDate], @Now),
                source.[Status]
            )
        OUTPUT
            $action,
            inserted.[ChapterID]
        INTO @MergeResults;

        TRUNCATE TABLE [staging].[chapters];

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        THROW;
    END CATCH;

    SELECT
        SUM(CASE WHEN [ActionType] = N'INSERT' THEN 1 ELSE 0 END) AS [InsertedChapterCount],
        SUM(CASE WHEN [ActionType] = N'UPDATE' THEN 1 ELSE 0 END) AS [UpdatedChapterCount],
        COUNT(*) AS [ChangedChapterCount]
    FROM @MergeResults;
END;
GO
