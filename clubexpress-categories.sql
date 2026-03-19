IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'membership')
BEGIN
    EXEC(N'CREATE SCHEMA [membership]');
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'staging')
BEGIN
    EXEC(N'CREATE SCHEMA [staging]');
END;
GO

IF OBJECT_ID(N'[membership].[categories]', N'U') IS NULL
BEGIN
    CREATE TABLE [membership].[categories]
    (
        [CategoryID] INT IDENTITY(1,1) NOT NULL,
        [CategoryName] NVARCHAR(200) NOT NULL,
        [CreatedDate] DATETIME2(7) NOT NULL
            CONSTRAINT [DF_categories_CreatedDate] DEFAULT (SYSDATETIME()),
        [LastUpdated] DATETIME2(7) NOT NULL
            CONSTRAINT [DF_categories_LastUpdated] DEFAULT (SYSDATETIME()),
        CONSTRAINT [PK_categories] PRIMARY KEY CLUSTERED ([CategoryID] ASC),
        CONSTRAINT [UQ_categories_CategoryName] UNIQUE ([CategoryName])
    );
END;
GO

IF OBJECT_ID(N'[membership].[member_categories]', N'U') IS NULL
BEGIN
    CREATE TABLE [membership].[member_categories]
    (
        [AGAID] INT NOT NULL,
        [CategoryID] INT NOT NULL,
        [CreatedDate] DATETIME2(7) NOT NULL
            CONSTRAINT [DF_member_categories_CreatedDate] DEFAULT (SYSDATETIME()),
        [LastUpdated] DATETIME2(7) NOT NULL
            CONSTRAINT [DF_member_categories_LastUpdated] DEFAULT (SYSDATETIME()),
        CONSTRAINT [PK_member_categories] PRIMARY KEY CLUSTERED ([AGAID] ASC, [CategoryID] ASC)
    );

    ALTER TABLE [membership].[member_categories]
        ADD CONSTRAINT [FK_member_categories_member]
            FOREIGN KEY ([AGAID]) REFERENCES [membership].[members] ([AGAID]);

    ALTER TABLE [membership].[member_categories]
        ADD CONSTRAINT [FK_member_categories_category]
            FOREIGN KEY ([CategoryID]) REFERENCES [membership].[categories] ([CategoryID]);
END;
GO

IF OBJECT_ID(N'[staging].[member_categories]', N'U') IS NULL
BEGIN
    CREATE TABLE [staging].[member_categories]
    (
        [AGAID] INT NOT NULL,
        [Category] NVARCHAR(200) NOT NULL
    );

    CREATE NONCLUSTERED INDEX [IX_staging_member_categories_AGAID]
        ON [staging].[member_categories] ([AGAID] ASC);
END;
GO

CREATE OR ALTER PROCEDURE [membership].[sp_import_member_categories]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Now DATETIME2(7) = SYSDATETIME();

    BEGIN TRY
        IF EXISTS (SELECT 1 FROM [staging].[member_categories] WHERE [AGAID] IS NULL OR LTRIM(RTRIM([Category])) = N'')
        BEGIN
            THROW 50201, 'staging.member_categories contains NULL or blank values.', 1;
        END;

        IF EXISTS
        (
            SELECT 1
            FROM [staging].[member_categories]
            GROUP BY [AGAID], [Category]
            HAVING COUNT(*) > 1
        )
        BEGIN
            THROW 50202, 'staging.member_categories contains duplicate AGAID/category pairs.', 1;
        END;

        BEGIN TRANSACTION;

        MERGE [membership].[categories] AS target
        USING
        (
            SELECT DISTINCT LTRIM(RTRIM([Category])) AS [CategoryName]
            FROM [staging].[member_categories]
        ) AS source
        ON target.[CategoryName] = source.[CategoryName]
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([CategoryName], [CreatedDate], [LastUpdated])
            VALUES (source.[CategoryName], @Now, @Now)
        WHEN MATCHED THEN
            UPDATE SET [LastUpdated] = @Now;

        MERGE [membership].[member_categories] AS target
        USING
        (
            SELECT DISTINCT
                source.[AGAID],
                category_row.[CategoryID]
            FROM [staging].[member_categories] AS source
            INNER JOIN [membership].[members] AS member_row
                ON member_row.[AGAID] = source.[AGAID]
            INNER JOIN [membership].[categories] AS category_row
                ON category_row.[CategoryName] = LTRIM(RTRIM(source.[Category]))
        ) AS source
        ON target.[AGAID] = source.[AGAID]
           AND target.[CategoryID] = source.[CategoryID]
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([AGAID], [CategoryID], [CreatedDate], [LastUpdated])
            VALUES (source.[AGAID], source.[CategoryID], @Now, @Now)
        WHEN MATCHED THEN
            UPDATE SET [LastUpdated] = @Now
        WHEN NOT MATCHED BY SOURCE THEN
            DELETE;

        TRUNCATE TABLE [staging].[member_categories];

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        THROW;
    END CATCH;
END;
GO
