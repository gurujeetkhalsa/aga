-- Live Azure SQL stored procedure export.
-- Source object: [membership].[sp_import_member_categories].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
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
