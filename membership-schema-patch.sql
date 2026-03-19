IF COL_LENGTH('staging.memchap', 'LastLogin') IS NULL
BEGIN
    ALTER TABLE [staging].[memchap]
        ADD [LastLogin] DATETIME2(7) NULL;
END;
GO
