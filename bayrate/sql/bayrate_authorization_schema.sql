IF SCHEMA_ID(N'ratings') IS NULL
    EXEC(N'CREATE SCHEMA [ratings]');

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

IF COL_LENGTH(N'ratings.bayrate_admins', N'Principal_Id') IS NULL
BEGIN
    ALTER TABLE [ratings].[bayrate_admins]
        ADD [Principal_Id] nvarchar(128) NULL;
END;

IF COL_LENGTH(N'ratings.bayrate_admins', N'Display_Name') IS NULL
BEGIN
    ALTER TABLE [ratings].[bayrate_admins]
        ADD [Display_Name] nvarchar(256) NULL;
END;

IF COL_LENGTH(N'ratings.bayrate_admins', N'Is_Active') IS NULL
BEGIN
    ALTER TABLE [ratings].[bayrate_admins]
        ADD [Is_Active] bit NOT NULL CONSTRAINT [DF_bayrate_admins_Is_Active] DEFAULT 1;
END;

IF COL_LENGTH(N'ratings.bayrate_admins', N'Revoked_At') IS NULL
BEGIN
    ALTER TABLE [ratings].[bayrate_admins]
        ADD [Revoked_At] datetime2(0) NULL;
END;

IF COL_LENGTH(N'ratings.bayrate_admins', N'Revoked_By') IS NULL
BEGIN
    ALTER TABLE [ratings].[bayrate_admins]
        ADD [Revoked_By] nvarchar(128) NULL;
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
