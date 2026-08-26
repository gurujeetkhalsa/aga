-- SPDX-FileCopyrightText: 2010 Philip Waldron
-- SPDX-FileCopyrightText: 2026 American Go Association
-- SPDX-License-Identifier: GPL-3.0-or-later
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

IF OBJECT_ID(N'ratings.admin_permissions', N'U') IS NULL
BEGIN
    CREATE TABLE [ratings].[admin_permissions]
    (
        [AdminPermissionID] int IDENTITY(1, 1) NOT NULL,
        [Principal_Name] nvarchar(256) NOT NULL,
        [Principal_Id] nvarchar(128) NULL,
        [Display_Name] nvarchar(256) NULL,
        [Permission_Code] nvarchar(64) NOT NULL,
        [Is_Active] bit NOT NULL CONSTRAINT [DF_admin_permissions_Is_Active] DEFAULT 1,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_admin_permissions_Created_At] DEFAULT SYSUTCDATETIME(),
        [Created_By] nvarchar(128) NULL CONSTRAINT [DF_admin_permissions_Created_By] DEFAULT SUSER_SNAME(),
        [Revoked_At] datetime2(0) NULL,
        [Revoked_By] nvarchar(128) NULL,
        CONSTRAINT [PK_admin_permissions] PRIMARY KEY CLUSTERED ([AdminPermissionID]),
        CONSTRAINT [UQ_admin_permissions_Principal_Permission] UNIQUE ([Principal_Name], [Permission_Code])
    );
END;

IF COL_LENGTH(N'ratings.admin_permissions', N'Principal_Id') IS NULL
BEGIN
    ALTER TABLE [ratings].[admin_permissions]
        ADD [Principal_Id] nvarchar(128) NULL;
END;

IF COL_LENGTH(N'ratings.admin_permissions', N'Display_Name') IS NULL
BEGIN
    ALTER TABLE [ratings].[admin_permissions]
        ADD [Display_Name] nvarchar(256) NULL;
END;

IF COL_LENGTH(N'ratings.admin_permissions', N'Is_Active') IS NULL
BEGIN
    ALTER TABLE [ratings].[admin_permissions]
        ADD [Is_Active] bit NOT NULL CONSTRAINT [DF_admin_permissions_Is_Active] DEFAULT 1;
END;

IF COL_LENGTH(N'ratings.admin_permissions', N'Revoked_At') IS NULL
BEGIN
    ALTER TABLE [ratings].[admin_permissions]
        ADD [Revoked_At] datetime2(0) NULL;
END;

IF COL_LENGTH(N'ratings.admin_permissions', N'Revoked_By') IS NULL
BEGIN
    ALTER TABLE [ratings].[admin_permissions]
        ADD [Revoked_By] nvarchar(128) NULL;
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_admin_permissions_Principal_Id'
      AND [object_id] = OBJECT_ID(N'ratings.admin_permissions')
)
BEGIN
    CREATE INDEX [IX_admin_permissions_Principal_Id]
        ON [ratings].[admin_permissions] ([Principal_Id], [Permission_Code])
        WHERE [Principal_Id] IS NOT NULL;
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_admin_permissions_Permission_Active'
      AND [object_id] = OBJECT_ID(N'ratings.admin_permissions')
)
BEGIN
    CREATE INDEX [IX_admin_permissions_Permission_Active]
        ON [ratings].[admin_permissions] ([Permission_Code], [Is_Active], [Principal_Name])
        INCLUDE ([Principal_Id], [Display_Name]);
END;

INSERT INTO [ratings].[admin_permissions]
(
    [Principal_Name],
    [Principal_Id],
    [Display_Name],
    [Permission_Code],
    [Is_Active],
    [Created_By]
)
SELECT
    admin.[Principal_Name],
    admin.[Principal_Id],
    admin.[Display_Name],
    N'admin_all',
    CAST(1 AS bit),
    N'bayrate_admins_backfill'
FROM [ratings].[bayrate_admins] AS admin
WHERE admin.[Is_Active] = 1
  AND NOT EXISTS
  (
      SELECT 1
      FROM [ratings].[admin_permissions] AS existing
      WHERE existing.[Principal_Name] = admin.[Principal_Name]
        AND existing.[Permission_Code] = N'admin_all'
  );

GO

CREATE OR ALTER PROCEDURE [ratings].[sp_grant_admin_permission]
    @Principal_Name nvarchar(256),
    @Permission_Code nvarchar(64),
    @Principal_Id nvarchar(128) = NULL,
    @Display_Name nvarchar(256) = NULL,
    @Granted_By nvarchar(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    SET @Principal_Name = NULLIF(LTRIM(RTRIM(@Principal_Name)), N'');
    SET @Permission_Code = NULLIF(LTRIM(RTRIM(@Permission_Code)), N'');
    SET @Principal_Id = NULLIF(LTRIM(RTRIM(@Principal_Id)), N'');
    SET @Display_Name = NULLIF(LTRIM(RTRIM(@Display_Name)), N'');
    SET @Granted_By = COALESCE(NULLIF(LTRIM(RTRIM(@Granted_By)), N''), SUSER_SNAME());

    IF @Principal_Name IS NULL
        THROW 51010, N'Principal_Name is required.', 1;

    IF @Permission_Code IS NULL
        THROW 51011, N'Permission_Code is required.', 1;

    MERGE [ratings].[admin_permissions] WITH (HOLDLOCK) AS target
    USING
    (
        SELECT
            @Principal_Name AS [Principal_Name],
            @Principal_Id AS [Principal_Id],
            @Display_Name AS [Display_Name],
            @Permission_Code AS [Permission_Code],
            @Granted_By AS [Granted_By]
    ) AS source
    ON target.[Principal_Name] = source.[Principal_Name]
       AND target.[Permission_Code] = source.[Permission_Code]
    WHEN MATCHED THEN
        UPDATE SET
            [Principal_Id] = COALESCE(source.[Principal_Id], target.[Principal_Id]),
            [Display_Name] = COALESCE(source.[Display_Name], target.[Display_Name]),
            [Is_Active] = 1,
            [Revoked_At] = NULL,
            [Revoked_By] = NULL
    WHEN NOT MATCHED THEN
        INSERT
        (
            [Principal_Name],
            [Principal_Id],
            [Display_Name],
            [Permission_Code],
            [Is_Active],
            [Created_By]
        )
        VALUES
        (
            source.[Principal_Name],
            source.[Principal_Id],
            source.[Display_Name],
            source.[Permission_Code],
            1,
            source.[Granted_By]
        );

    SELECT
        [AdminPermissionID],
        [Principal_Name],
        [Principal_Id],
        [Display_Name],
        [Permission_Code],
        [Is_Active],
        [Created_At],
        [Created_By],
        [Revoked_At],
        [Revoked_By]
    FROM [ratings].[admin_permissions]
    WHERE [Principal_Name] = @Principal_Name
      AND [Permission_Code] = @Permission_Code;
END;

GO

CREATE OR ALTER PROCEDURE [ratings].[sp_revoke_admin_permission]
    @Principal_Name nvarchar(256),
    @Permission_Code nvarchar(64),
    @Revoked_By nvarchar(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    SET @Principal_Name = NULLIF(LTRIM(RTRIM(@Principal_Name)), N'');
    SET @Permission_Code = NULLIF(LTRIM(RTRIM(@Permission_Code)), N'');
    SET @Revoked_By = COALESCE(NULLIF(LTRIM(RTRIM(@Revoked_By)), N''), SUSER_SNAME());

    IF @Principal_Name IS NULL
        THROW 51012, N'Principal_Name is required.', 1;

    IF @Permission_Code IS NULL
        THROW 51013, N'Permission_Code is required.', 1;

    UPDATE [ratings].[admin_permissions]
       SET [Is_Active] = 0,
           [Revoked_At] = SYSUTCDATETIME(),
           [Revoked_By] = @Revoked_By
     WHERE [Principal_Name] = @Principal_Name
       AND [Permission_Code] = @Permission_Code;

    IF @@ROWCOUNT = 0
        THROW 51014, N'Admin permission was not found.', 1;

    SELECT
        [AdminPermissionID],
        [Principal_Name],
        [Principal_Id],
        [Display_Name],
        [Permission_Code],
        [Is_Active],
        [Created_At],
        [Created_By],
        [Revoked_At],
        [Revoked_By]
    FROM [ratings].[admin_permissions]
    WHERE [Principal_Name] = @Principal_Name
      AND [Permission_Code] = @Permission_Code;
END;

GO
