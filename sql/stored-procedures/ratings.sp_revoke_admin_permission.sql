-- Copyright 2026, American Go Association, All rights reserved

-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[sp_revoke_admin_permission].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
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
