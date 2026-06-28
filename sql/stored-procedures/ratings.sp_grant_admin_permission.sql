-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[sp_grant_admin_permission].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
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
