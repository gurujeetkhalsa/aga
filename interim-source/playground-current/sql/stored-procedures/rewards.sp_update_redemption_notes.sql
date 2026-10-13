-- Copyright 2026, American Go Association, All rights reserved

-- Live Azure SQL stored procedure export.
-- Source object: [rewards].[sp_update_redemption_notes].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_update_redemption_notes]
    @RedemptionID bigint,
    @Notes nvarchar(max) = NULL,
    @UpdatedByPrincipalName nvarchar(256) = NULL,
    @UpdatedByPrincipalId nvarchar(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    UPDATE [rewards].[redemption_requests]
    SET
        [Notes] = NULLIF(LTRIM(RTRIM(@Notes)), N''),
        [Notes_Updated_At] = SYSUTCDATETIME(),
        [Notes_Updated_By_Principal_Name] = @UpdatedByPrincipalName,
        [Notes_Updated_By_Principal_Id] = @UpdatedByPrincipalId
    WHERE [RedemptionID] = @RedemptionID;

    IF @@ROWCOUNT = 0
        THROW 52926, N'Redemption request was not found.', 1;

    SELECT
        [RedemptionID],
        [Notes],
        [Notes_Updated_At],
        [Notes_Updated_By_Principal_Name]
    FROM [rewards].[redemption_requests]
    WHERE [RedemptionID] = @RedemptionID;
END;
GO
