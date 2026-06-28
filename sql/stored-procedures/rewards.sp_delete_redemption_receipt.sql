-- Copyright 2026, American Go Association, All rights reserved

-- Live Azure SQL stored procedure export.
-- Source object: [rewards].[sp_delete_redemption_receipt].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_delete_redemption_receipt]
    @ReceiptID bigint,
    @DeletedByPrincipalName nvarchar(256) = NULL,
    @DeletedByPrincipalId nvarchar(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'rewards.redemption_receipts', N'U') IS NULL
        THROW 52924, N'Rewards redemption receipt table does not exist. Apply rewards/sql/redemption_processing.sql first.', 1;

    UPDATE [rewards].[redemption_receipts]
    SET
        [Is_Deleted] = 1,
        [Deleted_At] = COALESCE([Deleted_At], SYSUTCDATETIME()),
        [Deleted_By_Principal_Name] = COALESCE([Deleted_By_Principal_Name], @DeletedByPrincipalName),
        [Deleted_By_Principal_Id] = COALESCE([Deleted_By_Principal_Id], @DeletedByPrincipalId)
    WHERE [ReceiptID] = @ReceiptID;

    IF @@ROWCOUNT = 0
        THROW 52925, N'Receipt was not found.', 1;

    SELECT
        [ReceiptID],
        [RedemptionID],
        [Is_Deleted],
        [Deleted_At]
    FROM [rewards].[redemption_receipts]
    WHERE [ReceiptID] = @ReceiptID;
END;
GO
