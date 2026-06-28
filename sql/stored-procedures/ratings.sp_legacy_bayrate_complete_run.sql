-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[sp_legacy_bayrate_complete_run].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_legacy_bayrate_complete_run]
    @RunID BIGINT,
    @EventCount INT,
    @PlayerCount INT,
    @GameCount INT
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [ratings].[legacy_bayrate_runs]
    SET [Status] = N'completed', [EventCount] = @EventCount, [PlayerCount] = @PlayerCount, [GameCount] = @GameCount, [CompletedAt] = SYSDATETIME(), [LastUpdated] = SYSDATETIME()
    WHERE [RunID] = @RunID;
END;
GO
