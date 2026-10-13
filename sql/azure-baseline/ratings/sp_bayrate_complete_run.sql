-- Live Azure SQL metadata; database aga-membership; SQL_STORED_PROCEDURE
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   PROCEDURE [ratings].[sp_bayrate_complete_run]
    @RunID BIGINT,
    @EventCount INT,
    @PlayerCount INT,
    @GameCount INT
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [ratings].[bayrate_runs]
    SET [Status] = N'completed', [EventCount] = @EventCount, [PlayerCount] = @PlayerCount, [GameCount] = @GameCount, [CompletedAt] = SYSDATETIME(), [LastUpdated] = SYSDATETIME()
    WHERE [RunID] = @RunID;
END;
GO
