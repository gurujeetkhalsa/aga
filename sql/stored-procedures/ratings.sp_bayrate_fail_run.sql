-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[sp_bayrate_fail_run].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_fail_run]
    @RunID BIGINT,
    @ErrorMessage NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [ratings].[bayrate_runs]
    SET [Status] = N'failed', [ErrorMessage] = @ErrorMessage, [CompletedAt] = SYSDATETIME(), [LastUpdated] = SYSDATETIME()
    WHERE [RunID] = @RunID;
END;
GO
