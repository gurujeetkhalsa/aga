-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[sp_bayrate_start_run].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_start_run]
    @ModelLabel NVARCHAR(200),
    @ConfigHash VARBINARY(32),
    @SourceVersion NVARCHAR(200) = NULL,
    @ReplayFromEventOrdinal INT,
    @ReplayFromEventKey NVARCHAR(300),
    @ReplayFromEventDate DATE
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO [ratings].[bayrate_runs]
    ([ModelLabel], [ConfigHash], [SourceVersion], [Status], [ReplayFromEventOrdinal], [ReplayFromEventKey], [ReplayFromEventDate])
    VALUES (@ModelLabel, @ConfigHash, @SourceVersion, N'running', @ReplayFromEventOrdinal, @ReplayFromEventKey, @ReplayFromEventDate);
    SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [RunID];
END;
GO
