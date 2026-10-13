-- Live Azure SQL metadata; database aga-membership; SQL_STORED_PROCEDURE
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   PROCEDURE [ratings].[sp_bayrate_get_replay_boundary]
    @ModelLabel NVARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT TOP (1) [EventOrdinal], [EventKey], [EventDate], [Tournament_Code]
    FROM [ratings].[bayrate_event_index]
    WHERE [ModelLabel] = @ModelLabel AND [Dirty] = 1
    ORDER BY [EventOrdinal];
END;
GO
