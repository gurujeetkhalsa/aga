-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[sp_legacy_bayrate_get_replay_boundary].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_legacy_bayrate_get_replay_boundary]
    @ModelLabel NVARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT TOP (1) [EventOrdinal], [EventKey], [EventDate], [Tournament_Code]
    FROM [ratings].[legacy_bayrate_event_index]
    WHERE [ModelLabel] = @ModelLabel AND [Dirty] = 1
    ORDER BY [EventOrdinal];
END;
GO
