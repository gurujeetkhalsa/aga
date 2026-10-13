-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[sp_bayrate_get_snapshot_seed_before_event].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_get_snapshot_seed_before_event]
    @ModelLabel NVARCHAR(200),
    @EventOrdinal INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @EventDate DATE;
    SELECT @EventDate = [EventDate]
    FROM [ratings].[bayrate_event_index]
    WHERE [ModelLabel] = @ModelLabel AND [EventOrdinal] = @EventOrdinal;

    IF @EventDate IS NULL
        THROW 50002, 'EventOrdinal not found for model label.', 1;

    WITH ranked AS
    (
        SELECT r.[Pin_Player] AS [AGAID],
               r.[Rating],
               r.[Sigma],
               r.[Elab_Date] AS [LastRatingDate],
               ROW_NUMBER() OVER (PARTITION BY r.[Pin_Player] ORDER BY r.[Elab_Date] DESC, r.[id] DESC) AS rn
        FROM [ratings].[ratings] AS r
        WHERE r.[Elab_Date] < @EventDate
    )
    SELECT [AGAID], [Rating], [Sigma], [LastRatingDate]
    FROM ranked
    WHERE rn = 1
    ORDER BY [AGAID];
END;
GO
