-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[sp_bayrate_purge_from_event].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_purge_from_event]
    @ModelLabel NVARCHAR(200),
    @EventOrdinal INT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRAN;
    DELETE FROM [ratings].[bayrate_event_game_results] WHERE [ModelLabel] = @ModelLabel AND [EventOrdinal] >= @EventOrdinal;
    DELETE FROM [ratings].[bayrate_event_player_ratings] WHERE [ModelLabel] = @ModelLabel AND [EventOrdinal] >= @EventOrdinal;
    DELETE FROM [ratings].[bayrate_player_checkpoint] WHERE [ModelLabel] = @ModelLabel AND [EventOrdinal] >= @EventOrdinal;
    UPDATE [ratings].[bayrate_event_index]
    SET [LastComputedRunID] = NULL, [LastComputedAt] = NULL, [LastUpdated] = SYSDATETIME()
    WHERE [ModelLabel] = @ModelLabel AND [EventOrdinal] >= @EventOrdinal;
    COMMIT;
END;
GO
