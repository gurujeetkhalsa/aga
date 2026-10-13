-- Live Azure SQL metadata; database aga-membership; SQL_STORED_PROCEDURE
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   PROCEDURE [ratings].[sp_bayrate_purge_from_event]
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
