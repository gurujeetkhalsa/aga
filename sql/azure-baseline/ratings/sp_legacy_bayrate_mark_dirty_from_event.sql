-- Live Azure SQL metadata; database aga-membership; SQL_STORED_PROCEDURE
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   PROCEDURE [ratings].[sp_legacy_bayrate_mark_dirty_from_event]
    @ModelLabel NVARCHAR(200),
    @EventKey NVARCHAR(300),
    @Reason NVARCHAR(200) = N'manual'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @EventOrdinal INT;
    SELECT @EventOrdinal = [EventOrdinal] FROM [ratings].[legacy_bayrate_event_index] WHERE [ModelLabel] = @ModelLabel AND [EventKey] = @EventKey;
    IF @EventOrdinal IS NULL THROW 50001, 'EventKey not found for model label.', 1;
    UPDATE [ratings].[legacy_bayrate_event_index]
    SET [Dirty] = 1, [DirtyReason] = @Reason, [LastUpdated] = SYSDATETIME()
    WHERE [ModelLabel] = @ModelLabel AND [EventOrdinal] >= @EventOrdinal;
END;

GO
