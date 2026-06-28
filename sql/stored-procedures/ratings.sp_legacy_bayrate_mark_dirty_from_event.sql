-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[sp_legacy_bayrate_mark_dirty_from_event].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_legacy_bayrate_mark_dirty_from_event]
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
