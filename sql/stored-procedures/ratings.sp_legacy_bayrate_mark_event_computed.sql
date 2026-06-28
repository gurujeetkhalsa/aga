-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[sp_legacy_bayrate_mark_event_computed].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_legacy_bayrate_mark_event_computed]
    @ModelLabel NVARCHAR(200),
    @EventOrdinal INT,
    @RunID BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [ratings].[legacy_bayrate_event_index]
    SET [Dirty] = 0, [DirtyReason] = NULL, [LastComputedRunID] = @RunID, [LastComputedAt] = SYSDATETIME(), [LastUpdated] = SYSDATETIME()
    WHERE [ModelLabel] = @ModelLabel AND [EventOrdinal] = @EventOrdinal;
    UPDATE r
    SET [CompletedThroughEventOrdinal] = ei.[EventOrdinal], [CompletedThroughEventKey] = ei.[EventKey], [CompletedThroughEventDate] = ei.[EventDate], [LastUpdated] = SYSDATETIME()
    FROM [ratings].[legacy_bayrate_runs] AS r
    JOIN [ratings].[legacy_bayrate_event_index] AS ei ON ei.[ModelLabel] = r.[ModelLabel] AND ei.[EventOrdinal] = @EventOrdinal
    WHERE r.[RunID] = @RunID;
END;
GO
