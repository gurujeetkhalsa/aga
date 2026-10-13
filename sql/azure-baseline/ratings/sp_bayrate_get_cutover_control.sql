-- Live Azure SQL metadata; database aga-membership; SQL_STORED_PROCEDURE
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO


CREATE   PROCEDURE [ratings].[sp_bayrate_get_cutover_control]
    @ModelLabel NVARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT c.[ModelLabel], c.[CutoverEventOrdinal], ei.[EventKey] AS [CutoverEventKey], ei.[EventDate] AS [CutoverEventDate],
           c.[SnapshotSeedEnabled], c.[FreezeBeforeCutover]
    FROM [ratings].[bayrate_control] AS c
    LEFT JOIN [ratings].[bayrate_event_index] AS ei
      ON ei.[ModelLabel] = c.[ModelLabel]
     AND ei.[EventOrdinal] = c.[CutoverEventOrdinal]
    WHERE c.[ModelLabel] = @ModelLabel;
END;
GO
