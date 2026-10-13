-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[sp_bayrate_set_cutover_control].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_set_cutover_control]
    @ModelLabel NVARCHAR(200),
    @CutoverEventOrdinal INT,
    @SnapshotSeedEnabled BIT = 1,
    @FreezeBeforeCutover BIT = 1,
    @MarkPreCutoverClean BIT = 1
AS
BEGIN
    SET NOCOUNT ON;

    MERGE [ratings].[bayrate_control] AS tgt
    USING (
        SELECT @ModelLabel AS [ModelLabel],
               @CutoverEventOrdinal AS [CutoverEventOrdinal],
               @SnapshotSeedEnabled AS [SnapshotSeedEnabled],
               @FreezeBeforeCutover AS [FreezeBeforeCutover]
    ) AS src
      ON tgt.[ModelLabel] = src.[ModelLabel]
    WHEN MATCHED THEN
        UPDATE SET [CutoverEventOrdinal] = src.[CutoverEventOrdinal],
                   [SnapshotSeedEnabled] = src.[SnapshotSeedEnabled],
                   [FreezeBeforeCutover] = src.[FreezeBeforeCutover],
                   [LastUpdated] = SYSDATETIME()
    WHEN NOT MATCHED THEN
        INSERT ([ModelLabel], [CutoverEventOrdinal], [SnapshotSeedEnabled], [FreezeBeforeCutover])
        VALUES (src.[ModelLabel], src.[CutoverEventOrdinal], src.[SnapshotSeedEnabled], src.[FreezeBeforeCutover]);

    IF @MarkPreCutoverClean = 1
    BEGIN
        UPDATE [ratings].[bayrate_event_index]
        SET [Dirty] = 0,
            [DirtyReason] = NULL,
            [LastUpdated] = SYSDATETIME()
        WHERE [ModelLabel] = @ModelLabel
          AND [EventOrdinal] < @CutoverEventOrdinal;
    END;
END;
GO
