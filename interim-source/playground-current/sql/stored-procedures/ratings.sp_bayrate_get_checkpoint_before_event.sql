-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[sp_bayrate_get_checkpoint_before_event].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_bayrate_get_checkpoint_before_event]
    @ModelLabel NVARCHAR(200),
    @EventOrdinal INT
AS
BEGIN
    SET NOCOUNT ON;
    WITH ranked AS
    (
        SELECT c.[AGAID], c.[EventOrdinal], c.[EventKey], c.[EventDate], c.[Rating], c.[Sigma], c.[LastRatingDate], c.[MomentumStreak], ROW_NUMBER() OVER (PARTITION BY c.[AGAID] ORDER BY c.[EventOrdinal] DESC) AS rn
        FROM [ratings].[bayrate_player_checkpoint] AS c
        WHERE c.[ModelLabel] = @ModelLabel AND c.[EventOrdinal] < @EventOrdinal
    )
    SELECT [AGAID], [EventOrdinal], [EventKey], [EventDate], [Rating], [Sigma], [LastRatingDate], [MomentumStreak]
    FROM ranked
    WHERE rn = 1
    ORDER BY [AGAID];
END;
GO
