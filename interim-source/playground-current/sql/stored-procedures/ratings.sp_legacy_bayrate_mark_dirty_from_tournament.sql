-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[sp_legacy_bayrate_mark_dirty_from_tournament].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [ratings].[sp_legacy_bayrate_mark_dirty_from_tournament]
      @ModelLabel NVARCHAR(200),
      @TournamentCode NVARCHAR(255),
      @Reason NVARCHAR(200) = N'tournament_changed'
  AS
  BEGIN
      SET NOCOUNT ON;

      DECLARE @EventKey NVARCHAR(300);
      SET @EventKey = CONCAT(N'code:', @TournamentCode);

      EXEC [ratings].[sp_legacy_bayrate_mark_dirty_from_event]
          @ModelLabel = @ModelLabel,
          @EventKey = @EventKey,
          @Reason = @Reason;
  END;
GO
