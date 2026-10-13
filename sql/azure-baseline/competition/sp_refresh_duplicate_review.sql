-- Live Azure SQL metadata; database aga-membership; SQL_STORED_PROCEDURE
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   PROCEDURE [competition].[sp_refresh_duplicate_review]
AS
BEGIN
    SET NOCOUNT ON;
    EXEC [competition].[sp_refresh_tournament_duplicate_review];
    EXEC [competition].[sp_refresh_game_duplicate_review];
END;
GO
