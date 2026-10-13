-- Copyright 2026, American Go Association, All rights reserved

-- Live Azure SQL stored procedure export.
-- Source object: [competition].[sp_refresh_duplicate_review].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [competition].[sp_refresh_duplicate_review]
AS
BEGIN
    SET NOCOUNT ON;
    EXEC [competition].[sp_refresh_tournament_duplicate_review];
    EXEC [competition].[sp_refresh_game_duplicate_review];
END;
GO
