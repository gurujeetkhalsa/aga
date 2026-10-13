-- Copyright 2026, American Go Association, All rights reserved

-- Live Azure SQL stored procedure export.
-- Source object: [competition].[sp_mark_obvious_duplicate_review].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [competition].[sp_mark_obvious_duplicate_review]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    UPDATE [competition].[game_duplicate_review]
    SET [Status] = N'likely_duplicate',
        [ReviewerNote] = N'Auto-marked: exact early duplicate group with duplicate count >= 5 and no excluded rows; highly likely repeated-import artifact.',
        [LastRefreshed] = SYSDATETIME()
    WHERE [ReviewType] = N'exact_early'
      AND [DuplicateCount] >= 5
      AND [ExcludedCount] = 0
      AND [Status] = N'pending';
END;
GO
