-- Live Azure SQL metadata; database aga-membership; SQL_STORED_PROCEDURE
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   PROCEDURE [competition].[sp_mark_obvious_duplicate_review]
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
