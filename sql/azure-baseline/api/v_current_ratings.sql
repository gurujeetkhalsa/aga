-- Live Azure SQL metadata; database aga-membership; VIEW
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/*
Public lookup support objects.

Design rules:
- Expired members remain visible.
- Test accounts are excluded by AGAID < 50000.
- Ratings come from the MySQL-synced ratings.ratings table.
- The lookup API reads from curated views / procedures only.
*/

CREATE   VIEW [api].[v_current_ratings]
AS
WITH ranked AS
(
    SELECT
        r.[Pin_Player] AS [AGAID],
        r.[Rating],
        r.[Sigma],
        r.[Elab_Date],
        r.[id],
        ROW_NUMBER() OVER
        (
            PARTITION BY r.[Pin_Player]
            ORDER BY r.[Elab_Date] DESC, r.[id] DESC
        ) AS rn
    FROM [ratings].[ratings] AS r
    WHERE r.[Pin_Player] IS NOT NULL
)
SELECT
    [AGAID],
    [Rating],
    [Sigma],
    [Elab_Date]
FROM ranked
WHERE rn = 1;
GO
