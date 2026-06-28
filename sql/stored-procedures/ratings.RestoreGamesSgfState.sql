-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[RestoreGamesSgfState].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [ratings].[RestoreGamesSgfState]
    @PreviewOnly BIT = 0,
    @ClearSavedStateAfterRestore BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'tempdb..#RestorePlan', N'U') IS NOT NULL
    BEGIN
        DROP TABLE #RestorePlan;
    END;

    SELECT
        g.[Game_ID],
        Current_Sgf_Code = LTRIM(RTRIM(CONVERT(NVARCHAR(2048), g.[Sgf_Code]))),
        Saved_Sgf_Code = b.[Sgf_Code],
        Planned_Sgf_Code = b.[Sgf_Code],
        Planned_Action =
            CASE
                WHEN b.[Game_ID] IS NOT NULL
                    THEN N'restore_saved'
                WHEN COALESCE(LTRIM(RTRIM(CONVERT(NVARCHAR(2048), g.[Sgf_Code]))), N'') <> N''
                    THEN N'clear_current'
                ELSE N'none'
            END
    INTO #RestorePlan
    FROM [ratings].[games] AS g
    LEFT JOIN [ratings].[GameSgfStateBackup] AS b
        ON b.[Game_ID] = g.[Game_ID];

    IF @PreviewOnly = 1
    BEGIN
        SELECT
            [Planned_Action] AS [planned_action],
            COUNT(*) AS [row_count]
        FROM #RestorePlan
        GROUP BY [Planned_Action]
        ORDER BY [planned_action];

        SELECT
            [Game_ID],
            [Current_Sgf_Code],
            [Saved_Sgf_Code],
            [Planned_Sgf_Code],
            [Planned_Action]
        FROM #RestorePlan
        WHERE [Planned_Action] <> N'none'
        ORDER BY [Game_ID];

        RETURN;
    END;

    BEGIN TRANSACTION;

    UPDATE g
    SET [Sgf_Code] =
        CASE
            WHEN p.[Planned_Sgf_Code] IS NULL THEN NULL
            ELSE p.[Planned_Sgf_Code]
        END
    FROM [ratings].[games] AS g
    INNER JOIN #RestorePlan AS p
        ON p.[Game_ID] = g.[Game_ID]
    WHERE p.[Planned_Action] IN (N'restore_saved', N'clear_current');

    IF @ClearSavedStateAfterRestore = 1
    BEGIN
        DELETE FROM [ratings].[GameSgfStateBackup];
    END;

    COMMIT TRANSACTION;

    SELECT
        [Planned_Action] AS [applied_action],
        COUNT(*) AS [row_count]
    FROM #RestorePlan
    WHERE [Planned_Action] IN (N'restore_saved', N'clear_current')
    GROUP BY [Planned_Action]
    ORDER BY [applied_action];
END;
GO
