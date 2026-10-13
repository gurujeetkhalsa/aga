SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

/*
Preserve and restore trusted ratings.games.Sgf_Code values across temporary
reloads from an external source.

Typical flow:

1. Run [ratings].[SaveGamesSgfState] before reloading [ratings].[games].
2. Reload [ratings].[games] from the external source.
3. Run [ratings].[RestoreGamesSgfState] to clear all current SGF values and
   restore the saved trusted values by Game_ID.

Notes:
- This utility does not touch blob storage.
- It assumes Game_ID remains stable across the reload.
- Blob storage remains unchanged; only [ratings].[games].[Sgf_Code] is touched.
*/

IF OBJECT_ID(N'[ratings].[GameSgfStateBackup]', N'U') IS NULL
BEGIN
    CREATE TABLE [ratings].[GameSgfStateBackup]
    (
        [Game_ID] INT NOT NULL,
        [Sgf_Code] NVARCHAR(2048) NOT NULL,
        [Saved_At] DATETIME2(0) NOT NULL
            CONSTRAINT [DF_GameSgfStateBackup_Saved_At] DEFAULT SYSUTCDATETIME(),
        [Save_Run_Id] UNIQUEIDENTIFIER NOT NULL,
        [Saved_By] SYSNAME NULL,
        [Save_Note] NVARCHAR(400) NULL,
        CONSTRAINT [PK_GameSgfStateBackup] PRIMARY KEY CLUSTERED ([Game_ID])
    );
END;
GO

CREATE OR ALTER PROCEDURE [ratings].[SaveGamesSgfState]
    @ReplaceExisting BIT = 1,
    @OnlyWhenSgfCodeLike NVARCHAR(4000) = NULL,
    @SkipWhenSgfCodeLike NVARCHAR(4000) = NULL,
    @SavedBy SYSNAME = NULL,
    @SaveNote NVARCHAR(400) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @SaveRunId UNIQUEIDENTIFIER = NEWID();

    ;WITH [SourceRows] AS
    (
        SELECT
            g.[Game_ID],
            LTRIM(RTRIM(CONVERT(NVARCHAR(2048), g.[Sgf_Code]))) AS [Sgf_Code]
        FROM [ratings].[games] AS g
        WHERE COALESCE(LTRIM(RTRIM(CONVERT(NVARCHAR(2048), g.[Sgf_Code]))), N'') <> N''
          AND (@OnlyWhenSgfCodeLike IS NULL OR CONVERT(NVARCHAR(2048), g.[Sgf_Code]) LIKE @OnlyWhenSgfCodeLike)
          AND (@SkipWhenSgfCodeLike IS NULL OR CONVERT(NVARCHAR(2048), g.[Sgf_Code]) NOT LIKE @SkipWhenSgfCodeLike)
    )
    MERGE [ratings].[GameSgfStateBackup] AS target
    USING [SourceRows] AS source
        ON target.[Game_ID] = source.[Game_ID]
    WHEN MATCHED AND @ReplaceExisting = 1 THEN
        UPDATE SET
            target.[Sgf_Code] = source.[Sgf_Code],
            target.[Saved_At] = SYSUTCDATETIME(),
            target.[Save_Run_Id] = @SaveRunId,
            target.[Saved_By] = COALESCE(@SavedBy, SUSER_SNAME()),
            target.[Save_Note] = @SaveNote
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ([Game_ID], [Sgf_Code], [Saved_At], [Save_Run_Id], [Saved_By], [Save_Note])
        VALUES
        (
            source.[Game_ID],
            source.[Sgf_Code],
            SYSUTCDATETIME(),
            @SaveRunId,
            COALESCE(@SavedBy, SUSER_SNAME()),
            @SaveNote
        );

    SELECT
        @SaveRunId AS [save_run_id],
        COUNT(*) AS [saved_row_count]
    FROM [ratings].[GameSgfStateBackup]
    WHERE [Save_Run_Id] = @SaveRunId;
END;
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

/*
Example usage
-------------

-- Save all current nonblank SGF values:
EXEC [ratings].[SaveGamesSgfState]
    @SaveNote = N'Before external ratings.games reload';

-- Preview the clear-and-restore pass:
EXEC [ratings].[RestoreGamesSgfState]
    @PreviewOnly = 1;

-- Apply the clear-and-restore pass:
EXEC [ratings].[RestoreGamesSgfState];
*/
