-- Copyright 2026, American Go Association, All rights reserved

-- Live Azure SQL stored procedure export.
-- Source object: [ratings].[SaveGamesSgfState].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
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
