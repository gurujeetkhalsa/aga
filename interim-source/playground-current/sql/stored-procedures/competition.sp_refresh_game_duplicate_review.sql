-- Copyright 2026, American Go Association, All rights reserved

-- Live Azure SQL stored procedure export.
-- Source object: [competition].[sp_refresh_game_duplicate_review].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [competition].[sp_refresh_game_duplicate_review]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Now DATETIME2(7) = SYSDATETIME();

    WITH source_rows AS
    (
        SELECT
            ReviewType = N'exact_early',
            ReviewKey = CONCAT(
                N'exact_early|',
                CONVERT(NVARCHAR(10), Game_Date, 23), N'|', COALESCE(Tournament_Code, '<NULL>'), N'|',
                COALESCE(CONVERT(NVARCHAR(20), Pin_Player_1), '<NULL>'), N'|', COALESCE(Color_1, '<NULL>'), N'|',
                COALESCE(CONVERT(NVARCHAR(20), Pin_Player_2), '<NULL>'), N'|', COALESCE(Color_2, '<NULL>'), N'|',
                COALESCE(CONVERT(NVARCHAR(20), Handicap), '<NULL>'), N'|', COALESCE(CONVERT(NVARCHAR(20), Komi), '<NULL>'), N'|', COALESCE(Result, '<NULL>')
            ),
            Game_Date,
            Tournament_Code,
            PlayerLowAGAID = CAST(NULL AS INT),
            PlayerHighAGAID = CAST(NULL AS INT),
            Pin_Player_1,
            Color_1,
            Pin_Player_2,
            Color_2,
            Handicap,
            Komi,
            Result,
            DuplicateCount,
            ExcludedCount,
            MinGameID,
            MaxGameID
        FROM
        (
            SELECT
                Game_Date,
                Tournament_Code,
                Pin_Player_1,
                Color_1,
                Pin_Player_2,
                Color_2,
                Handicap,
                Komi,
                Result,
                COUNT(*) AS DuplicateCount,
                SUM(CASE WHEN [Exclude] = 1 THEN 1 ELSE 0 END) AS ExcludedCount,
                MIN(Game_ID) AS MinGameID,
                MAX(Game_ID) AS MaxGameID
            FROM [ratingsync].[games]
            WHERE Game_Date < '2000-01-01'
            GROUP BY
                Game_Date,
                Tournament_Code,
                Pin_Player_1,
                Color_1,
                Pin_Player_2,
                Color_2,
                Handicap,
                Komi,
                Result
            HAVING COUNT(*) > 1
        ) AS exact_rows

        UNION ALL

        SELECT
            ReviewType = N'pair_early',
            ReviewKey = CONCAT(
                N'pair_early|',
                CONVERT(NVARCHAR(10), Game_Date, 23), N'|', COALESCE(Tournament_Code, '<NULL>'), N'|',
                COALESCE(CONVERT(NVARCHAR(20), PlayerLowAGAID), '<NULL>'), N'|', COALESCE(CONVERT(NVARCHAR(20), PlayerHighAGAID), '<NULL>'), N'|',
                COALESCE(CONVERT(NVARCHAR(20), Handicap), '<NULL>'), N'|', COALESCE(CONVERT(NVARCHAR(20), Komi), '<NULL>'), N'|', COALESCE(Result, '<NULL>')
            ),
            Game_Date,
            Tournament_Code,
            PlayerLowAGAID,
            PlayerHighAGAID,
            Pin_Player_1 = CAST(NULL AS INT),
            Color_1 = CAST(NULL AS NVARCHAR(20)),
            Pin_Player_2 = CAST(NULL AS INT),
            Color_2 = CAST(NULL AS NVARCHAR(20)),
            Handicap,
            Komi,
            Result,
            DuplicateCount,
            ExcludedCount,
            MinGameID = CAST(NULL AS INT),
            MaxGameID = CAST(NULL AS INT)
        FROM
        (
            SELECT
                Game_Date,
                Tournament_Code,
                CASE WHEN Pin_Player_1 < Pin_Player_2 THEN Pin_Player_1 ELSE Pin_Player_2 END AS PlayerLowAGAID,
                CASE WHEN Pin_Player_1 < Pin_Player_2 THEN Pin_Player_2 ELSE Pin_Player_1 END AS PlayerHighAGAID,
                Handicap,
                Komi,
                Result,
                COUNT(*) AS DuplicateCount,
                SUM(CASE WHEN [Exclude] = 1 THEN 1 ELSE 0 END) AS ExcludedCount
            FROM [ratingsync].[games]
            WHERE Game_Date < '2000-01-01'
            GROUP BY
                Game_Date,
                Tournament_Code,
                CASE WHEN Pin_Player_1 < Pin_Player_2 THEN Pin_Player_1 ELSE Pin_Player_2 END,
                CASE WHEN Pin_Player_1 < Pin_Player_2 THEN Pin_Player_2 ELSE Pin_Player_1 END,
                Handicap,
                Komi,
                Result
            HAVING COUNT(*) > 1
        ) AS pair_rows
    )
    MERGE [competition].[game_duplicate_review] AS target
    USING source_rows AS source
        ON target.[ReviewType] = source.[ReviewType]
       AND target.[ReviewKey] = source.[ReviewKey]
    WHEN MATCHED THEN
        UPDATE SET
            [GameDate] = source.[Game_Date],
            [TournamentCode] = source.[Tournament_Code],
            [PlayerLowAGAID] = source.[PlayerLowAGAID],
            [PlayerHighAGAID] = source.[PlayerHighAGAID],
            [Pin_Player_1] = source.[Pin_Player_1],
            [Color_1] = source.[Color_1],
            [Pin_Player_2] = source.[Pin_Player_2],
            [Color_2] = source.[Color_2],
            [Handicap] = source.[Handicap],
            [Komi] = source.[Komi],
            [Result] = source.[Result],
            [DuplicateCount] = source.[DuplicateCount],
            [ExcludedCount] = source.[ExcludedCount],
            [MinGameID] = source.[MinGameID],
            [MaxGameID] = source.[MaxGameID],
            [LastRefreshed] = @Now
    WHEN NOT MATCHED THEN
        INSERT
        (
            [ReviewType], [ReviewKey], [GameDate], [TournamentCode], [PlayerLowAGAID], [PlayerHighAGAID],
            [Pin_Player_1], [Color_1], [Pin_Player_2], [Color_2], [Handicap], [Komi], [Result],
            [DuplicateCount], [ExcludedCount], [MinGameID], [MaxGameID], [Status], [ReviewerNote],
            [CanonicalGameID], [CreatedDate], [LastRefreshed]
        )
        VALUES
        (
            source.[ReviewType], source.[ReviewKey], source.[Game_Date], source.[Tournament_Code], source.[PlayerLowAGAID], source.[PlayerHighAGAID],
            source.[Pin_Player_1], source.[Color_1], source.[Pin_Player_2], source.[Color_2], source.[Handicap], source.[Komi], source.[Result],
            source.[DuplicateCount], source.[ExcludedCount], source.[MinGameID], source.[MaxGameID], N'pending', NULL,
            NULL, @Now, @Now
        )
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
END;
GO
