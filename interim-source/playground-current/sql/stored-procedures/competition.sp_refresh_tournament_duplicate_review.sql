-- Copyright 2026, American Go Association, All rights reserved

-- Live Azure SQL stored procedure export.
-- Source object: [competition].[sp_refresh_tournament_duplicate_review].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [competition].[sp_refresh_tournament_duplicate_review]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Now DATETIME2(7) = SYSDATETIME();

    WITH source_rows AS
    (
        SELECT
            ReviewType = N'code_family',
            ReviewKey = CONCAT(N'code_family|', CodeBase),
            CodeBase,
            NormalizedName = CAST(NULL AS NVARCHAR(400)),
            TournamentDate,
            VariantCount,
            TournamentCodes,
            TournamentDescriptions,
            Locations = CAST(NULL AS NVARCHAR(MAX))
        FROM
        (
            SELECT
                CodeBase,
                MIN(Tournament_Date) AS TournamentDate,
                COUNT(*) AS VariantCount,
                STRING_AGG(Tournament_Code, ', ') WITHIN GROUP (ORDER BY Tournament_Code) AS TournamentCodes,
                STRING_AGG(COALESCE(Tournament_Descr, '<NULL>'), ' | ') WITHIN GROUP (ORDER BY Tournament_Code) AS TournamentDescriptions
            FROM
            (
                SELECT
                    Tournament_Code,
                    Tournament_Descr,
                    Tournament_Date,
                    CASE
                        WHEN Tournament_Code LIKE '%[a-z]' THEN LEFT(Tournament_Code, LEN(Tournament_Code) - 1)
                        ELSE Tournament_Code
                    END AS CodeBase
                FROM [ratingsync].[tournaments]
                WHERE Tournament_Date < '2000-01-01'
            ) AS family_source
            GROUP BY CodeBase
            HAVING COUNT(*) > 1
        ) AS code_family_rows

        UNION ALL

        SELECT
            ReviewType = N'normalized_name_date',
            ReviewKey = CONCAT(N'normalized_name_date|', NormalizedName, N'|', CONVERT(NVARCHAR(10), Tournament_Date, 23)),
            CodeBase = CAST(NULL AS NVARCHAR(255)),
            NormalizedName,
            Tournament_Date,
            TournamentCount,
            TournamentCodes,
            TournamentDescriptions = CAST(NULL AS NVARCHAR(MAX)),
            Locations
        FROM
        (
            SELECT
                NormalizedName,
                Tournament_Date,
                COUNT(*) AS TournamentCount,
                STRING_AGG(Tournament_Code, ', ') WITHIN GROUP (ORDER BY Tournament_Code) AS TournamentCodes,
                STRING_AGG(COALESCE(City, '<NULL>') + '/' + COALESCE(State_Code, '<NULL>'), ' | ') WITHIN GROUP (ORDER BY Tournament_Code) AS Locations
            FROM
            (
                SELECT
                    Tournament_Code,
                    Tournament_Date,
                    City,
                    State_Code,
                    UPPER(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    LTRIM(RTRIM(COALESCE(Tournament_Descr, ''))),
                                    ' ',
                                    ''
                                ),
                                '-',
                                ''
                            ),
                            '.',
                            ''
                        )
                    ) AS NormalizedName
                FROM [ratingsync].[tournaments]
                WHERE Tournament_Date < '2000-01-01'
            ) AS normalized_source
            WHERE NormalizedName <> ''
            GROUP BY NormalizedName, Tournament_Date
            HAVING COUNT(*) > 1
        ) AS normalized_rows
    )
    MERGE [competition].[tournament_duplicate_review] AS target
    USING source_rows AS source
        ON target.[ReviewType] = source.[ReviewType]
       AND target.[ReviewKey] = source.[ReviewKey]
    WHEN MATCHED THEN
        UPDATE SET
            [CodeBase] = source.[CodeBase],
            [NormalizedName] = source.[NormalizedName],
            [TournamentDate] = source.[TournamentDate],
            [VariantCount] = source.[VariantCount],
            [TournamentCodes] = source.[TournamentCodes],
            [TournamentDescriptions] = source.[TournamentDescriptions],
            [Locations] = source.[Locations],
            [LastRefreshed] = @Now
    WHEN NOT MATCHED THEN
        INSERT
        (
            [ReviewType], [ReviewKey], [CodeBase], [NormalizedName], [TournamentDate], [VariantCount],
            [TournamentCodes], [TournamentDescriptions], [Locations], [Status], [ReviewerNote],
            [CanonicalTournamentCode], [CreatedDate], [LastRefreshed]
        )
        VALUES
        (
            source.[ReviewType], source.[ReviewKey], source.[CodeBase], source.[NormalizedName], source.[TournamentDate], source.[VariantCount],
            source.[TournamentCodes], source.[TournamentDescriptions], source.[Locations], N'pending', NULL,
            NULL, @Now, @Now
        )
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
END;
GO
