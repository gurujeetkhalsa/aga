SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [membership].[sp_find_invalid_youth_members]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Today DATE = CAST(SYSDATETIME() AS DATE);

    SELECT
        m.[AGAID],
        m.[FirstName],
        m.[LastName],
        m.[MemberType],
        m.[DateOfBirth],
	m.[ExpirationDate],
	m.[Status],
        AgeYears =
            CASE
                WHEN m.[DateOfBirth] IS NULL THEN NULL
                ELSE DATEDIFF(YEAR, m.[DateOfBirth], @Today)
                    - CASE
                        WHEN DATEADD(YEAR, DATEDIFF(YEAR, m.[DateOfBirth], @Today), m.[DateOfBirth]) > @Today
                            THEN 1
                        ELSE 0
                      END
            END,
        Issue =
            CASE
                WHEN m.[DateOfBirth] IS NULL THEN N'Missing DateOfBirth'
                ELSE N'Older than 22'
            END
    FROM [membership].[members] AS m
    WHERE m.[MemberType] = N'Youth'
      AND
      (
          m.[DateOfBirth] IS NULL
          OR DATEADD(YEAR, 23, m.[DateOfBirth]) <= @Today
      )
    ORDER BY
        m.[LastName],
        m.[FirstName],
        m.[AGAID];
END
GO