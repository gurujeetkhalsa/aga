SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'api')
BEGIN
    EXEC(N'CREATE SCHEMA [api]');
END;
GO

/*
Public lookup support objects.

Design rules:
- Expired members remain visible.
- Test accounts are excluded by AGAID < 50000.
- Ratings come from the MySQL-synced ratingsync.ratings table.
- The lookup API reads from curated views / procedures only.
*/

CREATE OR ALTER VIEW [api].[v_current_ratings]
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
    FROM [ratingsync].[ratings] AS r
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

CREATE OR ALTER VIEW [api].[v_member_lookup]
AS
SELECT
    m.[AGAID],
    m.[FirstName],
    m.[LastName],
    CONCAT(m.[LastName], N', ', m.[FirstName]) AS [DisplayName],
    cr.[Rating],
    cr.[Sigma],
    cr.[Elab_Date] AS [RatingDate],
    m.[MemberType],
    m.[ExpirationDate],
    c.[ChapterCode],
    c.[ChapterName],
    m.[State]
FROM [membership].[members] AS m
LEFT JOIN [membership].[chapters] AS c
    ON c.[ChapterID] = m.[ChapterID]
LEFT JOIN [api].[v_current_ratings] AS cr
    ON cr.[AGAID] = m.[AGAID]
WHERE m.[AGAID] < 50000;
GO

CREATE OR ALTER PROCEDURE [api].[sp_lookup_members]
    @AGAID INT = NULL,
    @LastNamePrefix NVARCHAR(100) = NULL,
    @FirstNamePrefix NVARCHAR(100) = NULL,
    @MaxRows INT = 100
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    SET @LastNamePrefix = NULLIF(LTRIM(RTRIM(@LastNamePrefix)), N'');
    SET @FirstNamePrefix = NULLIF(LTRIM(RTRIM(@FirstNamePrefix)), N'');

    IF @AGAID IS NULL
       AND @LastNamePrefix IS NULL
       AND @FirstNamePrefix IS NULL
    BEGIN
        THROW 51001, 'At least one search parameter is required.', 1;
    END;

    SET @MaxRows = CASE
        WHEN @MaxRows IS NULL OR @MaxRows < 1 THEN 100
        WHEN @MaxRows > 100 THEN 100
        ELSE @MaxRows
    END;

    SELECT TOP (@MaxRows)
        [AGAID],
        [FirstName],
        [LastName],
        [DisplayName],
        [Rating],
        [Sigma],
        [RatingDate],
        [MemberType],
        [ExpirationDate],
        [ChapterCode],
        [ChapterName],
        [State]
    FROM [api].[v_member_lookup]
    WHERE (@AGAID IS NULL OR [AGAID] = @AGAID)
      AND (@LastNamePrefix IS NULL OR [LastName] LIKE @LastNamePrefix + N'%')
      AND (@FirstNamePrefix IS NULL OR [FirstName] LIKE @FirstNamePrefix + N'%')
    ORDER BY [LastName], [FirstName], [AGAID];
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_members_Lookup_Last_First'
      AND object_id = OBJECT_ID(N'[membership].[members]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_members_Lookup_Last_First]
        ON [membership].[members] ([LastName] ASC, [FirstName] ASC)
        INCLUDE ([AGAID], [MemberType], [ExpirationDate], [ChapterID], [State]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_members_Lookup_First_Last'
      AND object_id = OBJECT_ID(N'[membership].[members]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_members_Lookup_First_Last]
        ON [membership].[members] ([FirstName] ASC, [LastName] ASC)
        INCLUDE ([AGAID], [MemberType], [ExpirationDate], [ChapterID], [State]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_ratings_Current_ByPlayer'
      AND object_id = OBJECT_ID(N'[ratingsync].[ratings]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ratings_Current_ByPlayer]
        ON [ratingsync].[ratings] ([Pin_Player] ASC, [Elab_Date] DESC, [id] DESC)
        INCLUDE ([Rating], [Sigma]);
END;
GO

/*
Suggested security model after deployment:

GRANT SELECT ON OBJECT::[api].[v_member_lookup] TO [lookup_reader];
GRANT EXECUTE ON OBJECT::[api].[sp_lookup_members] TO [lookup_reader];

Prefer granting EXECUTE on the procedure only to the Function app identity/login.
*/
