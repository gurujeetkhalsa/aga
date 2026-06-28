-- Live Azure SQL stored procedure export.
-- Source object: [api].[sp_lookup_members].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [api].[sp_lookup_members]
    @AGAID INT = NULL,
    @LastNamePrefix NVARCHAR(100) = NULL,
    @FirstNamePrefix NVARCHAR(100) = NULL,
    @MaxRows INT = 100,
    @OffsetRows INT = 0
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

    SET @OffsetRows = CASE
        WHEN @OffsetRows IS NULL OR @OffsetRows < 0 THEN 0
        ELSE @OffsetRows
    END;

    SELECT
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
    ORDER BY [LastName], [FirstName], [AGAID]
    OFFSET @OffsetRows ROWS FETCH NEXT @MaxRows ROWS ONLY;
END;
GO
