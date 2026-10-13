-- Live Azure SQL metadata; database aga-membership; VIEW
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   VIEW [api].[v_member_lookup]
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
