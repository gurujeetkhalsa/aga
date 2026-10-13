-- Live Azure SQL metadata; database aga-membership; VIEW
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE   VIEW [ratings].[vw_legacy_bayrate_snapshots_source]
AS
SELECT r.[Pin_Player] AS [Player_ID], r.[Rating], r.[Sigma], CAST(r.[Elab_Date] AS date) AS [Elab_Date], r.[Tournament_Code], r.[id] AS [Row_ID]
FROM [ratings].[ratings] AS r;

GO
