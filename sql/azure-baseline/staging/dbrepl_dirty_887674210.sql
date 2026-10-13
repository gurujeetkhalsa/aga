-- Live Azure SQL metadata; database aga-membership; SQL_TRIGGER
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   TRIGGER [staging].[dbrepl_dirty_887674210] ON [staging].[chapters] WITH EXECUTE AS OWNER AFTER INSERT, UPDATE, DELETE AS BEGIN IF ROWCOUNT_BIG()=0 RETURN; SET NOCOUNT ON; UPDATE dbrepl_meta.generations SET generation=generation+1 WHERE object_id=887674210; END;
GO
