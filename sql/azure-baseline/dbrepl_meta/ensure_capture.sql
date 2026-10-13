-- Live Azure SQL metadata; database aga-membership; SQL_STORED_PROCEDURE
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   PROCEDURE dbrepl_meta.ensure_capture
WITH EXECUTE AS OWNER
AS
BEGIN
 SET NOCOUNT ON;
 DECLARE @id int, @schema sysname, @name sysname, @qualified nvarchar(520),
 @trigger sysname, @sql nvarchar(max), @pk bit;
 DECLARE @errors TABLE(schema_name sysname,table_name sysname,error_message nvarchar(4000));
 DECLARE objects CURSOR LOCAL FAST_FORWARD FOR
 SELECT t.object_id,s.name,t.name,CAST(CASE WHEN EXISTS(SELECT 1 FROM sys.indexes i WHERE i.object_id=t.object_id AND i.is_primary_key=1) THEN 1 ELSE 0 END AS bit)
 FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id
 WHERE t.is_ms_shipped=0 AND s.name<>N'dbrepl_meta'
 AND NOT(s.name=N'integration' AND t.name=N'news_archive_article_member_match');
 OPEN objects;
 FETCH NEXT FROM objects INTO @id,@schema,@name,@pk;
 WHILE @@FETCH_STATUS=0
 BEGIN
  BEGIN TRY
   SET @qualified=QUOTENAME(@schema)+N'.'+QUOTENAME(@name);
   IF @pk=1
   BEGIN
    IF NOT EXISTS(SELECT 1 FROM sys.change_tracking_tables WHERE object_id=@id)
    BEGIN
     SET @sql=N'ALTER TABLE '+@qualified+N' ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED=OFF)';
     EXEC sp_executesql @sql;
    END
   END
   ELSE
   BEGIN
    IF NOT EXISTS(SELECT 1 FROM dbrepl_meta.generations WHERE object_id=@id)
     INSERT dbrepl_meta.generations(object_id) VALUES(@id);
    SET @trigger=N'dbrepl_dirty_'+CONVERT(nvarchar(20),@id);
    IF NOT EXISTS(SELECT 1 FROM sys.triggers WHERE parent_id=@id AND name=@trigger AND is_disabled=0)
    BEGIN
     SET @sql=N'CREATE OR ALTER TRIGGER '+QUOTENAME(@schema)+N'.'+QUOTENAME(@trigger)+N' ON '+@qualified+N' WITH EXECUTE AS OWNER AFTER INSERT, UPDATE, DELETE AS BEGIN IF ROWCOUNT_BIG()=0 RETURN; SET NOCOUNT ON; UPDATE dbrepl_meta.generations SET generation=generation+1 WHERE object_id='+CONVERT(nvarchar(20),@id)+N'; END;';
     EXEC sp_executesql @sql;
     SET @sql=N'ENABLE TRIGGER '+QUOTENAME(@schema)+N'.'+QUOTENAME(@trigger)+N' ON '+@qualified;
     EXEC sp_executesql @sql;
     UPDATE dbrepl_meta.generations SET incarnation=NEWID(),generation=generation+1 WHERE object_id=@id;
    END
   END
   IF NOT EXISTS(SELECT 1 FROM sys.database_permissions WHERE grantee_principal_id=USER_ID(N'aga-dbrepl') AND class=3 AND major_id=SCHEMA_ID(@schema) AND permission_name=N'VIEW CHANGE TRACKING' AND state='G')
   BEGIN
    SET @sql=N'GRANT VIEW CHANGE TRACKING ON SCHEMA::'+QUOTENAME(@schema)+N' TO [aga-dbrepl]';
    EXEC sp_executesql @sql;
   END
  END TRY
  BEGIN CATCH
   INSERT @errors VALUES(@schema,@name,ERROR_MESSAGE());
  END CATCH;
  FETCH NEXT FROM objects INTO @id,@schema,@name,@pk;
 END
 CLOSE objects;
 DEALLOCATE objects;
 -- Block inherited public EXECUTE grants in application schemas, while allowing
 -- only this helper in its dedicated schema.
 SET @sql=N'';
 SELECT @sql=@sql+N'DENY EXECUTE ON SCHEMA::'+QUOTENAME(s.name)+N' TO [aga-dbrepl];'
 FROM sys.schemas s WHERE s.name NOT IN(N'dbrepl_meta',N'sys',N'INFORMATION_SCHEMA')
 AND NOT EXISTS(SELECT 1 FROM sys.database_permissions p WHERE p.grantee_principal_id=USER_ID(N'aga-dbrepl') AND p.class=3 AND p.major_id=s.schema_id AND p.permission_name=N'EXECUTE' AND p.state='D');
 IF LEN(@sql)>0 EXEC sp_executesql @sql;
 SELECT * FROM @errors;
END;
GO
