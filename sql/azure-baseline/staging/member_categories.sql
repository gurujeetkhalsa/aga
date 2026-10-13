-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [staging].[member_categories] (
    [AGAID] int NOT NULL,
    [Category] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL
 );
GO
CREATE NONCLUSTERED INDEX [IX_staging_member_categories_AGAID] ON [staging].[member_categories] ([AGAID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
