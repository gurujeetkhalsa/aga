-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [membership].[categories] (
    [CategoryID] int IDENTITY(1,1) NOT NULL,
    [CategoryName] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [CreatedDate] datetime2(7) NOT NULL CONSTRAINT [DF_categories_CreatedDate] DEFAULT (sysdatetime()),
    [LastUpdated] datetime2(7) NOT NULL CONSTRAINT [DF_categories_LastUpdated] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [membership].[categories] ADD CONSTRAINT [PK_categories] PRIMARY KEY CLUSTERED ([CategoryID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [membership].[categories] ADD CONSTRAINT [UQ_categories_CategoryName] UNIQUE NONCLUSTERED ([CategoryName] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
