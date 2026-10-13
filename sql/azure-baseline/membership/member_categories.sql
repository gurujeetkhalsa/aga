-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [membership].[member_categories] (
    [AGAID] int NOT NULL,
    [CategoryID] int NOT NULL,
    [CreatedDate] datetime2(7) NOT NULL CONSTRAINT [DF_member_categories_CreatedDate] DEFAULT (sysdatetime()),
    [LastUpdated] datetime2(7) NOT NULL CONSTRAINT [DF_member_categories_LastUpdated] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [membership].[member_categories] ADD CONSTRAINT [PK_member_categories] PRIMARY KEY CLUSTERED ([AGAID] ASC, [CategoryID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [membership].[member_categories] WITH CHECK ADD CONSTRAINT [FK_member_categories_member] FOREIGN KEY ([AGAID]) REFERENCES [membership].[members] ([AGAID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [membership].[member_categories] WITH CHECK ADD CONSTRAINT [FK_member_categories_category] FOREIGN KEY ([CategoryID]) REFERENCES [membership].[categories] ([CategoryID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
