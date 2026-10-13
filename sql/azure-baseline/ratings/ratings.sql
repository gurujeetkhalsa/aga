-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[ratings] (
    [Pin_Player] int NULL,
    [Rating] float NULL,
    [Sigma] float NULL,
    [Elab_Date] date NULL,
    [Tournament_Code] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [id] int NOT NULL
 );
GO
CREATE UNIQUE NONCLUSTERED INDEX [UX_ratings_id] ON [ratings].[ratings] ([id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_ratings_Current_ByPlayer] ON [ratings].[ratings] ([Pin_Player] ASC, [Elab_Date] DESC, [id] DESC) INCLUDE ([Rating], [Sigma], [Tournament_Code]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
