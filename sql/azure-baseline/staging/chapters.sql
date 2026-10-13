-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [staging].[chapters] (
    [ChapterID] int NULL,
    [ChapterCode] nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ChapterName] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [City] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [State] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ChapterRepID] int NULL,
    [CreatedDate] datetime NULL,
    [Status] nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 );
GO
