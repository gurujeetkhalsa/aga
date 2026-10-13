-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[GameSgfStateBackup] (
    [Game_ID] int NOT NULL,
    [Sgf_Code] nvarchar(2048) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Saved_At] datetime2(0) NOT NULL CONSTRAINT [DF_GameSgfStateBackup_Saved_At] DEFAULT (sysutcdatetime()),
    [Save_Run_Id] uniqueidentifier NOT NULL,
    [Saved_By] sysname COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Save_Note] nvarchar(400) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 );
GO
ALTER TABLE [ratings].[GameSgfStateBackup] ADD CONSTRAINT [PK_GameSgfStateBackup] PRIMARY KEY CLUSTERED ([Game_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
