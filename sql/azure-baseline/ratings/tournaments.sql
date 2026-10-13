-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[tournaments] (
    [Tournament_Code] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Tournament_Descr] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Tournament_Date] date NULL,
    [City] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [State_Code] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Country_Code] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Rounds] int NULL,
    [Total_Players] int NULL,
    [Wallist] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Elab_Date] date NULL,
    [status] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Host_ChapterID] int NULL,
    [Host_ChapterCode] nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Host_ChapterName] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Reward_Event_Key] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Reward_Event_Name] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Reward_Is_State_Championship] bit NOT NULL CONSTRAINT [DF_tournaments_Reward_Is_State_Championship] DEFAULT ((0))
 );
GO
CREATE UNIQUE NONCLUSTERED INDEX [UX_tournaments_Tournament_Code] ON [ratings].[tournaments] ([Tournament_Code] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_tournaments_Date_Code] ON [ratings].[tournaments] ([Tournament_Date] DESC, [Tournament_Code] ASC) INCLUDE ([Tournament_Descr], [City], [State_Code], [Country_Code], [Rounds], [Total_Players]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_tournaments_Host_Chapter] ON [ratings].[tournaments] ([Host_ChapterID] ASC, [Reward_Event_Key] ASC, [Tournament_Date] ASC) WHERE ([Host_ChapterID] IS NOT NULL) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
