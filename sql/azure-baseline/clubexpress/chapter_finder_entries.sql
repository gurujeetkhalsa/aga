-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[chapter_finder_entries] (
    [Chapter_Finder_Entry_ID] bigint IDENTITY(1,1) NOT NULL,
    [Club_ID] int NOT NULL CONSTRAINT [DF_chapter_finder_entries_Club_ID] DEFAULT ((454497)),
    [Chapter_Finder_ID] int NOT NULL,
    [ClubExpress_Chapter_ID] int NULL,
    [Chapter_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Marker_Name] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Latitude] decimal(10,7) NULL,
    [Longitude] decimal(10,7) NULL,
    [Map_Icon] int NULL,
    [On_ClubExpress] bit NOT NULL CONSTRAINT [DF_chapter_finder_entries_On_ClubExpress] DEFAULT ((1)),
    [Address_Text] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Postal_Text] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [City] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [State_Province] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Postal_Code] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Country] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Contact_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Contact_Email] nvarchar(320) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Contact_Phone] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Website_Url] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Website_Label] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Description_Text] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Organization_Parent] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Org_Chart_Url] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Raw_Point_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Raw_Detail_Html] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Content_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [First_Seen_Run_ID] bigint NOT NULL,
    [Last_Seen_Run_ID] bigint NOT NULL,
    [First_Seen_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_entries_First_Seen_At] DEFAULT (sysutcdatetime()),
    [Last_Seen_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_entries_Last_Seen_At] DEFAULT (sysutcdatetime()),
    [Last_Changed_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_entries_Last_Changed_At] DEFAULT (sysutcdatetime()),
    [Is_Current] bit NOT NULL CONSTRAINT [DF_chapter_finder_entries_Is_Current] DEFAULT ((1)),
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_entries_Created_At] DEFAULT (sysutcdatetime()),
    [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_entries_Updated_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [clubexpress].[chapter_finder_entries] ADD CONSTRAINT [PK_chapter_finder_entries] PRIMARY KEY CLUSTERED ([Chapter_Finder_Entry_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_entries] ADD CONSTRAINT [UQ_chapter_finder_entries_Club_Finder] UNIQUE NONCLUSTERED ([Club_ID] ASC, [Chapter_Finder_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_finder_entries_ClubExpress_Chapter_ID] ON [clubexpress].[chapter_finder_entries] ([ClubExpress_Chapter_ID] ASC) INCLUDE ([Chapter_Name], [Is_Current]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_finder_entries_State_City] ON [clubexpress].[chapter_finder_entries] ([State_Province] ASC, [City] ASC, [Chapter_Name] ASC) INCLUDE ([Chapter_Finder_ID], [ClubExpress_Chapter_ID], [Is_Current]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_entries] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_entries_Raw_Point_Json] CHECK ([Raw_Point_Json] IS NULL OR isjson([Raw_Point_Json])=(1));
GO
ALTER TABLE [clubexpress].[chapter_finder_entries] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_entries_First_Run] FOREIGN KEY ([First_Seen_Run_ID]) REFERENCES [clubexpress].[chapter_finder_sync_runs] ([Chapter_Finder_Sync_Run_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [clubexpress].[chapter_finder_entries] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_entries_Last_Run] FOREIGN KEY ([Last_Seen_Run_ID]) REFERENCES [clubexpress].[chapter_finder_sync_runs] ([Chapter_Finder_Sync_Run_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
