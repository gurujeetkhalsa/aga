-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[chapter_finder_entry_versions] (
    [Chapter_Finder_Entry_Version_ID] bigint IDENTITY(1,1) NOT NULL,
    [Chapter_Finder_Entry_ID] bigint NOT NULL,
    [Chapter_Finder_Sync_Run_ID] bigint NOT NULL,
    [Change_Type] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Club_ID] int NOT NULL,
    [Chapter_Finder_ID] int NOT NULL,
    [ClubExpress_Chapter_ID] int NULL,
    [Chapter_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Marker_Name] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Latitude] decimal(10,7) NULL,
    [Longitude] decimal(10,7) NULL,
    [Map_Icon] int NULL,
    [On_ClubExpress] bit NOT NULL,
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
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_entry_versions_Created_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [clubexpress].[chapter_finder_entry_versions] ADD CONSTRAINT [PK_chapter_finder_entry_versions] PRIMARY KEY CLUSTERED ([Chapter_Finder_Entry_Version_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_finder_entry_versions_Entry_Created] ON [clubexpress].[chapter_finder_entry_versions] ([Chapter_Finder_Entry_ID] ASC, [Created_At] DESC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_entry_versions] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_entry_versions_Change_Type] CHECK ([Change_Type]=N'marked_inactive' OR [Change_Type]=N'updated' OR [Change_Type]=N'inserted');
GO
ALTER TABLE [clubexpress].[chapter_finder_entry_versions] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_entry_versions_Raw_Point_Json] CHECK ([Raw_Point_Json] IS NULL OR isjson([Raw_Point_Json])=(1));
GO
ALTER TABLE [clubexpress].[chapter_finder_entry_versions] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_entry_versions_Entry] FOREIGN KEY ([Chapter_Finder_Entry_ID]) REFERENCES [clubexpress].[chapter_finder_entries] ([Chapter_Finder_Entry_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [clubexpress].[chapter_finder_entry_versions] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_entry_versions_Run] FOREIGN KEY ([Chapter_Finder_Sync_Run_ID]) REFERENCES [clubexpress].[chapter_finder_sync_runs] ([Chapter_Finder_Sync_Run_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
