-- Copyright 2026, American Go Association, All rights reserved

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'clubexpress') IS NULL
    EXEC(N'CREATE SCHEMA [clubexpress]');
GO

IF OBJECT_ID(N'clubexpress.chapter_finder_sync_runs', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[chapter_finder_sync_runs]
    (
        [Chapter_Finder_Sync_Run_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Run_Key] nvarchar(256) NOT NULL,
        [Run_Type] nvarchar(32) NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Run_Type] DEFAULT N'manual',
        [Source_Page_Url] nvarchar(1024) NOT NULL,
        [Started_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Started_At] DEFAULT SYSUTCDATETIME(),
        [Completed_At] datetime2(0) NULL,
        [Status] nvarchar(32) NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Status] DEFAULT N'running',
        [Point_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Point_Count] DEFAULT 0,
        [Detail_Success_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Detail_Success_Count] DEFAULT 0,
        [Detail_Failure_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Detail_Failure_Count] DEFAULT 0,
        [Inserted_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Inserted_Count] DEFAULT 0,
        [Updated_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Updated_Count] DEFAULT 0,
        [Unchanged_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Unchanged_Count] DEFAULT 0,
        [Marked_Inactive_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Marked_Inactive_Count] DEFAULT 0,
        [Summary_Json] nvarchar(max) NULL,
        [Error_Message] nvarchar(max) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_sync_runs_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_chapter_finder_sync_runs] PRIMARY KEY CLUSTERED ([Chapter_Finder_Sync_Run_ID]),
        CONSTRAINT [UQ_chapter_finder_sync_runs_Run_Key] UNIQUE ([Run_Key]),
        CONSTRAINT [CK_chapter_finder_sync_runs_Run_Type] CHECK ([Run_Type] IN (N'manual', N'scheduled', N'backfill')),
        CONSTRAINT [CK_chapter_finder_sync_runs_Status] CHECK ([Status] IN (N'running', N'succeeded', N'partial', N'failed', N'cancelled')),
        CONSTRAINT [CK_chapter_finder_sync_runs_Summary_Json] CHECK ([Summary_Json] IS NULL OR ISJSON([Summary_Json]) = 1)
    );
END;
GO

IF OBJECT_ID(N'clubexpress.chapter_finder_entries', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[chapter_finder_entries]
    (
        [Chapter_Finder_Entry_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Club_ID] int NOT NULL CONSTRAINT [DF_chapter_finder_entries_Club_ID] DEFAULT 454497,
        [Chapter_Finder_ID] int NOT NULL,
        [ClubExpress_Chapter_ID] int NULL,
        [Chapter_Name] nvarchar(256) NOT NULL,
        [Marker_Name] nvarchar(512) NULL,
        [Latitude] decimal(10, 7) NULL,
        [Longitude] decimal(10, 7) NULL,
        [Map_Icon] int NULL,
        [On_ClubExpress] bit NOT NULL CONSTRAINT [DF_chapter_finder_entries_On_ClubExpress] DEFAULT 1,
        [Address_Text] nvarchar(512) NULL,
        [Postal_Text] nvarchar(512) NULL,
        [City] nvarchar(128) NULL,
        [State_Province] nvarchar(64) NULL,
        [Postal_Code] nvarchar(32) NULL,
        [Country] nvarchar(128) NULL,
        [Contact_Name] nvarchar(256) NULL,
        [Contact_Email] nvarchar(320) NULL,
        [Contact_Phone] nvarchar(64) NULL,
        [Website_Url] nvarchar(1024) NULL,
        [Website_Label] nvarchar(512) NULL,
        [Description_Text] nvarchar(max) NULL,
        [Organization_Parent] nvarchar(256) NULL,
        [Org_Chart_Url] nvarchar(1024) NULL,
        [Raw_Point_Json] nvarchar(max) NULL,
        [Raw_Detail_Html] nvarchar(max) NULL,
        [Content_Sha256] char(64) NOT NULL,
        [First_Seen_Run_ID] bigint NOT NULL,
        [Last_Seen_Run_ID] bigint NOT NULL,
        [First_Seen_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_entries_First_Seen_At] DEFAULT SYSUTCDATETIME(),
        [Last_Seen_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_entries_Last_Seen_At] DEFAULT SYSUTCDATETIME(),
        [Last_Changed_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_entries_Last_Changed_At] DEFAULT SYSUTCDATETIME(),
        [Is_Current] bit NOT NULL CONSTRAINT [DF_chapter_finder_entries_Is_Current] DEFAULT 1,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_entries_Created_At] DEFAULT SYSUTCDATETIME(),
        [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_entries_Updated_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_chapter_finder_entries] PRIMARY KEY CLUSTERED ([Chapter_Finder_Entry_ID]),
        CONSTRAINT [UQ_chapter_finder_entries_Club_Finder] UNIQUE ([Club_ID], [Chapter_Finder_ID]),
        CONSTRAINT [CK_chapter_finder_entries_Raw_Point_Json] CHECK ([Raw_Point_Json] IS NULL OR ISJSON([Raw_Point_Json]) = 1),
        CONSTRAINT [FK_chapter_finder_entries_First_Run] FOREIGN KEY ([First_Seen_Run_ID]) REFERENCES [clubexpress].[chapter_finder_sync_runs] ([Chapter_Finder_Sync_Run_ID]),
        CONSTRAINT [FK_chapter_finder_entries_Last_Run] FOREIGN KEY ([Last_Seen_Run_ID]) REFERENCES [clubexpress].[chapter_finder_sync_runs] ([Chapter_Finder_Sync_Run_ID])
    );
END;
GO

IF OBJECT_ID(N'clubexpress.chapter_finder_entry_versions', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[chapter_finder_entry_versions]
    (
        [Chapter_Finder_Entry_Version_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Chapter_Finder_Entry_ID] bigint NOT NULL,
        [Chapter_Finder_Sync_Run_ID] bigint NOT NULL,
        [Change_Type] nvarchar(32) NOT NULL,
        [Club_ID] int NOT NULL,
        [Chapter_Finder_ID] int NOT NULL,
        [ClubExpress_Chapter_ID] int NULL,
        [Chapter_Name] nvarchar(256) NOT NULL,
        [Marker_Name] nvarchar(512) NULL,
        [Latitude] decimal(10, 7) NULL,
        [Longitude] decimal(10, 7) NULL,
        [Map_Icon] int NULL,
        [On_ClubExpress] bit NOT NULL,
        [Address_Text] nvarchar(512) NULL,
        [Postal_Text] nvarchar(512) NULL,
        [City] nvarchar(128) NULL,
        [State_Province] nvarchar(64) NULL,
        [Postal_Code] nvarchar(32) NULL,
        [Country] nvarchar(128) NULL,
        [Contact_Name] nvarchar(256) NULL,
        [Contact_Email] nvarchar(320) NULL,
        [Contact_Phone] nvarchar(64) NULL,
        [Website_Url] nvarchar(1024) NULL,
        [Website_Label] nvarchar(512) NULL,
        [Description_Text] nvarchar(max) NULL,
        [Organization_Parent] nvarchar(256) NULL,
        [Org_Chart_Url] nvarchar(1024) NULL,
        [Raw_Point_Json] nvarchar(max) NULL,
        [Raw_Detail_Html] nvarchar(max) NULL,
        [Content_Sha256] char(64) NOT NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_entry_versions_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_chapter_finder_entry_versions] PRIMARY KEY CLUSTERED ([Chapter_Finder_Entry_Version_ID]),
        CONSTRAINT [CK_chapter_finder_entry_versions_Change_Type] CHECK ([Change_Type] IN (N'inserted', N'updated', N'marked_inactive')),
        CONSTRAINT [CK_chapter_finder_entry_versions_Raw_Point_Json] CHECK ([Raw_Point_Json] IS NULL OR ISJSON([Raw_Point_Json]) = 1),
        CONSTRAINT [FK_chapter_finder_entry_versions_Entry] FOREIGN KEY ([Chapter_Finder_Entry_ID]) REFERENCES [clubexpress].[chapter_finder_entries] ([Chapter_Finder_Entry_ID]),
        CONSTRAINT [FK_chapter_finder_entry_versions_Run] FOREIGN KEY ([Chapter_Finder_Sync_Run_ID]) REFERENCES [clubexpress].[chapter_finder_sync_runs] ([Chapter_Finder_Sync_Run_ID])
    );
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_chapter_finder_entries_ClubExpress_Chapter_ID'
      AND [object_id] = OBJECT_ID(N'clubexpress.chapter_finder_entries')
)
BEGIN
    CREATE INDEX [IX_chapter_finder_entries_ClubExpress_Chapter_ID]
        ON [clubexpress].[chapter_finder_entries] ([ClubExpress_Chapter_ID])
        INCLUDE ([Chapter_Name], [Is_Current]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_chapter_finder_entries_State_City'
      AND [object_id] = OBJECT_ID(N'clubexpress.chapter_finder_entries')
)
BEGIN
    CREATE INDEX [IX_chapter_finder_entries_State_City]
        ON [clubexpress].[chapter_finder_entries] ([State_Province], [City], [Chapter_Name])
        INCLUDE ([Chapter_Finder_ID], [ClubExpress_Chapter_ID], [Is_Current]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_chapter_finder_entry_versions_Entry_Created'
      AND [object_id] = OBJECT_ID(N'clubexpress.chapter_finder_entry_versions')
)
BEGIN
    CREATE INDEX [IX_chapter_finder_entry_versions_Entry_Created]
        ON [clubexpress].[chapter_finder_entry_versions] ([Chapter_Finder_Entry_ID], [Created_At] DESC);
END;
GO

