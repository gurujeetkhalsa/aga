-- Copyright 2026, American Go Association, All rights reserved

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'clubexpress') IS NULL
    EXEC(N'CREATE SCHEMA [clubexpress]');
GO

IF OBJECT_ID(N'clubexpress.chapter_finder_administrator_sync_runs', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[chapter_finder_administrator_sync_runs]
    (
        [Chapter_Finder_Administrator_Sync_Run_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Run_Key] nvarchar(256) NOT NULL,
        [Club_ID] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Club_ID] DEFAULT 454497,
        [Source_Url_Template] nvarchar(1024) NOT NULL,
        [Extracted_At] datetime2(0) NULL,
        [Started_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Started_At] DEFAULT SYSUTCDATETIME(),
        [Completed_At] datetime2(0) NULL,
        [Status] nvarchar(32) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Status] DEFAULT N'running',
        [Chapter_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Chapter_Count] DEFAULT 0,
        [Chapters_With_Administrators_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Chapters_With_Administrators_Count] DEFAULT 0,
        [Assignment_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Assignment_Count] DEFAULT 0,
        [Inserted_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Inserted_Count] DEFAULT 0,
        [Updated_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Updated_Count] DEFAULT 0,
        [Unchanged_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Unchanged_Count] DEFAULT 0,
        [Marked_Inactive_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Marked_Inactive_Count] DEFAULT 0,
        [Matched_Unique_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Matched_Unique_Count] DEFAULT 0,
        [Ambiguous_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Ambiguous_Count] DEFAULT 0,
        [Unmatched_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Unmatched_Count] DEFAULT 0,
        [Error_Message] nvarchar(max) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_sync_runs_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_chapter_finder_administrator_sync_runs] PRIMARY KEY CLUSTERED ([Chapter_Finder_Administrator_Sync_Run_ID]),
        CONSTRAINT [UQ_chapter_finder_administrator_sync_runs_Run_Key] UNIQUE ([Run_Key]),
        CONSTRAINT [CK_chapter_finder_administrator_sync_runs_Status] CHECK ([Status] IN (N'running', N'succeeded', N'partial', N'failed', N'cancelled'))
    );
END;
GO

IF OBJECT_ID(N'clubexpress.chapter_finder_administrator_assignments', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[chapter_finder_administrator_assignments]
    (
        [Chapter_Finder_Administrator_Assignment_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Chapter_Finder_Entry_ID] bigint NOT NULL,
        [Club_ID] int NOT NULL,
        [Chapter_Finder_ID] int NOT NULL,
        [Administrator_Display_Name] nvarchar(256) NOT NULL,
        [Normalized_Display_Name] nvarchar(256) NOT NULL,
        [Duplicate_Ordinal] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_Duplicate_Ordinal] DEFAULT 1,
        [Assignment_Key_Sha256] char(64) NOT NULL,
        [Membership_AGAID] int NULL,
        [Member_Match_Status] nvarchar(32) NOT NULL,
        [Member_Match_Candidate_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_Member_Match_Candidate_Count] DEFAULT 0,
        [First_Seen_Run_ID] bigint NOT NULL,
        [Last_Seen_Run_ID] bigint NOT NULL,
        [First_Seen_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_First_Seen_At] DEFAULT SYSUTCDATETIME(),
        [Last_Seen_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_Last_Seen_At] DEFAULT SYSUTCDATETIME(),
        [Last_Changed_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_Last_Changed_At] DEFAULT SYSUTCDATETIME(),
        [Is_Current] bit NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_Is_Current] DEFAULT 1,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_Created_At] DEFAULT SYSUTCDATETIME(),
        [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignments_Updated_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_chapter_finder_administrator_assignments] PRIMARY KEY CLUSTERED ([Chapter_Finder_Administrator_Assignment_ID]),
        CONSTRAINT [UQ_chapter_finder_administrator_assignments_Entry_Key] UNIQUE ([Chapter_Finder_Entry_ID], [Assignment_Key_Sha256]),
        CONSTRAINT [CK_chapter_finder_administrator_assignments_Duplicate_Ordinal] CHECK ([Duplicate_Ordinal] >= 1),
        CONSTRAINT [CK_chapter_finder_administrator_assignments_Member_Match_Status] CHECK ([Member_Match_Status] IN (N'matched_unique', N'ambiguous', N'unmatched')),
        CONSTRAINT [FK_chapter_finder_administrator_assignments_Entry] FOREIGN KEY ([Chapter_Finder_Entry_ID]) REFERENCES [clubexpress].[chapter_finder_entries] ([Chapter_Finder_Entry_ID]),
        CONSTRAINT [FK_chapter_finder_administrator_assignments_Member] FOREIGN KEY ([Membership_AGAID]) REFERENCES [membership].[members] ([AGAID]),
        CONSTRAINT [FK_chapter_finder_administrator_assignments_First_Run] FOREIGN KEY ([First_Seen_Run_ID]) REFERENCES [clubexpress].[chapter_finder_administrator_sync_runs] ([Chapter_Finder_Administrator_Sync_Run_ID]),
        CONSTRAINT [FK_chapter_finder_administrator_assignments_Last_Run] FOREIGN KEY ([Last_Seen_Run_ID]) REFERENCES [clubexpress].[chapter_finder_administrator_sync_runs] ([Chapter_Finder_Administrator_Sync_Run_ID])
    );
END;
GO

IF OBJECT_ID(N'clubexpress.chapter_finder_administrator_assignment_versions', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[chapter_finder_administrator_assignment_versions]
    (
        [Chapter_Finder_Administrator_Assignment_Version_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Chapter_Finder_Administrator_Assignment_ID] bigint NOT NULL,
        [Chapter_Finder_Administrator_Sync_Run_ID] bigint NOT NULL,
        [Change_Type] nvarchar(32) NOT NULL,
        [Chapter_Finder_Entry_ID] bigint NOT NULL,
        [Club_ID] int NOT NULL,
        [Chapter_Finder_ID] int NOT NULL,
        [Administrator_Display_Name] nvarchar(256) NOT NULL,
        [Normalized_Display_Name] nvarchar(256) NOT NULL,
        [Duplicate_Ordinal] int NOT NULL,
        [Assignment_Key_Sha256] char(64) NOT NULL,
        [Membership_AGAID] int NULL,
        [Member_Match_Status] nvarchar(32) NOT NULL,
        [Member_Match_Candidate_Count] int NOT NULL,
        [Is_Current] bit NOT NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_administrator_assignment_versions_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_chapter_finder_administrator_assignment_versions] PRIMARY KEY CLUSTERED ([Chapter_Finder_Administrator_Assignment_Version_ID]),
        CONSTRAINT [CK_chapter_finder_administrator_assignment_versions_Change_Type] CHECK ([Change_Type] IN (N'inserted', N'reactivated', N'resolution_changed', N'marked_inactive')),
        CONSTRAINT [CK_chapter_finder_administrator_assignment_versions_Member_Match_Status] CHECK ([Member_Match_Status] IN (N'matched_unique', N'ambiguous', N'unmatched')),
        CONSTRAINT [FK_chapter_finder_administrator_assignment_versions_Assignment] FOREIGN KEY ([Chapter_Finder_Administrator_Assignment_ID]) REFERENCES [clubexpress].[chapter_finder_administrator_assignments] ([Chapter_Finder_Administrator_Assignment_ID]),
        CONSTRAINT [FK_chapter_finder_administrator_assignment_versions_Run] FOREIGN KEY ([Chapter_Finder_Administrator_Sync_Run_ID]) REFERENCES [clubexpress].[chapter_finder_administrator_sync_runs] ([Chapter_Finder_Administrator_Sync_Run_ID])
    );
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_chapter_finder_administrator_assignments_Current_Chapter'
      AND [object_id] = OBJECT_ID(N'clubexpress.chapter_finder_administrator_assignments')
)
BEGIN
    CREATE INDEX [IX_chapter_finder_administrator_assignments_Current_Chapter]
        ON [clubexpress].[chapter_finder_administrator_assignments] ([Is_Current], [Chapter_Finder_ID])
        INCLUDE ([Administrator_Display_Name], [Membership_AGAID], [Member_Match_Status]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_chapter_finder_administrator_assignments_Membership_AGAID'
      AND [object_id] = OBJECT_ID(N'clubexpress.chapter_finder_administrator_assignments')
)
BEGIN
    CREATE INDEX [IX_chapter_finder_administrator_assignments_Membership_AGAID]
        ON [clubexpress].[chapter_finder_administrator_assignments] ([Membership_AGAID], [Is_Current])
        INCLUDE ([Chapter_Finder_ID], [Administrator_Display_Name]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_chapter_finder_administrator_assignment_versions_Assignment_Created'
      AND [object_id] = OBJECT_ID(N'clubexpress.chapter_finder_administrator_assignment_versions')
)
BEGIN
    CREATE INDEX [IX_chapter_finder_administrator_assignment_versions_Assignment_Created]
        ON [clubexpress].[chapter_finder_administrator_assignment_versions] ([Chapter_Finder_Administrator_Assignment_ID], [Created_At] DESC);
END;
GO
