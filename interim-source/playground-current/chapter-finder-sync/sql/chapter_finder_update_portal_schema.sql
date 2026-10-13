-- Copyright 2026, American Go Association, All rights reserved

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'clubexpress') IS NULL
    EXEC(N'CREATE SCHEMA [clubexpress]');
GO

IF OBJECT_ID(N'clubexpress.chapter_finder_update_access_links', N'U') IS NULL
BEGIN
    CREATE TABLE [clubexpress].[chapter_finder_update_access_links]
    (
        [Chapter_Finder_Update_Access_Link_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Chapter_Finder_Entry_ID] bigint NOT NULL,
        [Club_ID] int NOT NULL CONSTRAINT [DF_chapter_finder_update_access_links_Club_ID] DEFAULT 454497,
        [Chapter_Finder_ID] int NOT NULL,
        [Token_Hash_Sha256] char(64) NOT NULL,
        [Link_Label] nvarchar(256) NULL,
        [Created_By] nvarchar(256) NULL,
        [Expires_At] datetime2(0) NULL,
        [Is_Active] bit NOT NULL CONSTRAINT [DF_chapter_finder_update_access_links_Is_Active] DEFAULT 1,
        [Last_Used_At] datetime2(0) NULL,
        [Use_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_update_access_links_Use_Count] DEFAULT 0,
        [Last_Submitted_At] datetime2(0) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_update_access_links_Created_At] DEFAULT SYSUTCDATETIME(),
        [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_update_access_links_Updated_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_chapter_finder_update_access_links] PRIMARY KEY CLUSTERED ([Chapter_Finder_Update_Access_Link_ID]),
        CONSTRAINT [UQ_chapter_finder_update_access_links_Token_Hash] UNIQUE ([Token_Hash_Sha256]),
        CONSTRAINT [CK_chapter_finder_update_access_links_Use_Count] CHECK ([Use_Count] >= 0),
        CONSTRAINT [FK_chapter_finder_update_access_links_Entry] FOREIGN KEY ([Chapter_Finder_Entry_ID]) REFERENCES [clubexpress].[chapter_finder_entries] ([Chapter_Finder_Entry_ID])
    );
END;
GO

IF COL_LENGTH(N'clubexpress.chapter_finder_update_requests', N'Chapter_Finder_Update_Access_Link_ID') IS NULL
    ALTER TABLE [clubexpress].[chapter_finder_update_requests] ADD [Chapter_Finder_Update_Access_Link_ID] bigint NULL;
GO

IF COL_LENGTH(N'clubexpress.chapter_finder_update_requests', N'Base_Administrator_Json') IS NULL
    ALTER TABLE [clubexpress].[chapter_finder_update_requests] ADD [Base_Administrator_Json] nvarchar(max) NULL;
GO

IF COL_LENGTH(N'clubexpress.chapter_finder_update_requests', N'Proposed_Administrator_Json') IS NULL
    ALTER TABLE [clubexpress].[chapter_finder_update_requests] ADD [Proposed_Administrator_Json] nvarchar(max) NULL;
GO

IF COL_LENGTH(N'clubexpress.chapter_finder_update_requests', N'Base_Administrator_Sha256') IS NULL
    ALTER TABLE [clubexpress].[chapter_finder_update_requests] ADD [Base_Administrator_Sha256] char(64) NULL;
GO

IF COL_LENGTH(N'clubexpress.chapter_finder_update_requests', N'Proposed_Administrator_Sha256') IS NULL
    ALTER TABLE [clubexpress].[chapter_finder_update_requests] ADD [Proposed_Administrator_Sha256] char(64) NULL;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.foreign_keys
    WHERE [name] = N'FK_chapter_finder_update_requests_Access_Link'
)
BEGIN
    ALTER TABLE [clubexpress].[chapter_finder_update_requests]
        ADD CONSTRAINT [FK_chapter_finder_update_requests_Access_Link]
        FOREIGN KEY ([Chapter_Finder_Update_Access_Link_ID])
        REFERENCES [clubexpress].[chapter_finder_update_access_links] ([Chapter_Finder_Update_Access_Link_ID]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.check_constraints
    WHERE [name] = N'CK_chapter_finder_update_requests_Base_Administrator_Json'
)
BEGIN
    ALTER TABLE [clubexpress].[chapter_finder_update_requests]
        ADD CONSTRAINT [CK_chapter_finder_update_requests_Base_Administrator_Json]
        CHECK ([Base_Administrator_Json] IS NULL OR ISJSON([Base_Administrator_Json]) = 1);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.check_constraints
    WHERE [name] = N'CK_chapter_finder_update_requests_Proposed_Administrator_Json'
)
BEGIN
    ALTER TABLE [clubexpress].[chapter_finder_update_requests]
        ADD CONSTRAINT [CK_chapter_finder_update_requests_Proposed_Administrator_Json]
        CHECK ([Proposed_Administrator_Json] IS NULL OR ISJSON([Proposed_Administrator_Json]) = 1);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_chapter_finder_update_access_links_Entry_Active'
      AND [object_id] = OBJECT_ID(N'clubexpress.chapter_finder_update_access_links')
)
BEGIN
    CREATE INDEX [IX_chapter_finder_update_access_links_Entry_Active]
        ON [clubexpress].[chapter_finder_update_access_links] ([Chapter_Finder_Entry_ID], [Is_Active], [Expires_At])
        INCLUDE ([Chapter_Finder_ID], [Link_Label], [Last_Submitted_At]);
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_chapter_finder_update_requests_Access_Link_Created'
      AND [object_id] = OBJECT_ID(N'clubexpress.chapter_finder_update_requests')
)
BEGIN
    CREATE INDEX [IX_chapter_finder_update_requests_Access_Link_Created]
        ON [clubexpress].[chapter_finder_update_requests] ([Chapter_Finder_Update_Access_Link_ID], [Created_At] DESC)
        INCLUDE ([Status], [Push_Status], [Chapter_Finder_ID]);
END;
GO

CREATE OR ALTER VIEW [clubexpress].[v_chapter_finder_update_portal_review]
AS
SELECT
    r.[Chapter_Finder_Update_Request_ID],
    r.[Request_Key],
    r.[Status],
    r.[Push_Status],
    r.[Chapter_Finder_Entry_ID],
    r.[Chapter_Finder_ID],
    r.[ClubExpress_Chapter_ID],
    r.[Chapter_Name_At_Submission],
    e.[Chapter_Name] AS [Current_Chapter_Name],
    r.[Submitted_By_Name],
    r.[Submitted_By_Email],
    r.[Submitted_By_Role],
    r.[Submitter_Notes],
    r.[Changed_Field_Count],
    r.[Field_Diff_Json],
    r.[Normalized_Proposed_Json],
    r.[Base_Administrator_Json],
    r.[Proposed_Administrator_Json],
    r.[Reviewed_By],
    r.[Reviewed_At],
    r.[Review_Notes],
    r.[Approved_By],
    r.[Approved_At],
    r.[Rejected_By],
    r.[Rejected_At],
    r.[Pushed_At],
    r.[Last_Push_Error],
    r.[Created_At],
    r.[Updated_At]
FROM [clubexpress].[chapter_finder_update_requests] AS r
INNER JOIN [clubexpress].[chapter_finder_entries] AS e
    ON e.[Chapter_Finder_Entry_ID] = r.[Chapter_Finder_Entry_ID];
GO
