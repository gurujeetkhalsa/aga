-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[chapter_finder_update_requests] (
    [Chapter_Finder_Update_Request_ID] bigint IDENTITY(1,1) NOT NULL,
    [Request_Key] uniqueidentifier NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Request_Key] DEFAULT (newid()),
    [Chapter_Finder_Entry_ID] bigint NOT NULL,
    [Club_ID] int NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Club_ID] DEFAULT ((454497)),
    [Chapter_Finder_ID] int NOT NULL,
    [ClubExpress_Chapter_ID] int NULL,
    [Chapter_Name_At_Submission] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Request_Source] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Request_Source] DEFAULT (N'chapter_update_form'),
    [Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Status] DEFAULT (N'submitted'),
    [Submitted_By_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Submitted_By_Email] nvarchar(320) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Submitted_By_Role] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Submitter_Notes] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Base_Content_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Proposed_Content_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Proposed_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Normalized_Proposed_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Field_Diff_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Changed_Field_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Changed_Field_Count] DEFAULT ((0)),
    [Reviewed_By] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Reviewed_At] datetime2(0) NULL,
    [Review_Notes] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Approved_At] datetime2(0) NULL,
    [Approved_By] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Rejected_At] datetime2(0) NULL,
    [Rejected_By] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Pushed_At] datetime2(0) NULL,
    [Push_Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Push_Status] DEFAULT (N'not_ready'),
    [Push_Attempt_Count] int NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Push_Attempt_Count] DEFAULT ((0)),
    [Last_Push_Attempt_At] datetime2(0) NULL,
    [Last_Push_Error] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Last_Push_Result_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Verification_Run_ID] bigint NULL,
    [Verified_At] datetime2(0) NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Created_At] DEFAULT (sysutcdatetime()),
    [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_update_requests_Updated_At] DEFAULT (sysutcdatetime()),
    [Chapter_Finder_Update_Access_Link_ID] bigint NULL,
    [Base_Administrator_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Proposed_Administrator_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Base_Administrator_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Proposed_Administrator_Sha256] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 );
GO
ALTER TABLE [clubexpress].[chapter_finder_update_requests] ADD CONSTRAINT [PK_chapter_finder_update_requests] PRIMARY KEY CLUSTERED ([Chapter_Finder_Update_Request_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_update_requests] ADD CONSTRAINT [UQ_chapter_finder_update_requests_Request_Key] UNIQUE NONCLUSTERED ([Request_Key] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_finder_update_requests_Status_Push] ON [clubexpress].[chapter_finder_update_requests] ([Status] ASC, [Push_Status] ASC, [Created_At] ASC) INCLUDE ([Chapter_Finder_Entry_ID], [Chapter_Finder_ID], [ClubExpress_Chapter_ID], [Chapter_Name_At_Submission]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_finder_update_requests_Entry_Created] ON [clubexpress].[chapter_finder_update_requests] ([Chapter_Finder_Entry_ID] ASC, [Created_At] DESC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_finder_update_requests_Access_Link_Created] ON [clubexpress].[chapter_finder_update_requests] ([Chapter_Finder_Update_Access_Link_ID] ASC, [Created_At] DESC) INCLUDE ([Status], [Push_Status], [Chapter_Finder_ID]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_update_requests] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_update_requests_Status] CHECK ([Status]=N'failed' OR [Status]=N'cancelled' OR [Status]=N'rejected' OR [Status]=N'verified' OR [Status]=N'pushed' OR [Status]=N'pushing' OR [Status]=N'pending_push' OR [Status]=N'approved' OR [Status]=N'needs_review' OR [Status]=N'submitted' OR [Status]=N'draft');
GO
ALTER TABLE [clubexpress].[chapter_finder_update_requests] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_update_requests_Push_Status] CHECK ([Push_Status]=N'skipped' OR [Push_Status]=N'failed' OR [Push_Status]=N'verified' OR [Push_Status]=N'pushed' OR [Push_Status]=N'pushing' OR [Push_Status]=N'pending' OR [Push_Status]=N'not_ready');
GO
ALTER TABLE [clubexpress].[chapter_finder_update_requests] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_update_requests_Proposed_Json] CHECK (isjson([Proposed_Json])=(1));
GO
ALTER TABLE [clubexpress].[chapter_finder_update_requests] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_update_requests_Normalized_Proposed_Json] CHECK (isjson([Normalized_Proposed_Json])=(1));
GO
ALTER TABLE [clubexpress].[chapter_finder_update_requests] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_update_requests_Field_Diff_Json] CHECK (isjson([Field_Diff_Json])=(1));
GO
ALTER TABLE [clubexpress].[chapter_finder_update_requests] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_update_requests_Last_Push_Result_Json] CHECK ([Last_Push_Result_Json] IS NULL OR isjson([Last_Push_Result_Json])=(1));
GO
ALTER TABLE [clubexpress].[chapter_finder_update_requests] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_update_requests_Changed_Field_Count] CHECK ([Changed_Field_Count]>=(0));
GO
ALTER TABLE [clubexpress].[chapter_finder_update_requests] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_update_requests_Push_Attempt_Count] CHECK ([Push_Attempt_Count]>=(0));
GO
ALTER TABLE [clubexpress].[chapter_finder_update_requests] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_update_requests_Base_Administrator_Json] CHECK ([Base_Administrator_Json] IS NULL OR isjson([Base_Administrator_Json])=(1));
GO
ALTER TABLE [clubexpress].[chapter_finder_update_requests] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_update_requests_Proposed_Administrator_Json] CHECK ([Proposed_Administrator_Json] IS NULL OR isjson([Proposed_Administrator_Json])=(1));
GO
ALTER TABLE [clubexpress].[chapter_finder_update_requests] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_update_requests_Entry] FOREIGN KEY ([Chapter_Finder_Entry_ID]) REFERENCES [clubexpress].[chapter_finder_entries] ([Chapter_Finder_Entry_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [clubexpress].[chapter_finder_update_requests] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_update_requests_Verification_Run] FOREIGN KEY ([Verification_Run_ID]) REFERENCES [clubexpress].[chapter_finder_sync_runs] ([Chapter_Finder_Sync_Run_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [clubexpress].[chapter_finder_update_requests] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_update_requests_Access_Link] FOREIGN KEY ([Chapter_Finder_Update_Access_Link_ID]) REFERENCES [clubexpress].[chapter_finder_update_access_links] ([Chapter_Finder_Update_Access_Link_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
