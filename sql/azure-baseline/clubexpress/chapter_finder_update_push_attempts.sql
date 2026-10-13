-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[chapter_finder_update_push_attempts] (
    [Chapter_Finder_Update_Push_Attempt_ID] bigint IDENTITY(1,1) NOT NULL,
    [Chapter_Finder_Update_Request_ID] bigint NOT NULL,
    [Attempt_Number] int NOT NULL,
    [Started_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_update_push_attempts_Started_At] DEFAULT (sysutcdatetime()),
    [Completed_At] datetime2(0) NULL,
    [Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_chapter_finder_update_push_attempts_Status] DEFAULT (N'running'),
    [Operator] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ClubExpress_Edit_Url] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Payload_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Result_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Error_Message] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_update_push_attempts_Created_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [clubexpress].[chapter_finder_update_push_attempts] ADD CONSTRAINT [PK_chapter_finder_update_push_attempts] PRIMARY KEY CLUSTERED ([Chapter_Finder_Update_Push_Attempt_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_update_push_attempts] ADD CONSTRAINT [UQ_chapter_finder_update_push_attempts_Request_Attempt] UNIQUE NONCLUSTERED ([Chapter_Finder_Update_Request_ID] ASC, [Attempt_Number] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_update_push_attempts] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_update_push_attempts_Status] CHECK ([Status]=N'cancelled' OR [Status]=N'failed' OR [Status]=N'succeeded' OR [Status]=N'running');
GO
ALTER TABLE [clubexpress].[chapter_finder_update_push_attempts] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_update_push_attempts_Payload_Json] CHECK (isjson([Payload_Json])=(1));
GO
ALTER TABLE [clubexpress].[chapter_finder_update_push_attempts] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_update_push_attempts_Result_Json] CHECK ([Result_Json] IS NULL OR isjson([Result_Json])=(1));
GO
ALTER TABLE [clubexpress].[chapter_finder_update_push_attempts] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_update_push_attempts_Request] FOREIGN KEY ([Chapter_Finder_Update_Request_ID]) REFERENCES [clubexpress].[chapter_finder_update_requests] ([Chapter_Finder_Update_Request_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
