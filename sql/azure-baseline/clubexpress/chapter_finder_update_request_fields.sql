-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [clubexpress].[chapter_finder_update_request_fields] (
    [Chapter_Finder_Update_Request_Field_ID] bigint IDENTITY(1,1) NOT NULL,
    [Chapter_Finder_Update_Request_ID] bigint NOT NULL,
    [Field_Name] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Current_Value] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Proposed_Value] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Change_Type] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Display_Order] int NOT NULL CONSTRAINT [DF_chapter_finder_update_request_fields_Display_Order] DEFAULT ((0)),
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_finder_update_request_fields_Created_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [clubexpress].[chapter_finder_update_request_fields] ADD CONSTRAINT [PK_chapter_finder_update_request_fields] PRIMARY KEY CLUSTERED ([Chapter_Finder_Update_Request_Field_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_update_request_fields] ADD CONSTRAINT [UQ_chapter_finder_update_request_fields_Request_Field] UNIQUE NONCLUSTERED ([Chapter_Finder_Update_Request_ID] ASC, [Field_Name] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [clubexpress].[chapter_finder_update_request_fields] WITH CHECK ADD CONSTRAINT [CK_chapter_finder_update_request_fields_Change_Type] CHECK ([Change_Type]=N'removed' OR [Change_Type]=N'changed' OR [Change_Type]=N'added');
GO
ALTER TABLE [clubexpress].[chapter_finder_update_request_fields] WITH CHECK ADD CONSTRAINT [FK_chapter_finder_update_request_fields_Request] FOREIGN KEY ([Chapter_Finder_Update_Request_ID]) REFERENCES [clubexpress].[chapter_finder_update_requests] ([Chapter_Finder_Update_Request_ID]) ON DELETE CASCADE ON UPDATE NO ACTION;
GO
