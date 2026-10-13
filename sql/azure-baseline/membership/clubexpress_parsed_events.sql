-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [membership].[clubexpress_parsed_events] (
    [Parsed_Event_ID] bigint IDENTITY(1,1) NOT NULL,
    [Message_ID] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Event_Key] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Message_Type] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Event_Type] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Received_At] datetime2(0) NOT NULL,
    [Event_Date] date NULL,
    [AGAID] int NULL,
    [ChapterID] int NULL,
    [Parsed_Item_Count] int NOT NULL CONSTRAINT [DF_clubexpress_parsed_events_Item_Count] DEFAULT ((1)),
    [Sender] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Subject] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Blob_Path] nvarchar(1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Parsed_Payload_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Downstream_Payload_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_clubexpress_parsed_events_Status] DEFAULT (N'staged'),
    [Attempt_Count] int NOT NULL CONSTRAINT [DF_clubexpress_parsed_events_Attempt_Count] DEFAULT ((0)),
    [Last_Processed_At] datetime2(0) NULL,
    [Last_Error_Message] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_clubexpress_parsed_events_Created_At] DEFAULT (sysutcdatetime()),
    [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_clubexpress_parsed_events_Updated_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [membership].[clubexpress_parsed_events] ADD CONSTRAINT [PK_clubexpress_parsed_events] PRIMARY KEY CLUSTERED ([Parsed_Event_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [membership].[clubexpress_parsed_events] ADD CONSTRAINT [UQ_clubexpress_parsed_events_Event_Key] UNIQUE NONCLUSTERED ([Event_Key] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_clubexpress_parsed_events_Message] ON [membership].[clubexpress_parsed_events] ([Message_ID] ASC, [Event_Type] ASC) INCLUDE ([Status], [Received_At], [AGAID], [ChapterID]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_clubexpress_parsed_events_Status] ON [membership].[clubexpress_parsed_events] ([Status] ASC, [Received_At] ASC, [Event_Type] ASC) INCLUDE ([Message_ID], [Event_Key], [AGAID], [ChapterID]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [membership].[clubexpress_parsed_events] WITH CHECK ADD CONSTRAINT [CK_clubexpress_parsed_events_Status] CHECK ([Status]=N'skipped' OR [Status]=N'error' OR [Status]=N'processed' OR [Status]=N'processing' OR [Status]=N'staged');
GO
ALTER TABLE [membership].[clubexpress_parsed_events] WITH CHECK ADD CONSTRAINT [CK_clubexpress_parsed_events_Item_Count] CHECK ([Parsed_Item_Count]>=(0));
GO
ALTER TABLE [membership].[clubexpress_parsed_events] WITH CHECK ADD CONSTRAINT [CK_clubexpress_parsed_events_Attempt_Count] CHECK ([Attempt_Count]>=(0));
GO
