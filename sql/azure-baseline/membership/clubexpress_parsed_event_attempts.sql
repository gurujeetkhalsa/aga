-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [membership].[clubexpress_parsed_event_attempts] (
    [Attempt_ID] bigint IDENTITY(1,1) NOT NULL,
    [Parsed_Event_ID] bigint NOT NULL,
    [Event_Key] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Error_Message] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Result_Payload_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Recorded_At] datetime2(0) NOT NULL CONSTRAINT [DF_clubexpress_parsed_event_attempts_Recorded_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [membership].[clubexpress_parsed_event_attempts] ADD CONSTRAINT [PK_clubexpress_parsed_event_attempts] PRIMARY KEY CLUSTERED ([Attempt_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_clubexpress_parsed_event_attempts_Event] ON [membership].[clubexpress_parsed_event_attempts] ([Parsed_Event_ID] ASC, [Recorded_At] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [membership].[clubexpress_parsed_event_attempts] WITH CHECK ADD CONSTRAINT [CK_clubexpress_parsed_event_attempts_Status] CHECK ([Status]=N'skipped' OR [Status]=N'error' OR [Status]=N'processed' OR [Status]=N'processing' OR [Status]=N'staged');
GO
ALTER TABLE [membership].[clubexpress_parsed_event_attempts] WITH CHECK ADD CONSTRAINT [FK_clubexpress_parsed_event_attempts_Event_ID] FOREIGN KEY ([Parsed_Event_ID]) REFERENCES [membership].[clubexpress_parsed_events] ([Parsed_Event_ID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
