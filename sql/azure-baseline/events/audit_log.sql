-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [events].[audit_log] (
    [audit_id] bigint IDENTITY(1,1) NOT NULL,
    [event_id] bigint NULL,
    [registration_id] uniqueidentifier NULL,
    [action] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [actor_type] varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [actor_principal_id] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [detail_json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_events_audit_detail] DEFAULT (N'{}'),
    [created_at_utc] datetime2(0) NOT NULL CONSTRAINT [DF_events_audit_created] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [events].[audit_log] ADD CONSTRAINT [PK_events_audit_log] PRIMARY KEY CLUSTERED ([audit_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_events_audit_event_time] ON [events].[audit_log] ([event_id] ASC, [created_at_utc] DESC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[audit_log] WITH CHECK ADD CONSTRAINT [CK_events_audit_detail_json] CHECK (isjson([detail_json])=(1));
GO
ALTER TABLE [events].[audit_log] WITH CHECK ADD CONSTRAINT [FK_events_audit_event] FOREIGN KEY ([event_id]) REFERENCES [events].[events] ([event_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [events].[audit_log] WITH CHECK ADD CONSTRAINT [FK_events_audit_registration] FOREIGN KEY ([registration_id]) REFERENCES [events].[registrations] ([registration_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
