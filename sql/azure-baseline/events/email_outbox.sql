-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [events].[email_outbox] (
    [email_id] bigint IDENTITY(1,1) NOT NULL,
    [event_id] bigint NOT NULL,
    [registration_id] uniqueidentifier NOT NULL,
    [occurrence_id] bigint NOT NULL,
    [kind] varchar(40) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [dedupe_key] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [status] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF__email_out__statu__40657506] DEFAULT ('pending'),
    [due_at_utc] datetime2(0) NOT NULL,
    [attempts] int NOT NULL CONSTRAINT [DF__email_out__attem__4159993F] DEFAULT ((0)),
    [claimed_at_utc] datetime2(0) NULL,
    [provider_message_id] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [last_error] nvarchar(500) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [created_at_utc] datetime2(0) NOT NULL CONSTRAINT [DF__email_out__creat__424DBD78] DEFAULT (sysutcdatetime()),
    [sent_at_utc] datetime2(0) NULL,
    [retry_after_utc] datetime2(0) NULL
 );
GO
ALTER TABLE [events].[email_outbox] ADD CONSTRAINT [PK__email_ou__3FEF87668BBE6C7E] PRIMARY KEY CLUSTERED ([email_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[email_outbox] ADD CONSTRAINT [UQ__email_ou__BBDAFDB809E4CC3A] UNIQUE NONCLUSTERED ([dedupe_key] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_events_email_due] ON [events].[email_outbox] ([status] ASC, [due_at_utc] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[email_outbox] WITH CHECK ADD CONSTRAINT [CK_events_email_status] CHECK ([status]='suppressed' OR [status]='uncertain' OR [status]='failed' OR [status]='sent' OR [status]='sending' OR [status]='pending');
GO
ALTER TABLE [events].[email_outbox] WITH CHECK ADD CONSTRAINT [FK__email_out__event__3D89085B] FOREIGN KEY ([event_id]) REFERENCES [events].[events] ([event_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [events].[email_outbox] WITH CHECK ADD CONSTRAINT [FK__email_out__regis__3E7D2C94] FOREIGN KEY ([registration_id]) REFERENCES [events].[registrations] ([registration_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [events].[email_outbox] WITH CHECK ADD CONSTRAINT [FK__email_out__occur__3F7150CD] FOREIGN KEY ([occurrence_id]) REFERENCES [events].[event_occurrences] ([occurrence_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
