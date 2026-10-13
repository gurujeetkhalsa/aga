-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [events].[registrations] (
    [registration_id] uniqueidentifier NOT NULL,
    [event_id] bigint NOT NULL,
    [event_version_id] bigint NOT NULL,
    [status] varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [payment_status] varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_events_registrations_payment] DEFAULT ('not_required'),
    [idempotency_key] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [submitted_at_utc] datetime2(0) NULL,
    [created_at_utc] datetime2(0) NOT NULL CONSTRAINT [DF_events_registrations_created] DEFAULT (sysutcdatetime()),
    [updated_at_utc] datetime2(0) NOT NULL CONSTRAINT [DF_events_registrations_updated] DEFAULT (sysutcdatetime()),
    [row_version] timestamp NOT NULL,
    [referral_chapter_id] int NULL
 );
GO
ALTER TABLE [events].[registrations] ADD CONSTRAINT [PK_events_registrations] PRIMARY KEY CLUSTERED ([registration_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[registrations] ADD CONSTRAINT [UQ_events_registrations_idempotency] UNIQUE NONCLUSTERED ([event_id] ASC, [idempotency_key] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_events_registrations_event_status] ON [events].[registrations] ([event_id] ASC, [status] ASC, [submitted_at_utc] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[registrations] WITH CHECK ADD CONSTRAINT [CK_events_registrations_status] CHECK ([status]='expired' OR [status]='withdrawn' OR [status]='cancelled' OR [status]='mixed' OR [status]='waitlisted' OR [status]='confirmed' OR [status]='submitted' OR [status]='in_progress');
GO
ALTER TABLE [events].[registrations] WITH CHECK ADD CONSTRAINT [CK_events_registrations_payment] CHECK ([payment_status]='disputed' OR [payment_status]='failed' OR [payment_status]='refunded' OR [payment_status]='partially_refunded' OR [payment_status]='succeeded' OR [payment_status]='authorized' OR [payment_status]='pending' OR [payment_status]='not_required');
GO
ALTER TABLE [events].[registrations] WITH CHECK ADD CONSTRAINT [FK_events_registrations_event] FOREIGN KEY ([event_id]) REFERENCES [events].[events] ([event_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [events].[registrations] WITH CHECK ADD CONSTRAINT [FK_events_registrations_version] FOREIGN KEY ([event_version_id]) REFERENCES [events].[event_versions] ([event_version_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
