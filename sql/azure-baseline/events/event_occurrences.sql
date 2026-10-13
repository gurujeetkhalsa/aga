-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [events].[event_occurrences] (
    [occurrence_id] bigint IDENTITY(1,1) NOT NULL,
    [event_id] bigint NOT NULL,
    [event_version_id] bigint NOT NULL,
    [occurrence_key] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [title] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [starts_at_utc] datetime2(0) NOT NULL,
    [ends_at_utc] datetime2(0) NOT NULL,
    [registration_opens_at_utc] datetime2(0) NULL,
    [registration_closes_at_utc] datetime2(0) NULL,
    [capacity] int NULL,
    [waitlist_enabled] bit NOT NULL CONSTRAINT [DF_events_occurrences_waitlist] DEFAULT ((1)),
    [location_name] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [lifecycle] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_events_occurrences_lifecycle] DEFAULT ('draft'),
    [created_at_utc] datetime2(0) NOT NULL CONSTRAINT [DF_events_occurrences_created] DEFAULT (sysutcdatetime()),
    [updated_at_utc] datetime2(0) NOT NULL CONSTRAINT [DF_events_occurrences_updated] DEFAULT (sysutcdatetime()),
    [row_version] timestamp NOT NULL
 );
GO
ALTER TABLE [events].[event_occurrences] ADD CONSTRAINT [PK_events_occurrences] PRIMARY KEY CLUSTERED ([occurrence_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[event_occurrences] ADD CONSTRAINT [UQ_events_occurrences_key] UNIQUE NONCLUSTERED ([event_id] ASC, [occurrence_key] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_events_occurrences_public] ON [events].[event_occurrences] ([event_id] ASC, [lifecycle] ASC, [starts_at_utc] ASC) INCLUDE ([ends_at_utc], [capacity], [waitlist_enabled], [registration_opens_at_utc], [registration_closes_at_utc]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[event_occurrences] WITH CHECK ADD CONSTRAINT [CK_events_occurrences_dates] CHECK ([ends_at_utc]>[starts_at_utc]);
GO
ALTER TABLE [events].[event_occurrences] WITH CHECK ADD CONSTRAINT [CK_events_occurrences_registration_dates] CHECK ([registration_closes_at_utc] IS NULL OR [registration_opens_at_utc] IS NULL OR [registration_closes_at_utc]>[registration_opens_at_utc]);
GO
ALTER TABLE [events].[event_occurrences] WITH CHECK ADD CONSTRAINT [CK_events_occurrences_capacity] CHECK ([capacity] IS NULL OR [capacity]>=(1));
GO
ALTER TABLE [events].[event_occurrences] WITH CHECK ADD CONSTRAINT [CK_events_occurrences_lifecycle] CHECK ([lifecycle]='archived' OR [lifecycle]='completed' OR [lifecycle]='cancelled' OR [lifecycle]='published' OR [lifecycle]='draft');
GO
ALTER TABLE [events].[event_occurrences] WITH CHECK ADD CONSTRAINT [FK_events_occurrences_event] FOREIGN KEY ([event_id]) REFERENCES [events].[events] ([event_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [events].[event_occurrences] WITH CHECK ADD CONSTRAINT [FK_events_occurrences_version] FOREIGN KEY ([event_version_id]) REFERENCES [events].[event_versions] ([event_version_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
