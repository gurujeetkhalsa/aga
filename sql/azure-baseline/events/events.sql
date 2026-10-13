-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [events].[events] (
    [event_id] bigint IDENTITY(1,1) NOT NULL,
    [organizer_id] bigint NOT NULL,
    [slug] nvarchar(120) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [name] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [description] nvarchar(2000) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [time_zone] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_events_time_zone] DEFAULT (N'America/New_York'),
    [lifecycle] varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_events_lifecycle] DEFAULT ('draft'),
    [selection_mode] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_events_selection_mode] DEFAULT ('single'),
    [max_occurrence_selections] smallint NULL,
    [current_event_version_id] bigint NULL,
    [created_at_utc] datetime2(0) NOT NULL CONSTRAINT [DF_events_created] DEFAULT (sysutcdatetime()),
    [updated_at_utc] datetime2(0) NOT NULL CONSTRAINT [DF_events_updated] DEFAULT (sysutcdatetime()),
    [row_version] timestamp NOT NULL,
    [public_occurrence_limit] smallint NOT NULL CONSTRAINT [DF_events_public_occurrence_limit] DEFAULT ((4))
 );
GO
ALTER TABLE [events].[events] ADD CONSTRAINT [PK_events_events] PRIMARY KEY CLUSTERED ([event_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[events] ADD CONSTRAINT [UQ_events_events_slug] UNIQUE NONCLUSTERED ([slug] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[events] WITH CHECK ADD CONSTRAINT [CK_events_events_lifecycle] CHECK ([lifecycle]='archived' OR [lifecycle]='completed' OR [lifecycle]='cancelled' OR [lifecycle]='registration_closed' OR [lifecycle]='registration_open' OR [lifecycle]='published' OR [lifecycle]='draft');
GO
ALTER TABLE [events].[events] WITH CHECK ADD CONSTRAINT [CK_events_events_selection_mode] CHECK ([selection_mode]='multiple' OR [selection_mode]='single');
GO
ALTER TABLE [events].[events] WITH CHECK ADD CONSTRAINT [CK_events_events_selection_max] CHECK ([max_occurrence_selections] IS NULL OR [max_occurrence_selections]>=(1));
GO
ALTER TABLE [events].[events] WITH CHECK ADD CONSTRAINT [CK_events_events_single_max] CHECK ([selection_mode]<>'single' OR [max_occurrence_selections] IS NULL OR [max_occurrence_selections]=(1));
GO
ALTER TABLE [events].[events] WITH CHECK ADD CONSTRAINT [CK_events_public_occurrence_limit] CHECK ([public_occurrence_limit]>=(1) AND [public_occurrence_limit]<=(24));
GO
ALTER TABLE [events].[events] WITH CHECK ADD CONSTRAINT [FK_events_events_organizer] FOREIGN KEY ([organizer_id]) REFERENCES [events].[organizers] ([organizer_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [events].[events] WITH CHECK ADD CONSTRAINT [FK_events_events_current_version] FOREIGN KEY ([current_event_version_id]) REFERENCES [events].[event_versions] ([event_version_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
