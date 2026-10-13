-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [events].[organizers] (
    [organizer_id] bigint IDENTITY(1,1) NOT NULL,
    [organizer_key] nvarchar(80) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [name] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [organizer_type] varchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_events_organizers_type] DEFAULT ('other'),
    [is_active] bit NOT NULL CONSTRAINT [DF_events_organizers_active] DEFAULT ((1)),
    [created_at_utc] datetime2(0) NOT NULL CONSTRAINT [DF_events_organizers_created] DEFAULT (sysutcdatetime()),
    [updated_at_utc] datetime2(0) NOT NULL CONSTRAINT [DF_events_organizers_updated] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [events].[organizers] ADD CONSTRAINT [PK_events_organizers] PRIMARY KEY CLUSTERED ([organizer_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[organizers] ADD CONSTRAINT [UQ_events_organizers_key] UNIQUE NONCLUSTERED ([organizer_key] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[organizers] WITH CHECK ADD CONSTRAINT [CK_events_organizers_type] CHECK ([organizer_type]='other' OR [organizer_type]='chapter' OR [organizer_type]='national_aga');
GO
