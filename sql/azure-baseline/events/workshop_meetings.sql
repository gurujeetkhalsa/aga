-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [events].[workshop_meetings] (
    [occurrence_id] bigint NOT NULL,
    [meeting_id] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [join_url] nvarchar(2000) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [source] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF__workshop___sourc__35E7E693] DEFAULT ('zoom'),
    [sync_status] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF__workshop___sync___36DC0ACC] DEFAULT ('pending'),
    [sync_error] nvarchar(500) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [updated_at_utc] datetime2(0) NOT NULL CONSTRAINT [DF__workshop___updat__37D02F05] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [events].[workshop_meetings] ADD CONSTRAINT [PK__workshop__674D4A808ABF4CC8] PRIMARY KEY CLUSTERED ([occurrence_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE UNIQUE NONCLUSTERED INDEX [UX_workshop_zoom_meeting] ON [events].[workshop_meetings] ([meeting_id] ASC) WHERE ([meeting_id] IS NOT NULL) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[workshop_meetings] WITH CHECK ADD CONSTRAINT [CK_workshop_meeting_source] CHECK ([source]='manual' OR [source]='zoom');
GO
ALTER TABLE [events].[workshop_meetings] WITH CHECK ADD CONSTRAINT [CK_workshop_meeting_sync] CHECK ([sync_status]='cancelled' OR [sync_status]='failed' OR [sync_status]='uncertain' OR [sync_status]='ready' OR [sync_status]='working' OR [sync_status]='pending');
GO
ALTER TABLE [events].[workshop_meetings] WITH CHECK ADD CONSTRAINT [FK__workshop___occur__34F3C25A] FOREIGN KEY ([occurrence_id]) REFERENCES [events].[event_occurrences] ([occurrence_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
