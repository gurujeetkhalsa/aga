-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [events].[registration_occurrences] (
    [registration_occurrence_id] bigint IDENTITY(1,1) NOT NULL,
    [registration_id] uniqueidentifier NOT NULL,
    [occurrence_id] bigint NOT NULL,
    [status] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [assigned_at_utc] datetime2(0) NOT NULL,
    [cancelled_at_utc] datetime2(0) NULL,
    [attendance] varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_events_attendance] DEFAULT ('unrecorded'),
    [attendance_recorded_at_utc] datetime2(0) NULL,
    [attendance_recorded_by] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 );
GO
ALTER TABLE [events].[registration_occurrences] ADD CONSTRAINT [PK_events_registration_occurrences] PRIMARY KEY CLUSTERED ([registration_occurrence_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[registration_occurrences] ADD CONSTRAINT [UQ_events_registration_occurrences_choice] UNIQUE NONCLUSTERED ([registration_id] ASC, [occurrence_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_events_registration_occurrences_capacity] ON [events].[registration_occurrences] ([occurrence_id] ASC, [status] ASC, [assigned_at_utc] ASC) INCLUDE ([registration_id]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[registration_occurrences] WITH CHECK ADD CONSTRAINT [CK_events_attendance] CHECK ([attendance]='absent' OR [attendance]='attended' OR [attendance]='unrecorded');
GO
ALTER TABLE [events].[registration_occurrences] WITH CHECK ADD CONSTRAINT [CK_events_registration_occurrences_status] CHECK ([status]='cancelled' OR [status]='waitlisted' OR [status]='confirmed');
GO
ALTER TABLE [events].[registration_occurrences] WITH CHECK ADD CONSTRAINT [FK_events_registration_occurrences_registration] FOREIGN KEY ([registration_id]) REFERENCES [events].[registrations] ([registration_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [events].[registration_occurrences] WITH CHECK ADD CONSTRAINT [FK_events_registration_occurrences_occurrence] FOREIGN KEY ([occurrence_id]) REFERENCES [events].[event_occurrences] ([occurrence_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
