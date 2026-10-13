-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [events].[organizer_memberships] (
    [organizer_membership_id] bigint IDENTITY(1,1) NOT NULL,
    [organizer_id] bigint NOT NULL,
    [principal_object_id] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [can_manage_events] bit NOT NULL CONSTRAINT [DF_events_memberships_manage] DEFAULT ((0)),
    [can_manage_registrations] bit NOT NULL CONSTRAINT [DF_events_memberships_registrations] DEFAULT ((0)),
    [can_view_finance] bit NOT NULL CONSTRAINT [DF_events_memberships_finance] DEFAULT ((0)),
    [is_active] bit NOT NULL CONSTRAINT [DF_events_memberships_active] DEFAULT ((1)),
    [created_at_utc] datetime2(0) NOT NULL CONSTRAINT [DF_events_memberships_created] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [events].[organizer_memberships] ADD CONSTRAINT [PK_events_organizer_memberships] PRIMARY KEY CLUSTERED ([organizer_membership_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[organizer_memberships] ADD CONSTRAINT [UQ_events_memberships_scope] UNIQUE NONCLUSTERED ([organizer_id] ASC, [principal_object_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[organizer_memberships] WITH CHECK ADD CONSTRAINT [FK_events_memberships_organizer] FOREIGN KEY ([organizer_id]) REFERENCES [events].[organizers] ([organizer_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
