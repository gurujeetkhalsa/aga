-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [events].[event_versions] (
    [event_version_id] bigint IDENTITY(1,1) NOT NULL,
    [event_id] bigint NOT NULL,
    [version_number] int NOT NULL,
    [form_definition_json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_events_versions_form] DEFAULT (N'{"pages":[]}'),
    [policy_snapshot_json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_events_versions_policy] DEFAULT (N'{}'),
    [published_at_utc] datetime2(0) NULL,
    [created_at_utc] datetime2(0) NOT NULL CONSTRAINT [DF_events_versions_created] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [events].[event_versions] ADD CONSTRAINT [PK_events_event_versions] PRIMARY KEY CLUSTERED ([event_version_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[event_versions] ADD CONSTRAINT [UQ_events_versions_number] UNIQUE NONCLUSTERED ([event_id] ASC, [version_number] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[event_versions] WITH CHECK ADD CONSTRAINT [CK_events_versions_form_json] CHECK (isjson([form_definition_json])=(1));
GO
ALTER TABLE [events].[event_versions] WITH CHECK ADD CONSTRAINT [CK_events_versions_policy_json] CHECK (isjson([policy_snapshot_json])=(1));
GO
ALTER TABLE [events].[event_versions] WITH CHECK ADD CONSTRAINT [FK_events_versions_event] FOREIGN KEY ([event_id]) REFERENCES [events].[events] ([event_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
