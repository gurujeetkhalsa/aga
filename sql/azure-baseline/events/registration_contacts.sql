-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [events].[registration_contacts] (
    [registration_id] uniqueidentifier NOT NULL,
    [first_name] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [last_name] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [email] nvarchar(254) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [city] nvarchar(120) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [state_province] nvarchar(120) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [country] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [postal_code] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ogs_handle] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 );
GO
ALTER TABLE [events].[registration_contacts] ADD CONSTRAINT [PK_events_registration_contacts] PRIMARY KEY CLUSTERED ([registration_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_events_registration_contacts_email] ON [events].[registration_contacts] ([email] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[registration_contacts] WITH CHECK ADD CONSTRAINT [FK_events_registration_contacts_registration] FOREIGN KEY ([registration_id]) REFERENCES [events].[registrations] ([registration_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
