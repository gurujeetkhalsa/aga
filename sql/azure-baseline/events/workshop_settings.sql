-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [events].[workshop_settings] (
    [event_id] bigint NOT NULL,
    [sender_email] nvarchar(254) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF__workshop___sende__2B6A5820] DEFAULT ('milan.mladenovic@usgo.org'),
    [instructor_name] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF__workshop___instr__2C5E7C59] DEFAULT ('Milan'),
    [reminder_hours] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF__workshop___remin__2D52A092] DEFAULT ('[24,1]'),
    [followup_hours] int NOT NULL CONSTRAINT [DF__workshop___follo__2E46C4CB] DEFAULT ((24)),
    [followup_text] nvarchar(4000) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF__workshop___follo__2F3AE904] DEFAULT ('Thank you for your interest in Go! Explore resources and find your local chapter at https://www.usgo.org/.'),
    [email_enabled] bit NOT NULL CONSTRAINT [DF__workshop___email__302F0D3D] DEFAULT ((0)),
    [zoom_host_email] nvarchar(254) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [is_test] bit NOT NULL CONSTRAINT [DF_workshop_is_test] DEFAULT ((0))
 );
GO
ALTER TABLE [events].[workshop_settings] ADD CONSTRAINT [PK__workshop__2370F727067CA724] PRIMARY KEY CLUSTERED ([event_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[workshop_settings] WITH CHECK ADD CONSTRAINT [CK_workshop_reminders] CHECK (isjson([reminder_hours])=(1));
GO
ALTER TABLE [events].[workshop_settings] WITH CHECK ADD CONSTRAINT [CK_workshop_followup] CHECK ([followup_hours]>=(0) AND [followup_hours]<=(720));
GO
ALTER TABLE [events].[workshop_settings] WITH CHECK ADD CONSTRAINT [FK__workshop___event__2A7633E7] FOREIGN KEY ([event_id]) REFERENCES [events].[events] ([event_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
