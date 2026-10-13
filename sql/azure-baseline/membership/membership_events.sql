-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [membership].[membership_events] (
    [EventID] int IDENTITY(1,1) NOT NULL,
    [AGAID] int NOT NULL,
    [EventType] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [EventDate] datetime2(7) NOT NULL CONSTRAINT [DF__membershi__Event__787EE5A0] DEFAULT (sysdatetime()),
    [Source] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Details] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [OldExpiration] date NULL,
    [NewExpiration] date NULL,
    [CreatedDate] datetime2(7) NULL CONSTRAINT [DF__membershi__Creat__797309D9] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [membership].[membership_events] ADD CONSTRAINT [PK__membersh__7944C870A83F10F1] PRIMARY KEY CLUSTERED ([EventID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_membership_events_AGAID_EventDate] ON [membership].[membership_events] ([AGAID] ASC, [EventDate] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_membership_events_EventType] ON [membership].[membership_events] ([EventType] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [membership].[membership_events] WITH CHECK ADD CONSTRAINT [FK_membership_events_member] FOREIGN KEY ([AGAID]) REFERENCES [membership].[members] ([AGAID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
