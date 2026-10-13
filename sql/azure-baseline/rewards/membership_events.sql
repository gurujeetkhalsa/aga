-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [rewards].[membership_events] (
    [Membership_Event_ID] bigint IDENTITY(1,1) NOT NULL,
    [Message_ID] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [AGAID] int NOT NULL,
    [Event_Type] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Event_Date] date NOT NULL,
    [Received_At] datetime2(0) NOT NULL,
    [Member_Type] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Base_Points] int NOT NULL,
    [Term_Years] int NOT NULL CONSTRAINT [DF_membership_events_Term_Years] DEFAULT ((1)),
    [Credit_Deadline] date NOT NULL,
    [Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_membership_events_Status] DEFAULT (N'pending'),
    [Credited_TransactionID] bigint NULL,
    [Expired_At] datetime2(0) NULL,
    [Source_Payload_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_membership_events_Created_At] DEFAULT (sysutcdatetime()),
    [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_membership_events_Updated_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [rewards].[membership_events] ADD CONSTRAINT [PK_membership_events] PRIMARY KEY CLUSTERED ([Membership_Event_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[membership_events] ADD CONSTRAINT [UQ_membership_events_Message_AGAID_Type] UNIQUE NONCLUSTERED ([Message_ID] ASC, [AGAID] ASC, [Event_Type] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_membership_events_Status_Deadline] ON [rewards].[membership_events] ([Status] ASC, [Credit_Deadline] ASC, [Event_Date] ASC) INCLUDE ([AGAID], [Event_Type], [Member_Type], [Base_Points]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_membership_events_AGAID] ON [rewards].[membership_events] ([AGAID] ASC, [Event_Date] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[membership_events] WITH CHECK ADD CONSTRAINT [CK_membership_events_Event_Type] CHECK ([Event_Type]=N'lifetime' OR [Event_Type]=N'renewal' OR [Event_Type]=N'new_membership');
GO
ALTER TABLE [rewards].[membership_events] WITH CHECK ADD CONSTRAINT [CK_membership_events_Status] CHECK ([Status]=N'ineligible' OR [Status]=N'expired_no_chapter' OR [Status]=N'credited' OR [Status]=N'pending');
GO
ALTER TABLE [rewards].[membership_events] WITH CHECK ADD CONSTRAINT [CK_membership_events_Base_Points] CHECK ([Base_Points]>=(0));
GO
ALTER TABLE [rewards].[membership_events] WITH CHECK ADD CONSTRAINT [CK_membership_events_Term_Years] CHECK ([Term_Years]>=(1));
GO
ALTER TABLE [rewards].[membership_events] WITH CHECK ADD CONSTRAINT [CK_membership_events_Credit_Deadline] CHECK ([Credit_Deadline]>=[Event_Date]);
GO
ALTER TABLE [rewards].[membership_events] WITH CHECK ADD CONSTRAINT [FK_membership_events_Credited_TransactionID] FOREIGN KEY ([Credited_TransactionID]) REFERENCES [rewards].[transactions] ([TransactionID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
