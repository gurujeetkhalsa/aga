-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [rewards].[redemption_requests] (
    [RedemptionID] bigint IDENTITY(1,1) NOT NULL,
    [External_Request_ID] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ChapterID] int NOT NULL,
    [Chapter_Code] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Chapter_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Request_Date] date NOT NULL,
    [Points] int NOT NULL,
    [Amount_USD] decimal(12,3) NOT NULL,
    [Redemption_Category] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Payment_Mode] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Description] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Receipt_Reference] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Legacy_Gap] bit NOT NULL CONSTRAINT [DF_redemption_requests_Legacy_Gap] DEFAULT ((0)),
    [Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_redemption_requests_Status] DEFAULT (N'draft'),
    [Posted_TransactionID] bigint NULL,
    [Source_Payload_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Notes] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_redemption_requests_Created_At] DEFAULT (sysutcdatetime()),
    [Created_By_Principal_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_By_Principal_Id] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Posted_At] datetime2(0) NULL,
    [Posted_By_Principal_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Posted_By_Principal_Id] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Cancelled_At] datetime2(0) NULL,
    [Cancelled_By_Principal_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Cancelled_By_Principal_Id] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Notes_Updated_At] datetime2(0) NULL,
    [Notes_Updated_By_Principal_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Notes_Updated_By_Principal_Id] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 );
GO
ALTER TABLE [rewards].[redemption_requests] ADD CONSTRAINT [PK_redemption_requests] PRIMARY KEY CLUSTERED ([RedemptionID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE UNIQUE NONCLUSTERED INDEX [UX_redemption_requests_External_Request_ID] ON [rewards].[redemption_requests] ([External_Request_ID] ASC) WHERE ([External_Request_ID] IS NOT NULL) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_redemption_requests_Chapter_Date] ON [rewards].[redemption_requests] ([ChapterID] ASC, [Request_Date] ASC, [RedemptionID] ASC) INCLUDE ([Chapter_Code], [Points], [Status], [Legacy_Gap]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[redemption_requests] WITH CHECK ADD CONSTRAINT [CK_redemption_requests_Points] CHECK ([Points]>(0));
GO
ALTER TABLE [rewards].[redemption_requests] WITH CHECK ADD CONSTRAINT [CK_redemption_requests_Amount_USD] CHECK ([Amount_USD]>(0));
GO
ALTER TABLE [rewards].[redemption_requests] WITH CHECK ADD CONSTRAINT [CK_redemption_requests_Category] CHECK ([Redemption_Category]=N'other' OR [Redemption_Category]=N'go_promotion' OR [Redemption_Category]=N'chapter_renewal');
GO
ALTER TABLE [rewards].[redemption_requests] WITH CHECK ADD CONSTRAINT [CK_redemption_requests_Payment_Mode] CHECK ([Payment_Mode]=N'other' OR [Payment_Mode]=N'reimbursement' OR [Payment_Mode]=N'dues_credit');
GO
ALTER TABLE [rewards].[redemption_requests] WITH CHECK ADD CONSTRAINT [CK_redemption_requests_Status] CHECK ([Status]=N'cancelled' OR [Status]=N'rejected' OR [Status]=N'posted' OR [Status]=N'approved' OR [Status]=N'draft');
GO
ALTER TABLE [rewards].[redemption_requests] WITH CHECK ADD CONSTRAINT [FK_redemption_requests_Posted_TransactionID] FOREIGN KEY ([Posted_TransactionID]) REFERENCES [rewards].[transactions] ([TransactionID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
