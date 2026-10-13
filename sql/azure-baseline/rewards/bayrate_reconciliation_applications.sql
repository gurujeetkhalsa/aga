-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [rewards].[bayrate_reconciliation_applications] (
    [Bayrate_RunID] int NOT NULL,
    [Reward_RunID] int NOT NULL,
    [Application_Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Effective_Date] date NOT NULL,
    [Applied_At] datetime2(0) NOT NULL CONSTRAINT [DF_bayrate_reconciliation_applications_Applied_At] DEFAULT (sysutcdatetime()),
    [Applied_By] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Applied_Principal_Id] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Transaction_Count] int NOT NULL,
    [Credit_Points] int NOT NULL,
    [Debit_Points] int NOT NULL,
    [Net_Points] int NOT NULL,
    [SummaryJson] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL
 );
GO
ALTER TABLE [rewards].[bayrate_reconciliation_applications] ADD CONSTRAINT [PK_bayrate_reconciliation_applications] PRIMARY KEY CLUSTERED ([Bayrate_RunID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[bayrate_reconciliation_applications] ADD CONSTRAINT [UQ_bayrate_reconciliation_applications_Reward_RunID] UNIQUE NONCLUSTERED ([Reward_RunID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[bayrate_reconciliation_applications] WITH CHECK ADD CONSTRAINT [CK_bayrate_reconciliation_applications_Status] CHECK ([Application_Status]=N'no_changes' OR [Application_Status]=N'applied');
GO
ALTER TABLE [rewards].[bayrate_reconciliation_applications] WITH CHECK ADD CONSTRAINT [CK_bayrate_reconciliation_applications_Counts] CHECK ([Transaction_Count]>=(0) AND [Credit_Points]>=(0) AND [Debit_Points]>=(0));
GO
ALTER TABLE [rewards].[bayrate_reconciliation_applications] WITH CHECK ADD CONSTRAINT [CK_bayrate_reconciliation_applications_SummaryJson] CHECK (isjson([SummaryJson])=(1));
GO
ALTER TABLE [rewards].[bayrate_reconciliation_applications] WITH CHECK ADD CONSTRAINT [FK_bayrate_reconciliation_applications_Reward_RunID] FOREIGN KEY ([Reward_RunID]) REFERENCES [rewards].[reward_runs] ([RunID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [rewards].[bayrate_reconciliation_applications] WITH CHECK ADD CONSTRAINT [FK_bayrate_reconciliation_applications_Bayrate_RunID] FOREIGN KEY ([Bayrate_RunID]) REFERENCES [ratings].[bayrate_runs] ([RunID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
