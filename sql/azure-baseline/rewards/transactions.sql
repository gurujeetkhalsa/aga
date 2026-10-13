-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [rewards].[transactions] (
    [TransactionID] bigint IDENTITY(1,1) NOT NULL,
    [ChapterID] int NOT NULL,
    [Chapter_Code] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Transaction_Type] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Points_Delta] int NOT NULL,
    [Base_Points] int NULL,
    [Multiplier] tinyint NULL,
    [Chapter_Active_Member_Count] int NULL,
    [Effective_Date] date NOT NULL,
    [Earned_Date] date NULL,
    [Valuation_Date] date NULL,
    [Posted_At] datetime2(0) NOT NULL CONSTRAINT [DF_transactions_Posted_At] DEFAULT (sysutcdatetime()),
    [RunID] int NULL,
    [Source_Type] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Source_Key] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Rule_Version] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_transactions_Rule_Version] DEFAULT (N'2026-05-02'),
    [MetadataJson] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_By] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL CONSTRAINT [DF_transactions_Created_By] DEFAULT (suser_sname())
 );
GO
ALTER TABLE [rewards].[transactions] ADD CONSTRAINT [PK_transactions] PRIMARY KEY CLUSTERED ([TransactionID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[transactions] ADD CONSTRAINT [UQ_transactions_Source] UNIQUE NONCLUSTERED ([Source_Type] ASC, [Source_Key] ASC, [Transaction_Type] ASC, [ChapterID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_transactions_Chapter_Date] ON [rewards].[transactions] ([ChapterID] ASC, [Effective_Date] ASC, [TransactionID] ASC) INCLUDE ([Chapter_Code], [Transaction_Type], [Points_Delta], [Earned_Date], [Source_Type], [Source_Key]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_transactions_RunID] ON [rewards].[transactions] ([RunID] ASC, [Transaction_Type] ASC, [TransactionID] ASC) WHERE ([RunID] IS NOT NULL) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[transactions] WITH CHECK ADD CONSTRAINT [CK_transactions_Type] CHECK ([Transaction_Type]=N'reversal' OR [Transaction_Type]=N'adjustment' OR [Transaction_Type]=N'transfer_out' OR [Transaction_Type]=N'transfer_in' OR [Transaction_Type]=N'expire' OR [Transaction_Type]=N'redeem' OR [Transaction_Type]=N'earn');
GO
ALTER TABLE [rewards].[transactions] WITH CHECK ADD CONSTRAINT [CK_transactions_Points_Delta_Nonzero] CHECK ([Points_Delta]<>(0));
GO
ALTER TABLE [rewards].[transactions] WITH CHECK ADD CONSTRAINT [CK_transactions_Points_Delta_Sign] CHECK (([Transaction_Type]=N'transfer_in' OR [Transaction_Type]=N'earn') AND [Points_Delta]>(0) OR ([Transaction_Type]=N'transfer_out' OR [Transaction_Type]=N'expire' OR [Transaction_Type]=N'redeem') AND [Points_Delta]<(0) OR ([Transaction_Type]=N'reversal' OR [Transaction_Type]=N'adjustment'));
GO
ALTER TABLE [rewards].[transactions] WITH CHECK ADD CONSTRAINT [CK_transactions_Earned_Date] CHECK (NOT ([Transaction_Type]=N'transfer_in' OR [Transaction_Type]=N'earn') OR [Earned_Date] IS NOT NULL);
GO
ALTER TABLE [rewards].[transactions] WITH CHECK ADD CONSTRAINT [CK_transactions_Multiplier] CHECK ([Multiplier] IS NULL OR ([Multiplier]=(3) OR [Multiplier]=(2) OR [Multiplier]=(1)));
GO
ALTER TABLE [rewards].[transactions] WITH CHECK ADD CONSTRAINT [CK_transactions_Chapter_Active_Member_Count] CHECK ([Chapter_Active_Member_Count] IS NULL OR [Chapter_Active_Member_Count]>=(0));
GO
ALTER TABLE [rewards].[transactions] WITH CHECK ADD CONSTRAINT [FK_transactions_RunID] FOREIGN KEY ([RunID]) REFERENCES [rewards].[reward_runs] ([RunID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
