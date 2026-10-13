-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [rewards].[chapter_transfers] (
    [TransferID] bigint IDENTITY(1,1) NOT NULL,
    [External_Transfer_ID] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [From_ChapterID] int NOT NULL,
    [From_Chapter_Code] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [From_Chapter_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [To_ChapterID] int NOT NULL,
    [To_Chapter_Code] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [To_Chapter_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Transfer_Date] date NOT NULL,
    [Points] int NOT NULL,
    [Description] nvarchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Notes] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Status] nvarchar(32) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_chapter_transfers_Status] DEFAULT (N'pending'),
    [Source_Payload_Json] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [RunID] int NULL,
    [Out_TransactionID] bigint NULL,
    [Posted_At] datetime2(0) NULL,
    [Posted_By_Principal_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Posted_By_Principal_Id] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_transfers_Created_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [rewards].[chapter_transfers] ADD CONSTRAINT [PK_chapter_transfers] PRIMARY KEY CLUSTERED ([TransferID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[chapter_transfers] ADD CONSTRAINT [UQ_chapter_transfers_External] UNIQUE NONCLUSTERED ([External_Transfer_ID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_transfers_From_Date] ON [rewards].[chapter_transfers] ([From_ChapterID] ASC, [Transfer_Date] DESC, [TransferID] DESC) INCLUDE ([To_ChapterID], [Points], [Status], [Out_TransactionID]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_chapter_transfers_To_Date] ON [rewards].[chapter_transfers] ([To_ChapterID] ASC, [Transfer_Date] DESC, [TransferID] DESC) INCLUDE ([From_ChapterID], [Points], [Status]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[chapter_transfers] WITH CHECK ADD CONSTRAINT [CK_chapter_transfers_Points] CHECK ([Points]>(0));
GO
ALTER TABLE [rewards].[chapter_transfers] WITH CHECK ADD CONSTRAINT [CK_chapter_transfers_Different_Chapters] CHECK ([From_ChapterID]<>[To_ChapterID]);
GO
ALTER TABLE [rewards].[chapter_transfers] WITH CHECK ADD CONSTRAINT [CK_chapter_transfers_Status] CHECK ([Status]=N'posted' OR [Status]=N'pending');
GO
ALTER TABLE [rewards].[chapter_transfers] WITH CHECK ADD CONSTRAINT [CK_chapter_transfers_Source_Payload] CHECK ([Source_Payload_Json] IS NULL OR isjson([Source_Payload_Json])=(1));
GO
ALTER TABLE [rewards].[chapter_transfers] WITH CHECK ADD CONSTRAINT [FK_chapter_transfers_RunID] FOREIGN KEY ([RunID]) REFERENCES [rewards].[reward_runs] ([RunID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [rewards].[chapter_transfers] WITH CHECK ADD CONSTRAINT [FK_chapter_transfers_Out_TransactionID] FOREIGN KEY ([Out_TransactionID]) REFERENCES [rewards].[transactions] ([TransactionID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
