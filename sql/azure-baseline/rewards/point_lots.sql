-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [rewards].[point_lots] (
    [LotID] bigint IDENTITY(1,1) NOT NULL,
    [Earn_TransactionID] bigint NOT NULL,
    [ChapterID] int NOT NULL,
    [Chapter_Code] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Original_Points] int NOT NULL,
    [Remaining_Points] int NOT NULL,
    [Earned_Date] date NOT NULL,
    [Expires_On] date NOT NULL,
    [Source_Type] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Source_Key] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_point_lots_Created_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [rewards].[point_lots] ADD CONSTRAINT [PK_point_lots] PRIMARY KEY CLUSTERED ([LotID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[point_lots] ADD CONSTRAINT [UQ_point_lots_Earn_TransactionID] UNIQUE NONCLUSTERED ([Earn_TransactionID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_point_lots_Chapter_Expires] ON [rewards].[point_lots] ([ChapterID] ASC, [Expires_On] ASC, [Earned_Date] ASC, [LotID] ASC) INCLUDE ([Chapter_Code], [Original_Points], [Remaining_Points]) WHERE ([Remaining_Points]>(0)) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[point_lots] WITH CHECK ADD CONSTRAINT [CK_point_lots_Original_Points] CHECK ([Original_Points]>(0));
GO
ALTER TABLE [rewards].[point_lots] WITH CHECK ADD CONSTRAINT [CK_point_lots_Remaining_Points] CHECK ([Remaining_Points]>=(0) AND [Remaining_Points]<=[Original_Points]);
GO
ALTER TABLE [rewards].[point_lots] WITH CHECK ADD CONSTRAINT [CK_point_lots_Expires_On] CHECK ([Expires_On]>=[Earned_Date]);
GO
ALTER TABLE [rewards].[point_lots] WITH CHECK ADD CONSTRAINT [FK_point_lots_Earn_TransactionID] FOREIGN KEY ([Earn_TransactionID]) REFERENCES [rewards].[transactions] ([TransactionID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
