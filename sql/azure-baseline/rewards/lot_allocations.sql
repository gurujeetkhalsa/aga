-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [rewards].[lot_allocations] (
    [AllocationID] bigint IDENTITY(1,1) NOT NULL,
    [Debit_TransactionID] bigint NOT NULL,
    [LotID] bigint NOT NULL,
    [Points_Allocated] int NOT NULL,
    [Allocated_At] datetime2(0) NOT NULL CONSTRAINT [DF_lot_allocations_Allocated_At] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [rewards].[lot_allocations] ADD CONSTRAINT [PK_lot_allocations] PRIMARY KEY CLUSTERED ([AllocationID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[lot_allocations] ADD CONSTRAINT [UQ_lot_allocations_Debit_Lot] UNIQUE NONCLUSTERED ([Debit_TransactionID] ASC, [LotID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_lot_allocations_LotID] ON [rewards].[lot_allocations] ([LotID] ASC, [Debit_TransactionID] ASC) INCLUDE ([Points_Allocated]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[lot_allocations] WITH CHECK ADD CONSTRAINT [CK_lot_allocations_Points_Allocated] CHECK ([Points_Allocated]>(0));
GO
ALTER TABLE [rewards].[lot_allocations] WITH CHECK ADD CONSTRAINT [FK_lot_allocations_Debit_TransactionID] FOREIGN KEY ([Debit_TransactionID]) REFERENCES [rewards].[transactions] ([TransactionID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [rewards].[lot_allocations] WITH CHECK ADD CONSTRAINT [FK_lot_allocations_LotID] FOREIGN KEY ([LotID]) REFERENCES [rewards].[point_lots] ([LotID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
