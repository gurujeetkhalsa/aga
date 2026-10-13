-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [rewards].[redemption_receipts] (
    [ReceiptID] bigint IDENTITY(1,1) NOT NULL,
    [RedemptionID] bigint NOT NULL,
    [Posted_TransactionID] bigint NULL,
    [ChapterID] int NOT NULL,
    [Chapter_Code] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Blob_Container] varchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Blob_Name] varchar(512) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Original_File_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Content_Type] varchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Content_Length] bigint NOT NULL,
    [Sha256_Hex] char(64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Uploaded_At] datetime2(0) NOT NULL CONSTRAINT [DF_redemption_receipts_Uploaded_At] DEFAULT (sysutcdatetime()),
    [Uploaded_By_Principal_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Uploaded_By_Principal_Id] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Is_Deleted] bit NOT NULL CONSTRAINT [DF_redemption_receipts_Is_Deleted] DEFAULT ((0)),
    [Deleted_At] datetime2(0) NULL,
    [Deleted_By_Principal_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Deleted_By_Principal_Id] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 );
GO
ALTER TABLE [rewards].[redemption_receipts] ADD CONSTRAINT [PK_redemption_receipts] PRIMARY KEY CLUSTERED ([ReceiptID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[redemption_receipts] ADD CONSTRAINT [UQ_redemption_receipts_Blob] UNIQUE NONCLUSTERED ([Blob_Container] ASC, [Blob_Name] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_redemption_receipts_Redemption_Active] ON [rewards].[redemption_receipts] ([RedemptionID] ASC, [Is_Deleted] ASC, [ReceiptID] ASC) INCLUDE ([Original_File_Name], [Content_Type], [Content_Length], [Uploaded_At]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [rewards].[redemption_receipts] WITH CHECK ADD CONSTRAINT [CK_redemption_receipts_Content_Length] CHECK ([Content_Length]>(0));
GO
ALTER TABLE [rewards].[redemption_receipts] WITH CHECK ADD CONSTRAINT [FK_redemption_receipts_RedemptionID] FOREIGN KEY ([RedemptionID]) REFERENCES [rewards].[redemption_requests] ([RedemptionID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
ALTER TABLE [rewards].[redemption_receipts] WITH CHECK ADD CONSTRAINT [FK_redemption_receipts_Posted_TransactionID] FOREIGN KEY ([Posted_TransactionID]) REFERENCES [rewards].[transactions] ([TransactionID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
