-- Copyright 2026, American Go Association, All rights reserved

-- Live Azure SQL stored procedure export.
-- Source object: [rewards].[sp_add_redemption_receipt].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_add_redemption_receipt]
    @RedemptionID bigint,
    @BlobContainer nvarchar(128),
    @BlobName nvarchar(512),
    @OriginalFileName nvarchar(256),
    @ContentType nvarchar(128),
    @ContentLength bigint,
    @Sha256Hex char(64) = NULL,
    @UploadedByPrincipalName nvarchar(256) = NULL,
    @UploadedByPrincipalId nvarchar(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'rewards.redemption_receipts', N'U') IS NULL
        THROW 52920, N'Rewards redemption receipt table does not exist. Apply rewards/sql/redemption_processing.sql first.', 1;

    IF @ContentLength IS NULL OR @ContentLength <= 0
        THROW 52921, N'Receipt content length must be positive.', 1;

    IF NULLIF(LTRIM(RTRIM(@BlobContainer)), N'') IS NULL
       OR NULLIF(LTRIM(RTRIM(@BlobName)), N'') IS NULL
       OR NULLIF(LTRIM(RTRIM(@OriginalFileName)), N'') IS NULL
       OR NULLIF(LTRIM(RTRIM(@ContentType)), N'') IS NULL
    BEGIN
        THROW 52922, N'Receipt metadata is incomplete.', 1;
    END;

    DECLARE
        @PostedTransactionID bigint,
        @ChapterID int,
        @ChapterCode nvarchar(64);

    SELECT TOP 1
        @PostedTransactionID = request.[Posted_TransactionID],
        @ChapterID = request.[ChapterID],
        @ChapterCode = request.[Chapter_Code]
    FROM [rewards].[redemption_requests] AS request
    WHERE request.[RedemptionID] = @RedemptionID;

    IF @ChapterID IS NULL
        THROW 52923, N'Redemption request was not found for receipt upload.', 1;

    DECLARE @Inserted table ([ReceiptID] bigint NOT NULL);

    INSERT INTO [rewards].[redemption_receipts]
    (
        [RedemptionID],
        [Posted_TransactionID],
        [ChapterID],
        [Chapter_Code],
        [Blob_Container],
        [Blob_Name],
        [Original_File_Name],
        [Content_Type],
        [Content_Length],
        [Sha256_Hex],
        [Uploaded_By_Principal_Name],
        [Uploaded_By_Principal_Id]
    )
    OUTPUT INSERTED.[ReceiptID] INTO @Inserted ([ReceiptID])
    VALUES
    (
        @RedemptionID,
        @PostedTransactionID,
        @ChapterID,
        @ChapterCode,
        LTRIM(RTRIM(@BlobContainer)),
        LTRIM(RTRIM(@BlobName)),
        LTRIM(RTRIM(@OriginalFileName)),
        LTRIM(RTRIM(@ContentType)),
        @ContentLength,
        @Sha256Hex,
        @UploadedByPrincipalName,
        @UploadedByPrincipalId
    );

    SELECT
        receipt.[ReceiptID],
        receipt.[RedemptionID],
        receipt.[Posted_TransactionID],
        receipt.[ChapterID],
        receipt.[Chapter_Code],
        receipt.[Blob_Container],
        receipt.[Blob_Name],
        receipt.[Original_File_Name],
        receipt.[Content_Type],
        receipt.[Content_Length],
        receipt.[Uploaded_At],
        receipt.[Uploaded_By_Principal_Name],
        receipt.[Is_Deleted]
    FROM [rewards].[redemption_receipts] AS receipt
    INNER JOIN @Inserted AS inserted
        ON inserted.[ReceiptID] = receipt.[ReceiptID];
END;
GO
