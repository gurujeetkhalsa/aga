-- Copyright 2026, American Go Association, All rights reserved

-- Live Azure SQL stored procedure export.
-- Source object: [rewards].[sp_record_chapter_renewal_confirmation].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_record_chapter_renewal_confirmation]
    @MessageId nvarchar(256),
    @ReceivedAt datetime2(0),
    @ChapterID int,
    @MemberType nvarchar(128) = NULL,
    @SourcePayloadJson nvarchar(max) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'rewards.chapter_renewal_notice_results', N'U') IS NULL
    BEGIN
        THROW 53106, N'rewards.chapter_renewal_notice_results does not exist. Apply chapter renewal notice processing SQL first.', 1;
    END;

    DECLARE @NormalizedMemberType nvarchar(128) = LTRIM(RTRIM(COALESCE(@MemberType, N'')));

    IF @NormalizedMemberType NOT LIKE N'Chapter%'
    BEGIN
        SELECT
            CAST(0 AS bit) AS [Recorded],
            @MessageId AS [ClubExpress_Renewal_Message_ID],
            @ChapterID AS [ChapterID],
            CAST(NULL AS bigint) AS [NoticeID],
            CAST(NULL AS nvarchar(64)) AS [Chapter_Code],
            CAST(NULL AS nvarchar(256)) AS [Chapter_Name],
            N'non_chapter_member_type' AS [Reason];
        RETURN;
    END;

    DECLARE @Recorded table
    (
        [Recorded] bit NOT NULL,
        [ClubExpress_Renewal_Message_ID] nvarchar(256) NOT NULL,
        [ChapterID] int NOT NULL,
        [NoticeID] bigint NOT NULL,
        [Chapter_Code] nvarchar(64) NULL,
        [Chapter_Name] nvarchar(256) NULL,
        [Reason] nvarchar(64) NULL
    );

    ;WITH [pending] AS
    (
        SELECT TOP (1)
            *
        FROM [rewards].[chapter_renewal_notice_results] WITH (UPDLOCK, HOLDLOCK)
        WHERE [ChapterID] = @ChapterID
          AND [Decision] IN (N'posted', N'already_posted')
          AND [TransactionID] IS NOT NULL
          AND [ClubExpress_Renewal_Message_ID] IS NULL
          AND [Received_At] <= @ReceivedAt
        ORDER BY [Notice_Date], [NoticeID]
    )
    UPDATE [pending]
    SET
        [ClubExpress_Renewal_Message_ID] = @MessageId,
        [ClubExpress_Renewed_At] = @ReceivedAt,
        [ClubExpress_Renewal_Recorded_At] = SYSUTCDATETIME(),
        [ClubExpress_Renewal_Source_Payload_Json] = @SourcePayloadJson
    OUTPUT
        CAST(1 AS bit) AS [Recorded],
        INSERTED.[ClubExpress_Renewal_Message_ID],
        INSERTED.[ChapterID],
        INSERTED.[NoticeID],
        INSERTED.[Chapter_Code],
        INSERTED.[Chapter_Name],
        CAST(NULL AS nvarchar(64)) AS [Reason]
    INTO @Recorded;

    IF EXISTS (SELECT 1 FROM @Recorded)
    BEGIN
        SELECT
            [Recorded],
            [ClubExpress_Renewal_Message_ID],
            [ChapterID],
            [NoticeID],
            [Chapter_Code],
            [Chapter_Name],
            [Reason]
        FROM @Recorded;
    END
    ELSE
    BEGIN
        SELECT
            CAST(0 AS bit) AS [Recorded],
            @MessageId AS [ClubExpress_Renewal_Message_ID],
            @ChapterID AS [ChapterID],
            CAST(NULL AS bigint) AS [NoticeID],
            CAST(NULL AS nvarchar(64)) AS [Chapter_Code],
            CAST(NULL AS nvarchar(256)) AS [Chapter_Name],
            N'no_pending_debit' AS [Reason];
    END;
END;
GO
