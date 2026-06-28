-- Live Azure SQL stored procedure export.
-- Source object: [rewards].[sp_backfill_chapter_renewal_confirmations].
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_backfill_chapter_renewal_confirmations]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'rewards.chapter_renewal_notice_results', N'U') IS NULL
       OR OBJECT_ID(N'rewards.membership_events', N'U') IS NULL
    BEGIN
        THROW 53107, N'Chapter renewal notice results or membership events table does not exist.', 1;
    END;

    ;WITH [matches] AS
    (
        SELECT
            notice.[NoticeID],
            event.[Message_ID],
            event.[Received_At],
            event.[Source_Payload_Json],
            ROW_NUMBER() OVER
            (
                PARTITION BY notice.[NoticeID]
                ORDER BY event.[Received_At], event.[Membership_Event_ID]
            ) AS rn
        FROM [rewards].[chapter_renewal_notice_results] AS notice
        INNER JOIN [rewards].[membership_events] AS event
            ON event.[AGAID] = notice.[ChapterID]
           AND event.[Event_Type] = N'renewal'
           AND LTRIM(RTRIM(COALESCE(event.[Member_Type], N''))) LIKE N'Chapter%'
           AND event.[Received_At] >= notice.[Received_At]
        WHERE notice.[Decision] IN (N'posted', N'already_posted')
          AND notice.[TransactionID] IS NOT NULL
          AND notice.[ClubExpress_Renewal_Message_ID] IS NULL
    )
    UPDATE notice
    SET
        [ClubExpress_Renewal_Message_ID] = matches.[Message_ID],
        [ClubExpress_Renewed_At] = matches.[Received_At],
        [ClubExpress_Renewal_Recorded_At] = SYSUTCDATETIME(),
        [ClubExpress_Renewal_Source_Payload_Json] = matches.[Source_Payload_Json]
    FROM [rewards].[chapter_renewal_notice_results] AS notice
    INNER JOIN [matches]
        ON matches.[NoticeID] = notice.[NoticeID]
       AND matches.rn = 1;

    SELECT @@ROWCOUNT AS [UpdatedCount];
END;
GO
