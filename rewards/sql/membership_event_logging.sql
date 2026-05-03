SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'rewards') IS NULL
    EXEC(N'CREATE SCHEMA [rewards]');
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_record_membership_event]
    @MessageId nvarchar(256),
    @ReceivedAt datetime2(0),
    @AGAID int,
    @EventType nvarchar(32),
    @EventDate date,
    @MemberType nvarchar(128) = NULL,
    @SourcePayloadJson nvarchar(max) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF OBJECT_ID(N'rewards.membership_events', N'U') IS NULL
    BEGIN
        THROW 52000, N'rewards.membership_events does not exist. Apply rewards/sql/chapter_rewards_schema.sql before recording membership reward events.', 1;
    END;

    DECLARE @NormalizedMemberType nvarchar(128) = LTRIM(RTRIM(COALESCE(@MemberType, N'')));
    DECLARE @NormalizedEventType nvarchar(32) = LTRIM(RTRIM(COALESCE(@EventType, N'')));

    IF @NormalizedMemberType = N'Adult Full - Lifetime'
    BEGIN
        SET @NormalizedEventType = N'lifetime';
    END;

    IF @NormalizedEventType NOT IN (N'new_membership', N'renewal', N'lifetime')
    BEGIN
        THROW 52001, N'Unsupported rewards membership event type.', 1;
    END;

    DECLARE @BasePoints int =
        CASE @NormalizedMemberType
            WHEN N'Adult Full' THEN 5000
            WHEN N'Youth' THEN 2000
            WHEN N'Adult Full - Lifetime' THEN 25000
            ELSE 0
        END;

    DECLARE @Status nvarchar(32) =
        CASE
            WHEN @BasePoints > 0 THEN N'pending'
            ELSE N'ineligible'
        END;

    DECLARE @CreditDeadline date = DATEADD(day, 30, @EventDate);

    IF EXISTS
    (
        SELECT 1
        FROM [rewards].[membership_events] WITH (UPDLOCK, HOLDLOCK)
        WHERE [Message_ID] = @MessageId
          AND [AGAID] = @AGAID
          AND [Event_Type] = @NormalizedEventType
    )
    BEGIN
        UPDATE [rewards].[membership_events]
        SET
            [Received_At] =
                CASE WHEN [Status] IN (N'credited', N'expired_no_chapter') THEN [Received_At] ELSE @ReceivedAt END,
            [Event_Date] =
                CASE WHEN [Status] IN (N'credited', N'expired_no_chapter') THEN [Event_Date] ELSE @EventDate END,
            [Member_Type] =
                CASE WHEN [Status] IN (N'credited', N'expired_no_chapter') THEN [Member_Type] ELSE NULLIF(@NormalizedMemberType, N'') END,
            [Base_Points] =
                CASE WHEN [Status] IN (N'credited', N'expired_no_chapter') THEN [Base_Points] ELSE @BasePoints END,
            [Term_Years] =
                CASE WHEN [Status] IN (N'credited', N'expired_no_chapter') THEN [Term_Years] ELSE 1 END,
            [Credit_Deadline] =
                CASE WHEN [Status] IN (N'credited', N'expired_no_chapter') THEN [Credit_Deadline] ELSE @CreditDeadline END,
            [Status] =
                CASE
                    WHEN [Status] IN (N'credited', N'expired_no_chapter') THEN [Status]
                    ELSE @Status
                END,
            [Source_Payload_Json] = @SourcePayloadJson,
            [Updated_At] = SYSUTCDATETIME()
        WHERE [Message_ID] = @MessageId
          AND [AGAID] = @AGAID
          AND [Event_Type] = @NormalizedEventType;
    END;
    ELSE
    BEGIN
        INSERT INTO [rewards].[membership_events]
        (
            [Message_ID],
            [AGAID],
            [Event_Type],
            [Event_Date],
            [Received_At],
            [Member_Type],
            [Base_Points],
            [Term_Years],
            [Credit_Deadline],
            [Status],
            [Source_Payload_Json]
        )
        VALUES
        (
            @MessageId,
            @AGAID,
            @NormalizedEventType,
            @EventDate,
            @ReceivedAt,
            NULLIF(@NormalizedMemberType, N''),
            @BasePoints,
            1,
            @CreditDeadline,
            @Status,
            @SourcePayloadJson
        );
    END;
END;
GO
