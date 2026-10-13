-- Live Azure SQL metadata; database aga-membership; VIEW
-- Review dependencies before use. Not a migration to PostgreSQL.
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE   VIEW [rewards].[v_membership_event_audit]
AS
SELECT
    e.[Membership_Event_ID],
    e.[Message_ID],
    e.[AGAID],
    e.[Event_Type],
    e.[Event_Date],
    e.[Received_At],
    e.[Member_Type],
    e.[Base_Points],
    e.[Term_Years],
    e.[Credit_Deadline],
    e.[Status],
    e.[Credited_TransactionID],
    t.[ChapterID] AS [Credited_ChapterID],
    t.[Chapter_Code] AS [Credited_Chapter_Code],
    t.[Points_Delta] AS [Credited_Points],
    t.[Multiplier] AS [Credited_Multiplier],
    t.[Chapter_Active_Member_Count] AS [Credited_Chapter_Active_Member_Count],
    t.[Effective_Date] AS [Credited_Effective_Date],
    t.[Posted_At] AS [Credited_Posted_At],
    t.[RunID] AS [Credited_RunID],
    e.[Expired_At],
    e.[Created_At],
    e.[Updated_At],
    e.[Source_Payload_Json]
FROM [rewards].[membership_events] AS e
LEFT JOIN [rewards].[transactions] AS t
    ON t.[TransactionID] = e.[Credited_TransactionID];
GO
