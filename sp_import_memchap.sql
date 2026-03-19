/****** Object:  StoredProcedure [membership].[sp_import_memchap]    Script Date: 3/12/2026 10:37:55 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [membership].[sp_import_memchap]
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @Now DATETIME2(7) = SYSDATETIME();
    DECLARE @Today DATE = CAST(@Now AS DATE);

    DECLARE @MergeResults TABLE
    (
        ActionType NVARCHAR(10) NOT NULL,
        AGAID INT NOT NULL,
        OldExpiration DATE NULL,
        NewExpiration DATE NULL,
        OldChapterID INT NULL,
        NewChapterID INT NULL
    );

    BEGIN TRY
        IF EXISTS (SELECT 1 FROM staging.memchap WHERE AGAID IS NULL)
        BEGIN
            THROW 50001, 'staging.memchap contains rows with NULL AGAID.', 1;
        END;

        IF EXISTS
        (
            SELECT 1
            FROM staging.memchap
            GROUP BY AGAID
            HAVING COUNT(*) > 1
        )
        BEGIN
            THROW 50002, 'staging.memchap contains duplicate AGAID values.', 1;
        END;

        BEGIN TRANSACTION;

        MERGE membership.members AS target
        USING staging.memchap AS source
        ON target.AGAID = source.AGAID

        WHEN MATCHED AND
        (
            ISNULL(target.MemberType, N'') <> ISNULL(source.MemberType, N'')
            OR ISNULL(target.FirstName, N'') <> ISNULL(source.FirstName, N'')
            OR ISNULL(target.MiddleInitial, N'') <> ISNULL(source.MiddleInitial, N'')
            OR ISNULL(target.LastName, N'') <> ISNULL(source.LastName, N'')
            OR ISNULL(target.Nickname, N'') <> ISNULL(source.Nickname, N'')
            OR ISNULL(target.Pronouns, N'') <> ISNULL(source.Pronouns, N'')
            OR ISNULL(target.LoginName, N'') <> ISNULL(source.LoginName, N'')
            OR ISNULL(target.Status, N'') <> ISNULL(source.Status, N'')
            OR ISNULL(target.EmailAddress, N'') <> ISNULL(source.EmailAddress, N'')
            OR ISNULL(target.CellPhone, N'') <> ISNULL(source.CellPhone, N'')
            OR ISNULL(target.PhoneNumber, N'') <> ISNULL(source.PhoneNumber, N'')
            OR ISNULL(target.Address1, N'') <> ISNULL(source.Address1, N'')
            OR ISNULL(target.Address2, N'') <> ISNULL(source.Address2, N'')
            OR ISNULL(target.City, N'') <> ISNULL(source.City, N'')
            OR ISNULL(target.State, N'') <> ISNULL(source.State, N'')
            OR ISNULL(target.ZipCode, N'') <> ISNULL(source.ZipCode, N'')
            OR ISNULL(target.Country, N'') <> ISNULL(source.Country, N'')
            OR ISNULL(target.DateOfBirth, '19000101') <> ISNULL(source.DateOfBirth, '19000101')
            OR ISNULL(target.WorkTitle, N'') <> ISNULL(source.WorkTitle, N'')
            OR ISNULL(target.Gender, N'') <> ISNULL(source.Gender, N'')
            OR ISNULL(target.JoinDate, '19000101') <> ISNULL(source.JoinDate, '19000101')
            OR ISNULL(target.ExpirationDate, '19000101') <> ISNULL(source.ExpirationDate, '19000101')
            OR ISNULL(target.LastRenewalDate, '19000101') <> ISNULL(source.LastRenewalDate, '19000101')
            OR ISNULL(target.ChapterID, -1) <> ISNULL(source.ChapterID, -1)
            OR ISNULL(target.EmergencyContactName, N'') <> ISNULL(source.EmergencyContactName, N'')
            OR ISNULL(target.EmergencyContactRelationship, N'') <> ISNULL(source.EmergencyContactRelationship, N'')
            OR ISNULL(target.EmergencyContactPhone, N'') <> ISNULL(source.EmergencyContactPhone, N'')
            OR ISNULL(target.EmergencyContactEmail, N'') <> ISNULL(source.EmergencyContactEmail, N'')
        )
        THEN
        UPDATE SET
            target.MemberType = source.MemberType,
            target.FirstName = source.FirstName,
            target.MiddleInitial = source.MiddleInitial,
            target.LastName = source.LastName,
            target.Nickname = source.Nickname,
            target.Pronouns = source.Pronouns,
            target.LoginName = source.LoginName,
            target.Status = source.Status,
            target.EmailAddress = source.EmailAddress,
            target.CellPhone = source.CellPhone,
            target.PhoneNumber = source.PhoneNumber,
            target.Address1 = source.Address1,
            target.Address2 = source.Address2,
            target.City = source.City,
            target.State = source.State,
            target.ZipCode = source.ZipCode,
            target.Country = source.Country,
            target.DateOfBirth = source.DateOfBirth,
            target.WorkTitle = source.WorkTitle,
            target.Gender = source.Gender,
            target.JoinDate = source.JoinDate,
            target.ExpirationDate = source.ExpirationDate,
            target.LastRenewalDate = source.LastRenewalDate,
            target.ChapterID = source.ChapterID,
            target.EmergencyContactName = source.EmergencyContactName,
            target.EmergencyContactRelationship = source.EmergencyContactRelationship,
            target.EmergencyContactPhone = source.EmergencyContactPhone,
            target.EmergencyContactEmail = source.EmergencyContactEmail,
            target.LastUpdated = @Now

        WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            AGAID, MemberType,
            FirstName, MiddleInitial, LastName, Nickname, Pronouns,
            LoginName, Status,
            EmailAddress, CellPhone, PhoneNumber,
            Address1, Address2, City, State, ZipCode, Country,
            DateOfBirth, WorkTitle, Gender,
            JoinDate, ExpirationDate, LastRenewalDate,
            ChapterID,
            EmergencyContactName, EmergencyContactRelationship,
            EmergencyContactPhone, EmergencyContactEmail,
            CreatedDate, LastUpdated
        )
        VALUES (
            source.AGAID, source.MemberType,
            source.FirstName, source.MiddleInitial, source.LastName, source.Nickname, source.Pronouns,
            source.LoginName, source.Status,
            source.EmailAddress, source.CellPhone, source.PhoneNumber,
            source.Address1, source.Address2, source.City, source.State, source.ZipCode, source.Country,
            source.DateOfBirth, source.WorkTitle, source.Gender,
            source.JoinDate, source.ExpirationDate, source.LastRenewalDate,
            source.ChapterID,
            source.EmergencyContactName, source.EmergencyContactRelationship,
            source.EmergencyContactPhone, source.EmergencyContactEmail,
            @Now, @Now
        )

        OUTPUT
            $action,
            inserted.AGAID,
            deleted.ExpirationDate,
            inserted.ExpirationDate,
            deleted.ChapterID,
            inserted.ChapterID
        INTO @MergeResults;

        INSERT INTO membership.membership_events
        (
            AGAID,
            EventType,
            EventDate,
            Source,
            OldExpiration,
            NewExpiration
        )
        SELECT
            AGAID,
            CASE
                WHEN ActionType = 'INSERT' THEN 'signup'
                WHEN ISNULL(OldExpiration, '19000101') <> ISNULL(NewExpiration, '19000101') THEN 'renewal'
                WHEN ISNULL(OldChapterID, -1) <> ISNULL(NewChapterID, -1) THEN 'chapter_change'
                ELSE 'update'
            END,
            @Now,
            'memchap_import',
            OldExpiration,
            NewExpiration
        FROM @MergeResults;

        UPDATE history
        SET EndDate = @Today
        FROM membership.chapter_membership_history AS history
        WHERE history.EndDate IS NULL
          AND EXISTS
        (
            SELECT 1
            FROM @MergeResults AS result
            WHERE result.AGAID = history.AGAID
              AND ISNULL(result.OldChapterID, -1) <> ISNULL(result.NewChapterID, -1)
        );

        INSERT INTO membership.chapter_membership_history
        (
            AGAID,
            ChapterID,
            StartDate,
            Source
        )
        SELECT
            AGAID,
            NewChapterID,
            @Today,
            'memchap_import'
        FROM @MergeResults
        WHERE NewChapterID IS NOT NULL
          AND
          (
              ActionType = 'INSERT'
              OR ISNULL(OldChapterID, -1) <> ISNULL(NewChapterID, -1)
          );

        TRUNCATE TABLE staging.memchap;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        THROW;
    END CATCH;

END;
GO

