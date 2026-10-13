/* Optional OGS handle recorded by authorized workshop organizers. */
SET XACT_ABORT ON;
BEGIN TRANSACTION;
IF COL_LENGTH('events.registration_contacts','ogs_handle') IS NULL
    ALTER TABLE events.registration_contacts ADD ogs_handle nvarchar(100) NULL;
COMMIT TRANSACTION;
