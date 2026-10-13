/* Rehearsal series never enter the live email queue. */
SET XACT_ABORT ON;
BEGIN TRANSACTION;
IF COL_LENGTH('events.workshop_settings', 'is_test') IS NULL
    ALTER TABLE events.workshop_settings ADD is_test bit NOT NULL
        CONSTRAINT DF_workshop_is_test DEFAULT (0);
COMMIT TRANSACTION;
