SET XACT_ABORT ON;
BEGIN TRANSACTION;
IF OBJECT_ID('events.rehearsal_clocks','U') IS NULL
    CREATE TABLE events.rehearsal_clocks(
        occurrence_id bigint NOT NULL PRIMARY KEY REFERENCES events.event_occurrences(occurrence_id),
        stage int NOT NULL CHECK(stage BETWEEN 0 AND 4),
        simulated_at_utc datetime2(0) NOT NULL,
        updated_at_utc datetime2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
IF COL_LENGTH('events.email_outbox','retry_after_utc') IS NULL
    ALTER TABLE events.email_outbox ADD retry_after_utc datetime2(0) NULL;
COMMIT TRANSACTION;
