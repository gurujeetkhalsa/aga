/* Copyright 2026, American Go Association, All rights reserved */
/* Adds the configurable public occurrence limit and optional contact location. */

SET XACT_ABORT ON;
BEGIN TRANSACTION;

IF COL_LENGTH(N'events.events', N'public_occurrence_limit') IS NULL
BEGIN
    ALTER TABLE events.events ADD public_occurrence_limit smallint NOT NULL
        CONSTRAINT DF_events_public_occurrence_limit DEFAULT (4) WITH VALUES;
    EXEC(N'ALTER TABLE events.events ADD CONSTRAINT CK_events_public_occurrence_limit
        CHECK (public_occurrence_limit BETWEEN 1 AND 24);');
END;

IF COL_LENGTH(N'events.registration_contacts', N'city') IS NULL
    ALTER TABLE events.registration_contacts ADD city nvarchar(120) NULL;

IF COL_LENGTH(N'events.registration_contacts', N'state_province') IS NULL
    ALTER TABLE events.registration_contacts ADD state_province nvarchar(120) NULL;

IF COL_LENGTH(N'events.registration_contacts', N'country') IS NULL
    ALTER TABLE events.registration_contacts ADD country nvarchar(100) NULL;

IF COL_LENGTH(N'events.registration_contacts', N'postal_code') IS NULL
    ALTER TABLE events.registration_contacts ADD postal_code nvarchar(32) NULL;

EXEC(N'
    UPDATE events.events
    SET public_occurrence_limit = 4,
        selection_mode = ''single'',
        max_occurrence_selections = 1,
        updated_at_utc = SYSUTCDATETIME()
    WHERE slug = N''learn-go-in-an-hour'';
');

COMMIT TRANSACTION;
