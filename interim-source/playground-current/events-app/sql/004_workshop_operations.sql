/* Copyright 2026, American Go Association. Apply after 001–003. */
SET XACT_ABORT ON;
BEGIN TRANSACTION;

IF COL_LENGTH('events.registration_occurrences', 'attendance') IS NULL
BEGIN
    ALTER TABLE events.registration_occurrences ADD
        attendance varchar(20) NOT NULL CONSTRAINT DF_events_attendance DEFAULT ('unrecorded'),
        attendance_recorded_at_utc datetime2(0) NULL,
        attendance_recorded_by nvarchar(128) NULL;
    EXEC(N'ALTER TABLE events.registration_occurrences ADD CONSTRAINT CK_events_attendance
        CHECK (attendance IN (''unrecorded'',''attended'',''absent''));');
END;

IF OBJECT_ID('events.workshop_settings', 'U') IS NULL
BEGIN
    CREATE TABLE events.workshop_settings (
        event_id bigint NOT NULL PRIMARY KEY REFERENCES events.events(event_id),
        sender_email nvarchar(254) NOT NULL DEFAULT ('milan.mladenovic@usgo.org'),
        instructor_name nvarchar(200) NOT NULL DEFAULT ('Milan'),
        reminder_hours nvarchar(100) NOT NULL DEFAULT ('[24,1]'),
        followup_hours int NOT NULL DEFAULT (24),
        followup_text nvarchar(4000) NOT NULL DEFAULT ('Thank you for your interest in Go! Explore resources and find your local chapter at https://www.usgo.org/.'),
        email_enabled bit NOT NULL DEFAULT (0),
        zoom_host_email nvarchar(254) NULL,
        CONSTRAINT CK_workshop_reminders CHECK (ISJSON(reminder_hours) = 1),
        CONSTRAINT CK_workshop_followup CHECK (followup_hours BETWEEN 0 AND 720)
    );
END;

IF OBJECT_ID('events.workshop_meetings', 'U') IS NULL
BEGIN
    CREATE TABLE events.workshop_meetings (
        occurrence_id bigint NOT NULL PRIMARY KEY REFERENCES events.event_occurrences(occurrence_id),
        meeting_id nvarchar(100) NULL,
        join_url nvarchar(2000) NULL,
        source varchar(20) NOT NULL DEFAULT ('zoom'),
        sync_status varchar(20) NOT NULL DEFAULT ('pending'),
        sync_error nvarchar(500) NULL,
        updated_at_utc datetime2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT CK_workshop_meeting_source CHECK (source IN ('zoom','manual')),
        CONSTRAINT CK_workshop_meeting_sync CHECK (sync_status IN ('pending','working','ready','uncertain','failed','cancelled'))
    );
    CREATE UNIQUE INDEX UX_workshop_zoom_meeting ON events.workshop_meetings(meeting_id)
        WHERE meeting_id IS NOT NULL;
END;

IF OBJECT_ID('events.email_outbox', 'U') IS NULL
BEGIN
    CREATE TABLE events.email_outbox (
        email_id bigint IDENTITY PRIMARY KEY,
        event_id bigint NOT NULL REFERENCES events.events(event_id),
        registration_id uniqueidentifier NOT NULL REFERENCES events.registrations(registration_id),
        occurrence_id bigint NOT NULL REFERENCES events.event_occurrences(occurrence_id),
        kind varchar(40) NOT NULL,
        dedupe_key nvarchar(200) NOT NULL UNIQUE,
        status varchar(20) NOT NULL DEFAULT ('pending'),
        due_at_utc datetime2(0) NOT NULL,
        attempts int NOT NULL DEFAULT (0),
        claimed_at_utc datetime2(0) NULL,
        provider_message_id nvarchar(200) NULL,
        last_error nvarchar(500) NULL,
        created_at_utc datetime2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
        sent_at_utc datetime2(0) NULL,
        CONSTRAINT CK_events_email_status CHECK (status IN ('pending','sending','sent','failed','uncertain','suppressed'))
    );
    CREATE INDEX IX_events_email_due ON events.email_outbox(status, due_at_utc);
END;
COMMIT TRANSACTION;
