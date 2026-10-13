/* Copyright 2026, American Go Association, All rights reserved */
/*
  First AGA Events vertical slice.

  events.events is the stable public event/series. A one-time event has one
  occurrence; a repeating event has many. Registrations can select one or more
  occurrences according to the event selection policy.
*/

SET XACT_ABORT ON;
BEGIN TRANSACTION;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'events')
    EXEC(N'CREATE SCHEMA events AUTHORIZATION dbo;');

IF OBJECT_ID(N'events.organizers', N'U') IS NULL
BEGIN
    CREATE TABLE events.organizers (
        organizer_id bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_events_organizers PRIMARY KEY,
        organizer_key nvarchar(80) NOT NULL,
        name nvarchar(200) NOT NULL,
        organizer_type varchar(30) NOT NULL CONSTRAINT DF_events_organizers_type DEFAULT ('other'),
        is_active bit NOT NULL CONSTRAINT DF_events_organizers_active DEFAULT (1),
        created_at_utc datetime2(0) NOT NULL CONSTRAINT DF_events_organizers_created DEFAULT (SYSUTCDATETIME()),
        updated_at_utc datetime2(0) NOT NULL CONSTRAINT DF_events_organizers_updated DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT UQ_events_organizers_key UNIQUE (organizer_key),
        CONSTRAINT CK_events_organizers_type CHECK (organizer_type IN ('national_aga','chapter','other'))
    );
END;

IF OBJECT_ID(N'events.organizer_memberships', N'U') IS NULL
BEGIN
    CREATE TABLE events.organizer_memberships (
        organizer_membership_id bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_events_organizer_memberships PRIMARY KEY,
        organizer_id bigint NOT NULL,
        principal_object_id nvarchar(128) NOT NULL,
        can_manage_events bit NOT NULL CONSTRAINT DF_events_memberships_manage DEFAULT (0),
        can_manage_registrations bit NOT NULL CONSTRAINT DF_events_memberships_registrations DEFAULT (0),
        can_view_finance bit NOT NULL CONSTRAINT DF_events_memberships_finance DEFAULT (0),
        is_active bit NOT NULL CONSTRAINT DF_events_memberships_active DEFAULT (1),
        created_at_utc datetime2(0) NOT NULL CONSTRAINT DF_events_memberships_created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT FK_events_memberships_organizer FOREIGN KEY (organizer_id) REFERENCES events.organizers(organizer_id),
        CONSTRAINT UQ_events_memberships_scope UNIQUE (organizer_id, principal_object_id)
    );
END;

IF OBJECT_ID(N'events.events', N'U') IS NULL
BEGIN
    CREATE TABLE events.events (
        event_id bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_events_events PRIMARY KEY,
        organizer_id bigint NOT NULL,
        slug nvarchar(120) NOT NULL,
        name nvarchar(200) NOT NULL,
        description nvarchar(2000) NULL,
        time_zone nvarchar(64) NOT NULL CONSTRAINT DF_events_time_zone DEFAULT (N'America/New_York'),
        lifecycle varchar(30) NOT NULL CONSTRAINT DF_events_lifecycle DEFAULT ('draft'),
        selection_mode varchar(20) NOT NULL CONSTRAINT DF_events_selection_mode DEFAULT ('single'),
        max_occurrence_selections smallint NULL,
        public_occurrence_limit smallint NOT NULL CONSTRAINT DF_events_public_occurrence_limit DEFAULT (4),
        current_event_version_id bigint NULL,
        created_at_utc datetime2(0) NOT NULL CONSTRAINT DF_events_created DEFAULT (SYSUTCDATETIME()),
        updated_at_utc datetime2(0) NOT NULL CONSTRAINT DF_events_updated DEFAULT (SYSUTCDATETIME()),
        row_version rowversion NOT NULL,
        CONSTRAINT FK_events_events_organizer FOREIGN KEY (organizer_id) REFERENCES events.organizers(organizer_id),
        CONSTRAINT UQ_events_events_slug UNIQUE (slug),
        CONSTRAINT CK_events_events_lifecycle CHECK (lifecycle IN ('draft','published','registration_open','registration_closed','cancelled','completed','archived')),
        CONSTRAINT CK_events_events_selection_mode CHECK (selection_mode IN ('single','multiple')),
        CONSTRAINT CK_events_events_selection_max CHECK (max_occurrence_selections IS NULL OR max_occurrence_selections >= 1),
        CONSTRAINT CK_events_events_single_max CHECK (selection_mode <> 'single' OR max_occurrence_selections IS NULL OR max_occurrence_selections = 1),
        CONSTRAINT CK_events_public_occurrence_limit CHECK (public_occurrence_limit BETWEEN 1 AND 24)
    );
END;

IF OBJECT_ID(N'events.event_versions', N'U') IS NULL
BEGIN
    CREATE TABLE events.event_versions (
        event_version_id bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_events_event_versions PRIMARY KEY,
        event_id bigint NOT NULL,
        version_number int NOT NULL,
        form_definition_json nvarchar(max) NOT NULL CONSTRAINT DF_events_versions_form DEFAULT (N'{"pages":[]}'),
        policy_snapshot_json nvarchar(max) NOT NULL CONSTRAINT DF_events_versions_policy DEFAULT (N'{}'),
        published_at_utc datetime2(0) NULL,
        created_at_utc datetime2(0) NOT NULL CONSTRAINT DF_events_versions_created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT FK_events_versions_event FOREIGN KEY (event_id) REFERENCES events.events(event_id),
        CONSTRAINT UQ_events_versions_number UNIQUE (event_id, version_number),
        CONSTRAINT CK_events_versions_form_json CHECK (ISJSON(form_definition_json) = 1),
        CONSTRAINT CK_events_versions_policy_json CHECK (ISJSON(policy_snapshot_json) = 1)
    );
END;

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_events_events_current_version')
BEGIN
    ALTER TABLE events.events ADD CONSTRAINT FK_events_events_current_version
        FOREIGN KEY (current_event_version_id) REFERENCES events.event_versions(event_version_id);
END;

IF OBJECT_ID(N'events.event_occurrences', N'U') IS NULL
BEGIN
    CREATE TABLE events.event_occurrences (
        occurrence_id bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_events_occurrences PRIMARY KEY,
        event_id bigint NOT NULL,
        event_version_id bigint NOT NULL,
        occurrence_key nvarchar(64) NOT NULL,
        title nvarchar(200) NOT NULL,
        starts_at_utc datetime2(0) NOT NULL,
        ends_at_utc datetime2(0) NOT NULL,
        registration_opens_at_utc datetime2(0) NULL,
        registration_closes_at_utc datetime2(0) NULL,
        capacity int NULL,
        waitlist_enabled bit NOT NULL CONSTRAINT DF_events_occurrences_waitlist DEFAULT (1),
        location_name nvarchar(200) NULL,
        lifecycle varchar(20) NOT NULL CONSTRAINT DF_events_occurrences_lifecycle DEFAULT ('draft'),
        created_at_utc datetime2(0) NOT NULL CONSTRAINT DF_events_occurrences_created DEFAULT (SYSUTCDATETIME()),
        updated_at_utc datetime2(0) NOT NULL CONSTRAINT DF_events_occurrences_updated DEFAULT (SYSUTCDATETIME()),
        row_version rowversion NOT NULL,
        CONSTRAINT FK_events_occurrences_event FOREIGN KEY (event_id) REFERENCES events.events(event_id),
        CONSTRAINT FK_events_occurrences_version FOREIGN KEY (event_version_id) REFERENCES events.event_versions(event_version_id),
        CONSTRAINT UQ_events_occurrences_key UNIQUE (event_id, occurrence_key),
        CONSTRAINT CK_events_occurrences_dates CHECK (ends_at_utc > starts_at_utc),
        CONSTRAINT CK_events_occurrences_registration_dates CHECK (registration_closes_at_utc IS NULL OR registration_opens_at_utc IS NULL OR registration_closes_at_utc > registration_opens_at_utc),
        CONSTRAINT CK_events_occurrences_capacity CHECK (capacity IS NULL OR capacity >= 1),
        CONSTRAINT CK_events_occurrences_lifecycle CHECK (lifecycle IN ('draft','published','cancelled','completed','archived'))
    );
    CREATE INDEX IX_events_occurrences_public ON events.event_occurrences(event_id, lifecycle, starts_at_utc)
        INCLUDE (ends_at_utc, capacity, waitlist_enabled, registration_opens_at_utc, registration_closes_at_utc);
END;

IF OBJECT_ID(N'events.registrations', N'U') IS NULL
BEGIN
    CREATE TABLE events.registrations (
        registration_id uniqueidentifier NOT NULL CONSTRAINT PK_events_registrations PRIMARY KEY,
        event_id bigint NOT NULL,
        event_version_id bigint NOT NULL,
        status varchar(30) NOT NULL,
        payment_status varchar(30) NOT NULL CONSTRAINT DF_events_registrations_payment DEFAULT ('not_required'),
        idempotency_key nvarchar(100) NOT NULL,
        submitted_at_utc datetime2(0) NULL,
        created_at_utc datetime2(0) NOT NULL CONSTRAINT DF_events_registrations_created DEFAULT (SYSUTCDATETIME()),
        updated_at_utc datetime2(0) NOT NULL CONSTRAINT DF_events_registrations_updated DEFAULT (SYSUTCDATETIME()),
        row_version rowversion NOT NULL,
        CONSTRAINT FK_events_registrations_event FOREIGN KEY (event_id) REFERENCES events.events(event_id),
        CONSTRAINT FK_events_registrations_version FOREIGN KEY (event_version_id) REFERENCES events.event_versions(event_version_id),
        CONSTRAINT UQ_events_registrations_idempotency UNIQUE (event_id, idempotency_key),
        CONSTRAINT CK_events_registrations_status CHECK (status IN ('in_progress','submitted','confirmed','waitlisted','mixed','cancelled','withdrawn','expired')),
        CONSTRAINT CK_events_registrations_payment CHECK (payment_status IN ('not_required','pending','authorized','succeeded','partially_refunded','refunded','failed','disputed'))
    );
    CREATE INDEX IX_events_registrations_event_status ON events.registrations(event_id, status, submitted_at_utc);
END;

IF OBJECT_ID(N'events.registration_contacts', N'U') IS NULL
BEGIN
    CREATE TABLE events.registration_contacts (
        registration_id uniqueidentifier NOT NULL CONSTRAINT PK_events_registration_contacts PRIMARY KEY,
        first_name nvarchar(100) NOT NULL,
        last_name nvarchar(100) NOT NULL,
        email nvarchar(254) NOT NULL,
        city nvarchar(120) NULL,
        state_province nvarchar(120) NULL,
        country nvarchar(100) NULL,
        postal_code nvarchar(32) NULL,
        CONSTRAINT FK_events_registration_contacts_registration FOREIGN KEY (registration_id) REFERENCES events.registrations(registration_id)
    );
    CREATE INDEX IX_events_registration_contacts_email ON events.registration_contacts(email);
END;

IF OBJECT_ID(N'events.registration_occurrences', N'U') IS NULL
BEGIN
    CREATE TABLE events.registration_occurrences (
        registration_occurrence_id bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_events_registration_occurrences PRIMARY KEY,
        registration_id uniqueidentifier NOT NULL,
        occurrence_id bigint NOT NULL,
        status varchar(20) NOT NULL,
        assigned_at_utc datetime2(0) NOT NULL,
        cancelled_at_utc datetime2(0) NULL,
        CONSTRAINT FK_events_registration_occurrences_registration FOREIGN KEY (registration_id) REFERENCES events.registrations(registration_id),
        CONSTRAINT FK_events_registration_occurrences_occurrence FOREIGN KEY (occurrence_id) REFERENCES events.event_occurrences(occurrence_id),
        CONSTRAINT UQ_events_registration_occurrences_choice UNIQUE (registration_id, occurrence_id),
        CONSTRAINT CK_events_registration_occurrences_status CHECK (status IN ('confirmed','waitlisted','cancelled'))
    );
    CREATE INDEX IX_events_registration_occurrences_capacity ON events.registration_occurrences(occurrence_id, status, assigned_at_utc)
        INCLUDE (registration_id);
END;

IF OBJECT_ID(N'events.audit_log', N'U') IS NULL
BEGIN
    CREATE TABLE events.audit_log (
        audit_id bigint IDENTITY(1,1) NOT NULL CONSTRAINT PK_events_audit_log PRIMARY KEY,
        event_id bigint NULL,
        registration_id uniqueidentifier NULL,
        action nvarchar(100) NOT NULL,
        actor_type varchar(30) NOT NULL,
        actor_principal_id nvarchar(128) NULL,
        detail_json nvarchar(max) NOT NULL CONSTRAINT DF_events_audit_detail DEFAULT (N'{}'),
        created_at_utc datetime2(0) NOT NULL CONSTRAINT DF_events_audit_created DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT FK_events_audit_event FOREIGN KEY (event_id) REFERENCES events.events(event_id),
        CONSTRAINT FK_events_audit_registration FOREIGN KEY (registration_id) REFERENCES events.registrations(registration_id),
        CONSTRAINT CK_events_audit_detail_json CHECK (ISJSON(detail_json) = 1)
    );
    CREATE INDEX IX_events_audit_event_time ON events.audit_log(event_id, created_at_utc DESC);
END;

COMMIT TRANSACTION;
