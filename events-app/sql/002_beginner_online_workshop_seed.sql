/* Copyright 2026, American Go Association, All rights reserved */
/* First production event series and occurrence. Safe to rerun. */

SET XACT_ABORT ON;
BEGIN TRANSACTION;

DECLARE @OrganizerPrincipalId nvarchar(128) = N'484247c8-2585-4306-83dd-74197d98bdc1';
DECLARE @OrganizerId bigint;
DECLARE @EventId bigint;
DECLARE @VersionId bigint;

SELECT @OrganizerId = organizer_id
FROM events.organizers
WHERE organizer_key = N'national-aga';

IF @OrganizerId IS NULL
BEGIN
    INSERT INTO events.organizers (organizer_key, name, organizer_type)
    VALUES (N'national-aga', N'American Go Association', 'national_aga');
    SET @OrganizerId = SCOPE_IDENTITY();
END;

IF NOT EXISTS (
    SELECT 1
    FROM events.organizer_memberships
    WHERE organizer_id = @OrganizerId
      AND principal_object_id = @OrganizerPrincipalId
)
BEGIN
    INSERT INTO events.organizer_memberships
        (organizer_id, principal_object_id, can_manage_events,
         can_manage_registrations, can_view_finance)
    VALUES
        (@OrganizerId, @OrganizerPrincipalId, 1, 1, 0);
END;

SELECT @EventId = event_id
FROM events.events
WHERE slug = N'learn-go-in-an-hour';

IF @EventId IS NULL
BEGIN
    INSERT INTO events.events
        (organizer_id, slug, name, description, time_zone, lifecycle,
         selection_mode, max_occurrence_selections, public_occurrence_limit)
    VALUES
        (@OrganizerId,
         N'learn-go-in-an-hour',
         N'Free Beginners Online Workshop - Learn Go in an Hour',
         N'Do you have a friend, family member, or coworker who’s always wanted to learn Go but didn’t know where to start? This is a great opportunity to introduce them. The American Go Association is excited to offer an online 1-hour beginner Go workshop led by Milan Mladenovic, American Go Foundation Teacher of the Year. The workshop is designed specifically for complete beginners. During the Zoom session, students will learn the basics of the game and be guided into playing their very first Go game. Class size is limited to 10 participants, ensuring a welcoming, small-group experience.',
         N'America/New_York',
         'registration_open',
         'single',
         1,
         4);
    SET @EventId = SCOPE_IDENTITY();
END
ELSE
BEGIN
    UPDATE events.events
    SET organizer_id = @OrganizerId,
        name = N'Free Beginners Online Workshop - Learn Go in an Hour',
        description = N'Do you have a friend, family member, or coworker who’s always wanted to learn Go but didn’t know where to start? This is a great opportunity to introduce them. The American Go Association is excited to offer an online 1-hour beginner Go workshop led by Milan Mladenovic, American Go Foundation Teacher of the Year. The workshop is designed specifically for complete beginners. During the Zoom session, students will learn the basics of the game and be guided into playing their very first Go game. Class size is limited to 10 participants, ensuring a welcoming, small-group experience.',
        time_zone = N'America/New_York',
        lifecycle = 'registration_open',
        selection_mode = 'single',
        max_occurrence_selections = 1,
        public_occurrence_limit = 4,
        updated_at_utc = SYSUTCDATETIME()
    WHERE event_id = @EventId;
END;

SELECT @VersionId = current_event_version_id
FROM events.events
WHERE event_id = @EventId;

IF @VersionId IS NULL
BEGIN
    INSERT INTO events.event_versions
        (event_id, version_number, form_definition_json,
         policy_snapshot_json, published_at_utc)
    VALUES
        (@EventId,
         1,
         N'{"pages":[{"id":"contact","required":["first_name","last_name","email"]}]}',
         N'{"price":"free","selection_mode":"single","max_occurrence_selections":1,"waitlist":true}',
         SYSUTCDATETIME());
    SET @VersionId = SCOPE_IDENTITY();

    UPDATE events.events
    SET current_event_version_id = @VersionId
    WHERE event_id = @EventId;
END;

IF NOT EXISTS (
    SELECT 1
    FROM events.event_occurrences
    WHERE event_id = @EventId
      AND occurrence_key = N'2026-09-10-1800-et'
)
BEGIN
    INSERT INTO events.event_occurrences
        (event_id, event_version_id, occurrence_key, title,
         starts_at_utc, ends_at_utc,
         registration_opens_at_utc, registration_closes_at_utc,
         capacity, waitlist_enabled, location_name, lifecycle)
    VALUES
        (@EventId,
         @VersionId,
         N'2026-09-10-1800-et',
         N'Thursday, September 10, 2026 Workshop',
         '2026-09-10T22:00:00', -- 6:00 PM America/New_York (EDT)
         '2026-09-10T23:00:00', -- 7:00 PM America/New_York (EDT)
         SYSUTCDATETIME(),
         '2026-09-10T22:00:00',
         10,
         1,
         N'Online via Zoom',
         'published');
END;

COMMIT TRANSACTION;

SELECT e.event_id, e.slug, e.name,
       N'/events/' + e.slug AS stable_public_path,
       o.occurrence_id, o.starts_at_utc, o.ends_at_utc,
       o.capacity, o.waitlist_enabled
FROM events.events e
JOIN events.event_occurrences o ON o.event_id = e.event_id
WHERE e.slug = N'learn-go-in-an-hour'
ORDER BY o.starts_at_utc;
