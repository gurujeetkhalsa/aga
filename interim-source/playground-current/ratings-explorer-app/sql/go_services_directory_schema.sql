IF SCHEMA_ID(N'go_directory') IS NULL
    EXEC(N'CREATE SCHEMA [go_directory]');
GO

IF OBJECT_ID(N'go_directory.organizations', N'U') IS NULL
BEGIN
    CREATE TABLE [go_directory].[organizations]
    (
        [OrganizationID] int IDENTITY(1,1) NOT NULL
            CONSTRAINT [PK_go_directory_organizations] PRIMARY KEY,
        [PublicName] nvarchar(200) NOT NULL,
        [SortName] nvarchar(200) NULL,
        [Description] nvarchar(2000) NULL,
        [PrimaryCity] nvarchar(100) NULL,
        [PrimaryState] nvarchar(20) NULL,
        [PrimaryCountry] nvarchar(10) NOT NULL
            CONSTRAINT [DF_go_directory_organizations_PrimaryCountry] DEFAULT (N'US'),
        [RelationshipStatus] nvarchar(40) NOT NULL
            CONSTRAINT [DF_go_directory_organizations_RelationshipStatus] DEFAULT (N'independent'),
        [PublishStatus] nvarchar(40) NOT NULL
            CONSTRAINT [DF_go_directory_organizations_PublishStatus] DEFAULT (N'pending_review'),
        [AGAChapterID] int NULL,
        [AGAChapterCode] nvarchar(20) NULL,
        [AGAChapterName] nvarchar(200) NULL,
        [AGAChapterStatus] nvarchar(40) NULL,
        [AGAChapterVerifiedAt] datetime2(7) NULL,
        [PublicContactName] nvarchar(200) NULL,
        [PublicContactEmail] nvarchar(320) NULL,
        [PublicContactPhone] nvarchar(40) NULL,
        [PublicSearchText] nvarchar(max) NULL,
        [CreatedAt] datetime2(7) NOT NULL
            CONSTRAINT [DF_go_directory_organizations_CreatedAt] DEFAULT (SYSUTCDATETIME()),
        [UpdatedAt] datetime2(7) NOT NULL
            CONSTRAINT [DF_go_directory_organizations_UpdatedAt] DEFAULT (SYSUTCDATETIME()),
        [FirstPublishedAt] datetime2(7) NULL,
        [LastPublishedAt] datetime2(7) NULL,
        [LastSubmittedAt] datetime2(7) NULL,
        [LastReviewedAt] datetime2(7) NULL,
        [LastConfirmedAt] datetime2(7) NULL,
        [LastContentUpdatedAt] datetime2(7) NULL,
        [ClubExpressSourceUpdatedAt] datetime2(7) NULL,
        CONSTRAINT [CK_go_directory_organizations_RelationshipStatus]
            CHECK ([RelationshipStatus] IN (N'aga_chapter', N'chapter_applicant', N'independent', N'pending_match', N'archived')),
        CONSTRAINT [CK_go_directory_organizations_PublishStatus]
            CHECK ([PublishStatus] IN (N'pending_review', N'published', N'needs_information', N'rejected', N'archived'))
    );
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_go_directory_organizations_publish_state'
      AND [object_id] = OBJECT_ID(N'go_directory.organizations')
)
BEGIN
    CREATE INDEX [IX_go_directory_organizations_publish_state]
        ON [go_directory].[organizations] ([PublishStatus], [PrimaryState], [RelationshipStatus])
        INCLUDE ([PublicName], [PrimaryCity], [LastConfirmedAt], [LastContentUpdatedAt]);
END;
GO

IF OBJECT_ID(N'go_directory.organization_contacts', N'U') IS NULL
BEGIN
    CREATE TABLE [go_directory].[organization_contacts]
    (
        [ContactID] int IDENTITY(1,1) NOT NULL
            CONSTRAINT [PK_go_directory_organization_contacts] PRIMARY KEY,
        [OrganizationID] int NOT NULL,
        [ContactName] nvarchar(200) NULL,
        [Email] nvarchar(320) NULL,
        [Phone] nvarchar(40) NULL,
        [Role] nvarchar(100) NULL,
        [AGAID] int NULL,
        [IsPublic] bit NOT NULL
            CONSTRAINT [DF_go_directory_organization_contacts_IsPublic] DEFAULT (0),
        [IsPrimary] bit NOT NULL
            CONSTRAINT [DF_go_directory_organization_contacts_IsPrimary] DEFAULT (0),
        [CreatedAt] datetime2(7) NOT NULL
            CONSTRAINT [DF_go_directory_organization_contacts_CreatedAt] DEFAULT (SYSUTCDATETIME()),
        [UpdatedAt] datetime2(7) NOT NULL
            CONSTRAINT [DF_go_directory_organization_contacts_UpdatedAt] DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT [FK_go_directory_organization_contacts_organizations]
            FOREIGN KEY ([OrganizationID])
            REFERENCES [go_directory].[organizations] ([OrganizationID])
    );
END;
GO

IF OBJECT_ID(N'go_directory.organization_links', N'U') IS NULL
BEGIN
    CREATE TABLE [go_directory].[organization_links]
    (
        [LinkID] int IDENTITY(1,1) NOT NULL
            CONSTRAINT [PK_go_directory_organization_links] PRIMARY KEY,
        [OrganizationID] int NOT NULL,
        [LinkType] nvarchar(40) NOT NULL,
        [Label] nvarchar(100) NULL,
        [Url] nvarchar(1000) NOT NULL,
        [IsPrimary] bit NOT NULL
            CONSTRAINT [DF_go_directory_organization_links_IsPrimary] DEFAULT (0),
        [LastCheckedAt] datetime2(7) NULL,
        [CreatedAt] datetime2(7) NOT NULL
            CONSTRAINT [DF_go_directory_organization_links_CreatedAt] DEFAULT (SYSUTCDATETIME()),
        [UpdatedAt] datetime2(7) NOT NULL
            CONSTRAINT [DF_go_directory_organization_links_UpdatedAt] DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT [FK_go_directory_organization_links_organizations]
            FOREIGN KEY ([OrganizationID])
            REFERENCES [go_directory].[organizations] ([OrganizationID]),
        CONSTRAINT [CK_go_directory_organization_links_LinkType]
            CHECK ([LinkType] IN (N'website', N'meetup', N'discord', N'facebook', N'instagram', N'mailing_list', N'calendar', N'tournament', N'online_room', N'donation', N'other'))
    );
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_go_directory_organization_links_org'
      AND [object_id] = OBJECT_ID(N'go_directory.organization_links')
)
BEGIN
    CREATE INDEX [IX_go_directory_organization_links_org]
        ON [go_directory].[organization_links] ([OrganizationID], [LinkType]);
END;
GO

IF OBJECT_ID(N'go_directory.organization_services', N'U') IS NULL
BEGIN
    CREATE TABLE [go_directory].[organization_services]
    (
        [OrganizationID] int NOT NULL,
        [ServiceCode] nvarchar(40) NOT NULL,
        [Details] nvarchar(500) NULL,
        [CreatedAt] datetime2(7) NOT NULL
            CONSTRAINT [DF_go_directory_organization_services_CreatedAt] DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT [PK_go_directory_organization_services]
            PRIMARY KEY ([OrganizationID], [ServiceCode]),
        CONSTRAINT [FK_go_directory_organization_services_organizations]
            FOREIGN KEY ([OrganizationID])
            REFERENCES [go_directory].[organizations] ([OrganizationID]),
        CONSTRAINT [CK_go_directory_organization_services_ServiceCode]
            CHECK ([ServiceCode] IN (N'casual_play', N'beginner_lessons', N'youth_program', N'tournaments', N'aga_rated_games', N'teaching', N'school_program', N'library_program', N'online_play', N'equipment_library', N'professional_services', N'special_events'))
    );
END;
GO

IF OBJECT_ID(N'go_directory.locations', N'U') IS NULL
BEGIN
    CREATE TABLE [go_directory].[locations]
    (
        [LocationID] int IDENTITY(1,1) NOT NULL
            CONSTRAINT [PK_go_directory_locations] PRIMARY KEY,
        [OrganizationID] int NOT NULL,
        [VenueName] nvarchar(200) NULL,
        [AddressLine1] nvarchar(200) NULL,
        [AddressLine2] nvarchar(200) NULL,
        [City] nvarchar(100) NULL,
        [State] nvarchar(20) NULL,
        [PostalCode] nvarchar(20) NULL,
        [Country] nvarchar(10) NOT NULL
            CONSTRAINT [DF_go_directory_locations_Country] DEFAULT (N'US'),
        [Latitude] decimal(9,6) NULL,
        [Longitude] decimal(9,6) NULL,
        [VenueType] nvarchar(60) NULL,
        [AddressVisibility] nvarchar(30) NOT NULL
            CONSTRAINT [DF_go_directory_locations_AddressVisibility] DEFAULT (N'public'),
        [AccessibilityNotes] nvarchar(1000) NULL,
        [ParkingNotes] nvarchar(1000) NULL,
        [IsPublished] bit NOT NULL
            CONSTRAINT [DF_go_directory_locations_IsPublished] DEFAULT (1),
        [CreatedAt] datetime2(7) NOT NULL
            CONSTRAINT [DF_go_directory_locations_CreatedAt] DEFAULT (SYSUTCDATETIME()),
        [UpdatedAt] datetime2(7) NOT NULL
            CONSTRAINT [DF_go_directory_locations_UpdatedAt] DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT [FK_go_directory_locations_organizations]
            FOREIGN KEY ([OrganizationID])
            REFERENCES [go_directory].[organizations] ([OrganizationID]),
        CONSTRAINT [CK_go_directory_locations_AddressVisibility]
            CHECK ([AddressVisibility] IN (N'public', N'approximate', N'contact_only', N'hidden'))
    );
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_go_directory_locations_org_state_city'
      AND [object_id] = OBJECT_ID(N'go_directory.locations')
)
BEGIN
    CREATE INDEX [IX_go_directory_locations_org_state_city]
        ON [go_directory].[locations] ([OrganizationID], [State], [City]);
END;
GO

IF OBJECT_ID(N'go_directory.events', N'U') IS NULL
BEGIN
    CREATE TABLE [go_directory].[events]
    (
        [EventID] int IDENTITY(1,1) NOT NULL
            CONSTRAINT [PK_go_directory_events] PRIMARY KEY,
        [OrganizationID] int NOT NULL,
        [LocationID] int NULL,
        [EventName] nvarchar(200) NULL,
        [ServiceCategory] nvarchar(40) NULL,
        [Mode] nvarchar(20) NOT NULL
            CONSTRAINT [DF_go_directory_events_Mode] DEFAULT (N'physical'),
        [DayOfWeek] tinyint NULL,
        [StartTime] time(0) NULL,
        [EndTime] time(0) NULL,
        [TimeZone] nvarchar(80) NULL,
        [RecurrenceText] nvarchar(300) NULL,
        [OnlineUrl] nvarchar(1000) NULL,
        [Audience] nvarchar(200) NULL,
        [IsBeginnerFriendly] bit NOT NULL
            CONSTRAINT [DF_go_directory_events_IsBeginnerFriendly] DEFAULT (0),
        [IsYouthFriendly] bit NOT NULL
            CONSTRAINT [DF_go_directory_events_IsYouthFriendly] DEFAULT (0),
        [HasAGARatedGames] bit NOT NULL
            CONSTRAINT [DF_go_directory_events_HasAGARatedGames] DEFAULT (0),
        [HasTournaments] bit NOT NULL
            CONSTRAINT [DF_go_directory_events_HasTournaments] DEFAULT (0),
        [Notes] nvarchar(1000) NULL,
        [LastConfirmedAt] datetime2(7) NULL,
        [IsPublished] bit NOT NULL
            CONSTRAINT [DF_go_directory_events_IsPublished] DEFAULT (1),
        [CreatedAt] datetime2(7) NOT NULL
            CONSTRAINT [DF_go_directory_events_CreatedAt] DEFAULT (SYSUTCDATETIME()),
        [UpdatedAt] datetime2(7) NOT NULL
            CONSTRAINT [DF_go_directory_events_UpdatedAt] DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT [FK_go_directory_events_organizations]
            FOREIGN KEY ([OrganizationID])
            REFERENCES [go_directory].[organizations] ([OrganizationID]),
        CONSTRAINT [FK_go_directory_events_locations]
            FOREIGN KEY ([LocationID])
            REFERENCES [go_directory].[locations] ([LocationID]),
        CONSTRAINT [CK_go_directory_events_Mode]
            CHECK ([Mode] IN (N'physical', N'online', N'hybrid')),
        CONSTRAINT [CK_go_directory_events_DayOfWeek]
            CHECK ([DayOfWeek] IS NULL OR [DayOfWeek] BETWEEN 0 AND 6)
    );
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_go_directory_events_org_location'
      AND [object_id] = OBJECT_ID(N'go_directory.events')
)
BEGIN
    CREATE INDEX [IX_go_directory_events_org_location]
        ON [go_directory].[events] ([OrganizationID], [LocationID], [IsPublished])
        INCLUDE ([DayOfWeek], [StartTime], [Mode], [IsBeginnerFriendly], [HasAGARatedGames], [HasTournaments]);
END;
GO

IF OBJECT_ID(N'go_directory.submissions', N'U') IS NULL
BEGIN
    CREATE TABLE [go_directory].[submissions]
    (
        [SubmissionID] int IDENTITY(1,1) NOT NULL
            CONSTRAINT [PK_go_directory_submissions] PRIMARY KEY,
        [OrganizationID] int NULL,
        [SubmissionType] nvarchar(40) NOT NULL,
        [Status] nvarchar(40) NOT NULL
            CONSTRAINT [DF_go_directory_submissions_Status] DEFAULT (N'received'),
        [SubmitterName] nvarchar(200) NULL,
        [SubmitterEmail] nvarchar(320) NULL,
        [SubmitterPhone] nvarchar(40) NULL,
        [SubmitterAGAID] int NULL,
        [PayloadJson] nvarchar(max) NOT NULL,
        [SourceIPAddress] nvarchar(80) NULL,
        [UserAgent] nvarchar(500) NULL,
        [ReviewerPrincipalName] nvarchar(320) NULL,
        [ReviewNotes] nvarchar(2000) NULL,
        [SubmittedAt] datetime2(7) NOT NULL
            CONSTRAINT [DF_go_directory_submissions_SubmittedAt] DEFAULT (SYSUTCDATETIME()),
        [ReviewedAt] datetime2(7) NULL,
        [AppliedAt] datetime2(7) NULL,
        CONSTRAINT [FK_go_directory_submissions_organizations]
            FOREIGN KEY ([OrganizationID])
            REFERENCES [go_directory].[organizations] ([OrganizationID]),
        CONSTRAINT [CK_go_directory_submissions_SubmissionType]
            CHECK ([SubmissionType] IN (N'new_listing', N'update_listing', N'chapter_application', N'confirm_listing')),
        CONSTRAINT [CK_go_directory_submissions_Status]
            CHECK ([Status] IN (N'received', N'needs_review', N'needs_submitter_response', N'approved', N'applied', N'rejected', N'duplicate', N'spam'))
    );
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_go_directory_submissions_status_submitted'
      AND [object_id] = OBJECT_ID(N'go_directory.submissions')
)
BEGIN
    CREATE INDEX [IX_go_directory_submissions_status_submitted]
        ON [go_directory].[submissions] ([Status], [SubmittedAt])
        INCLUDE ([SubmissionType], [OrganizationID], [SubmitterEmail]);
END;
GO

IF OBJECT_ID(N'go_directory.audit_log', N'U') IS NULL
BEGIN
    CREATE TABLE [go_directory].[audit_log]
    (
        [AuditID] bigint IDENTITY(1,1) NOT NULL
            CONSTRAINT [PK_go_directory_audit_log] PRIMARY KEY,
        [OrganizationID] int NULL,
        [SubmissionID] int NULL,
        [ActorPrincipalName] nvarchar(320) NULL,
        [EventType] nvarchar(80) NOT NULL,
        [EventAt] datetime2(7) NOT NULL
            CONSTRAINT [DF_go_directory_audit_log_EventAt] DEFAULT (SYSUTCDATETIME()),
        [DetailsJson] nvarchar(max) NULL,
        CONSTRAINT [FK_go_directory_audit_log_organizations]
            FOREIGN KEY ([OrganizationID])
            REFERENCES [go_directory].[organizations] ([OrganizationID]),
        CONSTRAINT [FK_go_directory_audit_log_submissions]
            FOREIGN KEY ([SubmissionID])
            REFERENCES [go_directory].[submissions] ([SubmissionID])
    );
END;
GO

IF OBJECT_ID(N'go_directory.v_public_directory', N'V') IS NOT NULL
    DROP VIEW [go_directory].[v_public_directory];
GO

CREATE VIEW [go_directory].[v_public_directory]
AS
SELECT
    org.[OrganizationID],
    org.[PublicName],
    org.[Description],
    org.[PrimaryCity],
    org.[PrimaryState],
    org.[RelationshipStatus],
    CAST(CASE WHEN org.[RelationshipStatus] = N'aga_chapter' THEN 1 ELSE 0 END AS bit) AS [IsAGAChapter],
    org.[AGAChapterID],
    org.[AGAChapterCode],
    org.[AGAChapterName],
    org.[AGAChapterStatus],
    org.[PublicContactName],
    org.[PublicContactEmail],
    org.[PublicContactPhone],
    org.[LastConfirmedAt],
    org.[LastContentUpdatedAt],
    org.[LastPublishedAt],
    loc.[LocationID],
    loc.[VenueName],
    loc.[City],
    loc.[State],
    loc.[PostalCode],
    loc.[Latitude],
    loc.[Longitude],
    loc.[AddressVisibility],
    event.[EventID],
    event.[EventName],
    event.[Mode],
    event.[DayOfWeek],
    event.[StartTime],
    event.[EndTime],
    event.[TimeZone],
    event.[RecurrenceText],
    event.[IsBeginnerFriendly],
    event.[IsYouthFriendly],
    event.[HasAGARatedGames],
    event.[HasTournaments]
FROM [go_directory].[organizations] AS org
LEFT JOIN [go_directory].[locations] AS loc
    ON loc.[OrganizationID] = org.[OrganizationID]
   AND loc.[IsPublished] = 1
LEFT JOIN [go_directory].[events] AS event
    ON event.[OrganizationID] = org.[OrganizationID]
   AND
   (
        event.[LocationID] = loc.[LocationID]
        OR (event.[LocationID] IS NULL AND loc.[LocationID] IS NULL)
   )
   AND event.[IsPublished] = 1
WHERE org.[PublishStatus] = N'published';
GO
