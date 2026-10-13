-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [membership].[members] (
    [AGAID] int NOT NULL,
    [FirstName] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [MiddleInitial] nvarchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [LastName] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Nickname] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Pronouns] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [LoginName] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Status] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [EmailAddress] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [CellPhone] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [PhoneNumber] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Address1] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Address2] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [City] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [State] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ZipCode] nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Country] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [DateOfBirth] date NULL,
    [WorkTitle] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Gender] nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [MemberType] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [JoinDate] date NULL,
    [ExpirationDate] date NULL,
    [LastRenewalDate] date NULL,
    [ChapterID] int NULL,
    [EmergencyContactName] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [EmergencyContactRelationship] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [EmergencyContactPhone] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [EmergencyContactEmail] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [CreatedDate] datetime2(7) NULL CONSTRAINT [DF__members__Created__73BA3083] DEFAULT (sysdatetime()),
    [LastUpdated] datetime2(7) NOT NULL CONSTRAINT [DF_members_LastUpdated] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [membership].[members] ADD CONSTRAINT [PK__members__E1A33BDC2CDDD5B8] PRIMARY KEY CLUSTERED ([AGAID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_members_ChapterID] ON [membership].[members] ([ChapterID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_members_EmailAddress] ON [membership].[members] ([EmailAddress] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_members_LastName] ON [membership].[members] ([LastName] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_members_Lookup_Last_First] ON [membership].[members] ([LastName] ASC, [FirstName] ASC) INCLUDE ([AGAID], [MemberType], [ExpirationDate], [ChapterID], [State]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_members_Lookup_First_Last] ON [membership].[members] ([FirstName] ASC, [LastName] ASC) INCLUDE ([AGAID], [MemberType], [ExpirationDate], [ChapterID], [State]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [membership].[members] WITH CHECK ADD CONSTRAINT [FK_member_chapter] FOREIGN KEY ([ChapterID]) REFERENCES [membership].[chapters] ([ChapterID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
