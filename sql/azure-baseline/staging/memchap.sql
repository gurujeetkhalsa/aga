-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [staging].[memchap] (
    [AGAID] int NULL,
    [MemberType] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [FirstName] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [MiddleInitial] nvarchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [LastName] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Nickname] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Pronouns] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [LoginName] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Status] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [LastLogin] datetime2(7) NULL,
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
    [JoinDate] date NULL,
    [ExpirationDate] date NULL,
    [LastRenewalDate] date NULL,
    [ChapterID] int NULL,
    [EmergencyContactName] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [EmergencyContactRelationship] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [EmergencyContactPhone] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [EmergencyContactEmail] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 );
GO
CREATE NONCLUSTERED INDEX [IX_memchap_AGAID] ON [staging].[memchap] ([AGAID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
