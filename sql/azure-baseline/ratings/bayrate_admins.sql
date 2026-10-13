-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[bayrate_admins] (
    [AdminID] int IDENTITY(1,1) NOT NULL,
    [Principal_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Principal_Id] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Display_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Is_Active] bit NOT NULL CONSTRAINT [DF_bayrate_admins_Is_Active] DEFAULT ((1)),
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_bayrate_admins_Created_At] DEFAULT (sysutcdatetime()),
    [Created_By] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL CONSTRAINT [DF_bayrate_admins_Created_By] DEFAULT (suser_sname()),
    [Revoked_At] datetime2(0) NULL,
    [Revoked_By] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 );
GO
ALTER TABLE [ratings].[bayrate_admins] ADD CONSTRAINT [PK_bayrate_admins] PRIMARY KEY CLUSTERED ([AdminID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [ratings].[bayrate_admins] ADD CONSTRAINT [UQ_bayrate_admins_Principal_Name] UNIQUE NONCLUSTERED ([Principal_Name] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_bayrate_admins_Principal_Id] ON [ratings].[bayrate_admins] ([Principal_Id] ASC) WHERE ([Principal_Id] IS NOT NULL) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
