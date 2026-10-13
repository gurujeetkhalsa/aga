-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[admin_permissions] (
    [AdminPermissionID] int IDENTITY(1,1) NOT NULL,
    [Principal_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Principal_Id] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Display_Name] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Permission_Code] nvarchar(64) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Is_Active] bit NOT NULL CONSTRAINT [DF_admin_permissions_Is_Active] DEFAULT ((1)),
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_admin_permissions_Created_At] DEFAULT (sysutcdatetime()),
    [Created_By] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL CONSTRAINT [DF_admin_permissions_Created_By] DEFAULT (suser_sname()),
    [Revoked_At] datetime2(0) NULL,
    [Revoked_By] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 );
GO
ALTER TABLE [ratings].[admin_permissions] ADD CONSTRAINT [PK_admin_permissions] PRIMARY KEY CLUSTERED ([AdminPermissionID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [ratings].[admin_permissions] ADD CONSTRAINT [UQ_admin_permissions_Principal_Permission] UNIQUE NONCLUSTERED ([Principal_Name] ASC, [Permission_Code] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_admin_permissions_Principal_Id] ON [ratings].[admin_permissions] ([Principal_Id] ASC, [Permission_Code] ASC) WHERE ([Principal_Id] IS NOT NULL) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_admin_permissions_Permission_Active] ON [ratings].[admin_permissions] ([Permission_Code] ASC, [Is_Active] ASC, [Principal_Name] ASC) INCLUDE ([Principal_Id], [Display_Name]) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
