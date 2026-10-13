-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[zz_20260325_bayrate_control] (
    [ModelLabel] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [CutoverEventOrdinal] int NULL,
    [SnapshotSeedEnabled] bit NOT NULL CONSTRAINT [DF_bayrate_control_SnapshotSeedEnabled] DEFAULT ((0)),
    [FreezeBeforeCutover] bit NOT NULL CONSTRAINT [DF_bayrate_control_FreezeBeforeCutover] DEFAULT ((0)),
    [CreatedDate] datetime2(7) NOT NULL CONSTRAINT [DF_bayrate_control_CreatedDate] DEFAULT (sysdatetime()),
    [LastUpdated] datetime2(7) NOT NULL CONSTRAINT [DF_bayrate_control_LastUpdated] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [ratings].[zz_20260325_bayrate_control] ADD CONSTRAINT [PK_bayrate_control] PRIMARY KEY CLUSTERED ([ModelLabel] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
