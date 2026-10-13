-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[zz_20260325_bayrate_runs] (
    [RunID] bigint IDENTITY(1,1) NOT NULL,
    [ModelLabel] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [ConfigHash] varbinary(32) NOT NULL,
    [SourceVersion] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Status] nvarchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [ReplayFromEventOrdinal] int NOT NULL,
    [ReplayFromEventKey] nvarchar(300) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [ReplayFromEventDate] date NOT NULL,
    [CompletedThroughEventOrdinal] int NULL,
    [CompletedThroughEventKey] nvarchar(300) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [CompletedThroughEventDate] date NULL,
    [EventCount] int NULL,
    [PlayerCount] int NULL,
    [GameCount] int NULL,
    [ErrorMessage] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [StartedAt] datetime2(7) NOT NULL CONSTRAINT [DF_bayrate_runs_StartedAt] DEFAULT (sysdatetime()),
    [CompletedAt] datetime2(7) NULL,
    [CreatedDate] datetime2(7) NOT NULL CONSTRAINT [DF_bayrate_runs_CreatedDate] DEFAULT (sysdatetime()),
    [LastUpdated] datetime2(7) NOT NULL CONSTRAINT [DF_bayrate_runs_LastUpdated] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [ratings].[zz_20260325_bayrate_runs] ADD CONSTRAINT [PK_bayrate_runs] PRIMARY KEY CLUSTERED ([RunID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
