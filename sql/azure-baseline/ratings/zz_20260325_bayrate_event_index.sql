-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[zz_20260325_bayrate_event_index] (
    [ModelLabel] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [EventOrdinal] int NOT NULL,
    [EventKey] nvarchar(300) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [EventDate] date NOT NULL,
    [Tournament_Code] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [SourceHash] varbinary(32) NOT NULL,
    [Dirty] bit NOT NULL CONSTRAINT [DF_bayrate_event_index_Dirty] DEFAULT ((0)),
    [DirtyReason] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [LastComputedRunID] bigint NULL,
    [LastComputedAt] datetime2(7) NULL,
    [CreatedDate] datetime2(7) NOT NULL CONSTRAINT [DF_bayrate_event_index_CreatedDate] DEFAULT (sysdatetime()),
    [LastUpdated] datetime2(7) NOT NULL CONSTRAINT [DF_bayrate_event_index_LastUpdated] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [ratings].[zz_20260325_bayrate_event_index] ADD CONSTRAINT [PK_bayrate_event_index] PRIMARY KEY CLUSTERED ([ModelLabel] ASC, [EventOrdinal] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [ratings].[zz_20260325_bayrate_event_index] ADD CONSTRAINT [UX_bayrate_event_index_Model_EventKey] UNIQUE NONCLUSTERED ([ModelLabel] ASC, [EventKey] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
