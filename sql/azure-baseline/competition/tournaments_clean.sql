-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [competition].[tournaments_clean] (
    [CanonicalTournamentCode] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [RepresentativeTournamentCode] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Tournament_Descr] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Tournament_Date] date NULL,
    [City] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [State_Code] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Country_Code] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Rounds] int NULL,
    [Total_Players] int NULL,
    [Wallist] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Elab_Date] date NULL,
    [SourceStatus] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [SourceTournamentCodes] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [SourceTournamentCount] int NOT NULL,
    [HasPendingTournamentReview] bit NOT NULL,
    [CreatedDate] datetime2(7) NOT NULL CONSTRAINT [DF_tournaments_clean_CreatedDate] DEFAULT (sysdatetime()),
    [LastRefreshed] datetime2(7) NOT NULL CONSTRAINT [DF_tournaments_clean_LastRefreshed] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [competition].[tournaments_clean] ADD CONSTRAINT [PK_tournaments_clean] PRIMARY KEY CLUSTERED ([CanonicalTournamentCode] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
