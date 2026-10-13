-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [competition].[tournament_duplicate_review] (
    [ReviewID] bigint IDENTITY(1,1) NOT NULL,
    [ReviewType] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [ReviewKey] nvarchar(400) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [CodeBase] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [NormalizedName] nvarchar(400) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [TournamentDate] date NULL,
    [VariantCount] int NOT NULL,
    [TournamentCodes] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [TournamentDescriptions] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Locations] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Status] nvarchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_tournament_duplicate_review_Status] DEFAULT (N'pending'),
    [ReviewerNote] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [CanonicalTournamentCode] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [CreatedDate] datetime2(7) NOT NULL CONSTRAINT [DF_tournament_duplicate_review_CreatedDate] DEFAULT (sysdatetime()),
    [LastRefreshed] datetime2(7) NOT NULL CONSTRAINT [DF_tournament_duplicate_review_LastRefreshed] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [competition].[tournament_duplicate_review] ADD CONSTRAINT [PK_tournament_duplicate_review] PRIMARY KEY CLUSTERED ([ReviewID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [competition].[tournament_duplicate_review] ADD CONSTRAINT [UX_tournament_duplicate_review_ReviewType_ReviewKey] UNIQUE NONCLUSTERED ([ReviewType] ASC, [ReviewKey] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_tournament_duplicate_review_Status] ON [competition].[tournament_duplicate_review] ([Status] ASC, [TournamentDate] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
