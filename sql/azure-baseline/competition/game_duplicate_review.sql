-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [competition].[game_duplicate_review] (
    [ReviewID] bigint IDENTITY(1,1) NOT NULL,
    [ReviewType] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [ReviewKey] nvarchar(400) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [GameDate] date NOT NULL,
    [TournamentCode] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [PlayerLowAGAID] int NULL,
    [PlayerHighAGAID] int NULL,
    [Pin_Player_1] int NULL,
    [Color_1] nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Pin_Player_2] int NULL,
    [Color_2] nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Handicap] int NULL,
    [Komi] int NULL,
    [Result] nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [DuplicateCount] int NOT NULL,
    [ExcludedCount] int NOT NULL,
    [MinGameID] int NULL,
    [MaxGameID] int NULL,
    [Status] nvarchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_game_duplicate_review_Status] DEFAULT (N'pending'),
    [ReviewerNote] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [CanonicalGameID] int NULL,
    [CreatedDate] datetime2(7) NOT NULL CONSTRAINT [DF_game_duplicate_review_CreatedDate] DEFAULT (sysdatetime()),
    [LastRefreshed] datetime2(7) NOT NULL CONSTRAINT [DF_game_duplicate_review_LastRefreshed] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [competition].[game_duplicate_review] ADD CONSTRAINT [PK_game_duplicate_review] PRIMARY KEY CLUSTERED ([ReviewID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [competition].[game_duplicate_review] ADD CONSTRAINT [UX_game_duplicate_review_ReviewType_ReviewKey] UNIQUE NONCLUSTERED ([ReviewType] ASC, [ReviewKey] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE NONCLUSTERED INDEX [IX_game_duplicate_review_Status] ON [competition].[game_duplicate_review] ([Status] ASC, [GameDate] ASC, [TournamentCode] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
