-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[zz_20260325_bayrate_event_player_ratings] (
    [ModelLabel] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [RunID] bigint NOT NULL,
    [EventOrdinal] int NOT NULL,
    [EventKey] nvarchar(300) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [EventDate] date NOT NULL,
    [Tournament_Code] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [AGAID] int NOT NULL,
    [RankSeed] float NOT NULL,
    [SeedBefore] float NOT NULL,
    [PriorRating] float NULL,
    [PriorSigma] float NULL,
    [RatingAfter] float NOT NULL,
    [SigmaAfter] float NOT NULL,
    [CreatedDate] datetime2(7) NOT NULL CONSTRAINT [DF_bayrate_event_player_ratings_CreatedDate] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [ratings].[zz_20260325_bayrate_event_player_ratings] ADD CONSTRAINT [PK_bayrate_event_player_ratings] PRIMARY KEY CLUSTERED ([ModelLabel] ASC, [AGAID] ASC, [EventOrdinal] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [ratings].[zz_20260325_bayrate_event_player_ratings] WITH CHECK ADD CONSTRAINT [FK_bayrate_event_player_ratings_RunID] FOREIGN KEY ([RunID]) REFERENCES [ratings].[zz_20260325_bayrate_runs] ([RunID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
