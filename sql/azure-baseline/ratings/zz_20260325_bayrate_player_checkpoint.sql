-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[zz_20260325_bayrate_player_checkpoint] (
    [ModelLabel] nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [AGAID] int NOT NULL,
    [EventOrdinal] int NOT NULL,
    [EventKey] nvarchar(300) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [EventDate] date NOT NULL,
    [Rating] float NOT NULL,
    [Sigma] float NOT NULL,
    [LastRatingDate] date NOT NULL,
    [MomentumStreak] int NOT NULL CONSTRAINT [DF_bayrate_player_checkpoint_MomentumStreak] DEFAULT ((0)),
    [RunID] bigint NOT NULL,
    [CreatedDate] datetime2(7) NOT NULL CONSTRAINT [DF_bayrate_player_checkpoint_CreatedDate] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [ratings].[zz_20260325_bayrate_player_checkpoint] ADD CONSTRAINT [PK_bayrate_player_checkpoint] PRIMARY KEY CLUSTERED ([ModelLabel] ASC, [AGAID] ASC, [EventOrdinal] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [ratings].[zz_20260325_bayrate_player_checkpoint] WITH CHECK ADD CONSTRAINT [FK_bayrate_player_checkpoint_RunID] FOREIGN KEY ([RunID]) REFERENCES [ratings].[zz_20260325_bayrate_runs] ([RunID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
