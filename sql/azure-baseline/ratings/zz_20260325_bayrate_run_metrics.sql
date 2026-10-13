-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[zz_20260325_bayrate_run_metrics] (
    [RunID] bigint NOT NULL,
    [MetricName] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [MetricValue] float NOT NULL,
    [MetricText] nvarchar(400) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [CreatedDate] datetime2(7) NOT NULL CONSTRAINT [DF_bayrate_run_metrics_CreatedDate] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [ratings].[zz_20260325_bayrate_run_metrics] ADD CONSTRAINT [PK_bayrate_run_metrics] PRIMARY KEY CLUSTERED ([RunID] ASC, [MetricName] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [ratings].[zz_20260325_bayrate_run_metrics] WITH CHECK ADD CONSTRAINT [FK_bayrate_run_metrics_RunID] FOREIGN KEY ([RunID]) REFERENCES [ratings].[zz_20260325_bayrate_runs] ([RunID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
