-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [ratings].[bayrate_reward_reconciliations] (
    [RunID] int NOT NULL,
    [ReconciliationJson] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_bayrate_reward_reconciliations_Created_At] DEFAULT (sysutcdatetime()),
    [Created_By] nvarchar(256) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Created_Principal_Id] nvarchar(128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
 );
GO
ALTER TABLE [ratings].[bayrate_reward_reconciliations] ADD CONSTRAINT [PK_bayrate_reward_reconciliations] PRIMARY KEY CLUSTERED ([RunID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [ratings].[bayrate_reward_reconciliations] WITH CHECK ADD CONSTRAINT [CK_bayrate_reward_reconciliations_Json] CHECK (isjson([ReconciliationJson])=(1));
GO
ALTER TABLE [ratings].[bayrate_reward_reconciliations] WITH CHECK ADD CONSTRAINT [FK_bayrate_reward_reconciliations_RunID] FOREIGN KEY ([RunID]) REFERENCES [ratings].[bayrate_runs] ([RunID]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
