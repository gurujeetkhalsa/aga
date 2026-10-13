-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [events].[rehearsal_clocks] (
    [occurrence_id] bigint NOT NULL,
    [stage] int NOT NULL,
    [simulated_at_utc] datetime2(0) NOT NULL,
    [updated_at_utc] datetime2(0) NOT NULL CONSTRAINT [DF__rehearsal__updat__591C1437] DEFAULT (sysutcdatetime())
 );
GO
ALTER TABLE [events].[rehearsal_clocks] ADD CONSTRAINT [PK__rehearsa__674D4A800C13F8CD] PRIMARY KEY CLUSTERED ([occurrence_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
ALTER TABLE [events].[rehearsal_clocks] WITH CHECK ADD CONSTRAINT [CK__rehearsal__stage__5827EFFE] CHECK ([stage]>=(0) AND [stage]<=(4));
GO
ALTER TABLE [events].[rehearsal_clocks] WITH CHECK ADD CONSTRAINT [FK__rehearsal__occur__5733CBC5] FOREIGN KEY ([occurrence_id]) REFERENCES [events].[event_occurrences] ([occurrence_id]) ON DELETE NO ACTION ON UPDATE NO ACTION;
GO
