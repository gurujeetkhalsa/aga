-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [dbrepl_meta].[generations] (
    [object_id] int NOT NULL,
    [incarnation] uniqueidentifier NOT NULL CONSTRAINT [DF__generatio__incar__57DDD73B] DEFAULT (newid()),
    [generation] bigint NOT NULL CONSTRAINT [DF__generatio__gener__58D1FB74] DEFAULT ((0))
 );
GO
ALTER TABLE [dbrepl_meta].[generations] ADD CONSTRAINT [PK__generati__3DC088B52837C2C4] PRIMARY KEY CLUSTERED ([object_id] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
