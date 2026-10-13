-- Live Azure SQL metadata; database aga-membership; USER_TABLE
-- Review dependencies before use. Not a migration to PostgreSQL.
CREATE TABLE [integration].[clubexpress_email_log] (
    [EmailLogID] int IDENTITY(1,1) NOT NULL,
    [MessageId] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [MessageType] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
    [Sender] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Subject] nvarchar(500) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ReceivedAt] datetime2(7) NOT NULL,
    [BlobPath] nvarchar(400) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [Status] nvarchar(30) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL CONSTRAINT [DF_clubexpress_email_log_Status] DEFAULT (N'received'),
    [ErrorMessage] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    [ProcessedAt] datetime2(7) NULL,
    [CreatedDate] datetime2(7) NOT NULL CONSTRAINT [DF_clubexpress_email_log_CreatedDate] DEFAULT (sysdatetime()),
    [LastUpdated] datetime2(7) NOT NULL CONSTRAINT [DF_clubexpress_email_log_LastUpdated] DEFAULT (sysdatetime())
 );
GO
ALTER TABLE [integration].[clubexpress_email_log] ADD CONSTRAINT [PK_clubexpress_email_log] PRIMARY KEY CLUSTERED ([EmailLogID] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
CREATE UNIQUE NONCLUSTERED INDEX [UX_clubexpress_email_log_MessageId_MessageType] ON [integration].[clubexpress_email_log] ([MessageId] ASC, [MessageType] ASC) WITH (PAD_INDEX = OFF, FILLFACTOR = 0, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON);
GO
