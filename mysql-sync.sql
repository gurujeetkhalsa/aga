IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'integration')
BEGIN
    EXEC(N'CREATE SCHEMA [integration]');
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'ratingsync')
BEGIN
    EXEC(N'CREATE SCHEMA [ratingsync]');
END;
GO

IF OBJECT_ID(N'[integration].[mysql_sync_runs]', N'U') IS NULL
BEGIN
    CREATE TABLE [integration].[mysql_sync_runs]
    (
        [RunID] BIGINT IDENTITY(1,1) NOT NULL,
        [JobName] NVARCHAR(100) NOT NULL,
        [SourceTable] NVARCHAR(128) NOT NULL,
        [TargetTable] NVARCHAR(128) NOT NULL,
        [StartedAt] DATETIME2(7) NOT NULL,
        [CompletedAt] DATETIME2(7) NULL,
        [Status] NVARCHAR(30) NOT NULL,
        [RowCount] INT NULL,
        [ErrorMessage] NVARCHAR(MAX) NULL,
        [CreatedDate] DATETIME2(7) NOT NULL
            CONSTRAINT [DF_mysql_sync_runs_CreatedDate] DEFAULT (SYSDATETIME()),
        CONSTRAINT [PK_mysql_sync_runs] PRIMARY KEY CLUSTERED ([RunID] ASC)
    );

    CREATE NONCLUSTERED INDEX [IX_mysql_sync_runs_JobName_StartedAt]
        ON [integration].[mysql_sync_runs] ([JobName] ASC, [StartedAt] DESC);
END;
GO
