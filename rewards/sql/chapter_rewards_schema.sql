IF SCHEMA_ID(N'rewards') IS NULL
    EXEC(N'CREATE SCHEMA [rewards]');

IF OBJECT_ID(N'rewards.reward_runs', N'U') IS NOT NULL
    PRINT N'rewards.reward_runs already exists.';

IF OBJECT_ID(N'rewards.member_daily_snapshot', N'U') IS NOT NULL
    PRINT N'rewards.member_daily_snapshot already exists.';

IF OBJECT_ID(N'rewards.chapter_daily_snapshot', N'U') IS NOT NULL
    PRINT N'rewards.chapter_daily_snapshot already exists.';

IF OBJECT_ID(N'rewards.membership_events', N'U') IS NOT NULL
    PRINT N'rewards.membership_events already exists.';

IF OBJECT_ID(N'rewards.transactions', N'U') IS NOT NULL
    PRINT N'rewards.transactions already exists.';

IF OBJECT_ID(N'rewards.point_lots', N'U') IS NOT NULL
    PRINT N'rewards.point_lots already exists.';

IF OBJECT_ID(N'rewards.lot_allocations', N'U') IS NOT NULL
    PRINT N'rewards.lot_allocations already exists.';

IF OBJECT_ID(N'rewards.chapter_eligibility_periods', N'U') IS NOT NULL
    PRINT N'rewards.chapter_eligibility_periods already exists.';

IF OBJECT_ID(N'rewards.reward_runs', N'U') IS NULL
BEGIN
    CREATE TABLE [rewards].[reward_runs]
    (
        [RunID] int IDENTITY(1, 1) NOT NULL,
        [Run_Type] nvarchar(32) NOT NULL CONSTRAINT [DF_reward_runs_Run_Type] DEFAULT N'daily',
        [Snapshot_Date] date NOT NULL,
        [Started_At] datetime2(0) NOT NULL CONSTRAINT [DF_reward_runs_Started_At] DEFAULT SYSUTCDATETIME(),
        [Completed_At] datetime2(0) NULL,
        [Status] nvarchar(32) NOT NULL CONSTRAINT [DF_reward_runs_Status] DEFAULT N'running',
        [SummaryJson] nvarchar(max) NULL,
        [Error_Message] nvarchar(max) NULL,
        CONSTRAINT [PK_reward_runs] PRIMARY KEY CLUSTERED ([RunID]),
        CONSTRAINT [CK_reward_runs_Run_Type] CHECK ([Run_Type] IN (N'daily', N'manual', N'import', N'backfill')),
        CONSTRAINT [CK_reward_runs_Status] CHECK ([Status] IN (N'running', N'succeeded', N'failed', N'cancelled'))
    );
END;

IF OBJECT_ID(N'rewards.chapter_eligibility_periods', N'U') IS NULL
BEGIN
    CREATE TABLE [rewards].[chapter_eligibility_periods]
    (
        [Chapter_Eligibility_Period_ID] int IDENTITY(1, 1) NOT NULL,
        [ChapterID] int NULL,
        [Chapter_Code] nvarchar(64) NOT NULL,
        [Effective_Start_Date] date NOT NULL,
        [Effective_End_Date] date NULL,
        [Is_Current] bit NOT NULL CONSTRAINT [DF_chapter_eligibility_periods_Is_Current] DEFAULT 1,
        [Source] nvarchar(64) NOT NULL CONSTRAINT [DF_chapter_eligibility_periods_Source] DEFAULT N'manual',
        [Notes] nvarchar(max) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_eligibility_periods_Created_At] DEFAULT SYSUTCDATETIME(),
        [Created_By] nvarchar(128) NULL CONSTRAINT [DF_chapter_eligibility_periods_Created_By] DEFAULT SUSER_SNAME(),
        CONSTRAINT [PK_chapter_eligibility_periods] PRIMARY KEY CLUSTERED ([Chapter_Eligibility_Period_ID]),
        CONSTRAINT [CK_chapter_eligibility_periods_Date_Range] CHECK ([Effective_End_Date] IS NULL OR [Effective_End_Date] >= [Effective_Start_Date])
    );
END;

IF OBJECT_ID(N'rewards.member_daily_snapshot', N'U') IS NULL
BEGIN
    CREATE TABLE [rewards].[member_daily_snapshot]
    (
        [Snapshot_Date] date NOT NULL,
        [AGAID] int NOT NULL,
        [Member_Type] nvarchar(128) NULL,
        [Expiration_Date] date NULL,
        [ChapterID] int NULL,
        [Chapter_Code] nvarchar(64) NULL,
        [Is_Active] bit NOT NULL,
        [Is_Tournament_Pass] bit NOT NULL,
        [Created_RunID] int NOT NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_member_daily_snapshot_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_member_daily_snapshot] PRIMARY KEY CLUSTERED ([Snapshot_Date], [AGAID]),
        CONSTRAINT [FK_member_daily_snapshot_RunID] FOREIGN KEY ([Created_RunID]) REFERENCES [rewards].[reward_runs] ([RunID])
    );
END;

IF OBJECT_ID(N'rewards.chapter_daily_snapshot', N'U') IS NULL
BEGIN
    CREATE TABLE [rewards].[chapter_daily_snapshot]
    (
        [Snapshot_Date] date NOT NULL,
        [ChapterID] int NOT NULL,
        [Chapter_Code] nvarchar(64) NOT NULL,
        [Chapter_Name] nvarchar(256) NULL,
        [Is_Current] bit NOT NULL CONSTRAINT [DF_chapter_daily_snapshot_Is_Current] DEFAULT 1,
        [Active_Member_Count] int NOT NULL,
        [Multiplier] tinyint NOT NULL,
        [Created_RunID] int NOT NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_chapter_daily_snapshot_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_chapter_daily_snapshot] PRIMARY KEY CLUSTERED ([Snapshot_Date], [ChapterID]),
        CONSTRAINT [FK_chapter_daily_snapshot_RunID] FOREIGN KEY ([Created_RunID]) REFERENCES [rewards].[reward_runs] ([RunID]),
        CONSTRAINT [CK_chapter_daily_snapshot_Active_Member_Count] CHECK ([Active_Member_Count] >= 0),
        CONSTRAINT [CK_chapter_daily_snapshot_Multiplier] CHECK ([Multiplier] IN (1, 2, 3))
    );
END;

IF OBJECT_ID(N'rewards.membership_events', N'U') IS NULL
BEGIN
    CREATE TABLE [rewards].[membership_events]
    (
        [Membership_Event_ID] bigint IDENTITY(1, 1) NOT NULL,
        [Message_ID] nvarchar(256) NOT NULL,
        [AGAID] int NOT NULL,
        [Event_Type] nvarchar(32) NOT NULL,
        [Event_Date] date NOT NULL,
        [Received_At] datetime2(0) NOT NULL,
        [Member_Type] nvarchar(128) NULL,
        [Base_Points] int NOT NULL,
        [Term_Years] int NOT NULL CONSTRAINT [DF_membership_events_Term_Years] DEFAULT 1,
        [Credit_Deadline] date NOT NULL,
        [Status] nvarchar(32) NOT NULL CONSTRAINT [DF_membership_events_Status] DEFAULT N'pending',
        [Credited_TransactionID] bigint NULL,
        [Expired_At] datetime2(0) NULL,
        [Source_Payload_Json] nvarchar(max) NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_membership_events_Created_At] DEFAULT SYSUTCDATETIME(),
        [Updated_At] datetime2(0) NOT NULL CONSTRAINT [DF_membership_events_Updated_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_membership_events] PRIMARY KEY CLUSTERED ([Membership_Event_ID]),
        CONSTRAINT [UQ_membership_events_Message_AGAID_Type] UNIQUE ([Message_ID], [AGAID], [Event_Type]),
        CONSTRAINT [CK_membership_events_Event_Type] CHECK ([Event_Type] IN (N'new_membership', N'renewal', N'lifetime')),
        CONSTRAINT [CK_membership_events_Status] CHECK ([Status] IN (N'pending', N'credited', N'expired_no_chapter', N'ineligible')),
        CONSTRAINT [CK_membership_events_Base_Points] CHECK ([Base_Points] >= 0),
        CONSTRAINT [CK_membership_events_Term_Years] CHECK ([Term_Years] >= 1),
        CONSTRAINT [CK_membership_events_Credit_Deadline] CHECK ([Credit_Deadline] >= [Event_Date])
    );
END;

IF OBJECT_ID(N'rewards.transactions', N'U') IS NULL
BEGIN
    CREATE TABLE [rewards].[transactions]
    (
        [TransactionID] bigint IDENTITY(1, 1) NOT NULL,
        [ChapterID] int NOT NULL,
        [Chapter_Code] nvarchar(64) NOT NULL,
        [Transaction_Type] nvarchar(32) NOT NULL,
        [Points_Delta] int NOT NULL,
        [Base_Points] int NULL,
        [Multiplier] tinyint NULL,
        [Chapter_Active_Member_Count] int NULL,
        [Effective_Date] date NOT NULL,
        [Earned_Date] date NULL,
        [Valuation_Date] date NULL,
        [Posted_At] datetime2(0) NOT NULL CONSTRAINT [DF_transactions_Posted_At] DEFAULT SYSUTCDATETIME(),
        [RunID] int NULL,
        [Source_Type] nvarchar(64) NOT NULL,
        [Source_Key] nvarchar(256) NOT NULL,
        [Rule_Version] nvarchar(32) NOT NULL CONSTRAINT [DF_transactions_Rule_Version] DEFAULT N'2026-05-02',
        [MetadataJson] nvarchar(max) NULL,
        [Created_By] nvarchar(128) NULL CONSTRAINT [DF_transactions_Created_By] DEFAULT SUSER_SNAME(),
        CONSTRAINT [PK_transactions] PRIMARY KEY CLUSTERED ([TransactionID]),
        CONSTRAINT [FK_transactions_RunID] FOREIGN KEY ([RunID]) REFERENCES [rewards].[reward_runs] ([RunID]),
        CONSTRAINT [UQ_transactions_Source] UNIQUE ([Source_Type], [Source_Key], [Transaction_Type], [ChapterID]),
        CONSTRAINT [CK_transactions_Type] CHECK ([Transaction_Type] IN (N'earn', N'redeem', N'expire', N'transfer_in', N'transfer_out', N'adjustment', N'reversal')),
        CONSTRAINT [CK_transactions_Points_Delta_Nonzero] CHECK ([Points_Delta] <> 0),
        CONSTRAINT [CK_transactions_Points_Delta_Sign] CHECK
        (
            ([Transaction_Type] IN (N'earn', N'transfer_in') AND [Points_Delta] > 0)
            OR ([Transaction_Type] IN (N'redeem', N'expire', N'transfer_out') AND [Points_Delta] < 0)
            OR ([Transaction_Type] IN (N'adjustment', N'reversal'))
        ),
        CONSTRAINT [CK_transactions_Earned_Date] CHECK
        (
            [Transaction_Type] NOT IN (N'earn', N'transfer_in')
            OR [Earned_Date] IS NOT NULL
        ),
        CONSTRAINT [CK_transactions_Multiplier] CHECK ([Multiplier] IS NULL OR [Multiplier] IN (1, 2, 3)),
        CONSTRAINT [CK_transactions_Chapter_Active_Member_Count] CHECK ([Chapter_Active_Member_Count] IS NULL OR [Chapter_Active_Member_Count] >= 0)
    );
END;

IF OBJECT_ID(N'rewards.point_lots', N'U') IS NULL
BEGIN
    CREATE TABLE [rewards].[point_lots]
    (
        [LotID] bigint IDENTITY(1, 1) NOT NULL,
        [Earn_TransactionID] bigint NOT NULL,
        [ChapterID] int NOT NULL,
        [Chapter_Code] nvarchar(64) NOT NULL,
        [Original_Points] int NOT NULL,
        [Remaining_Points] int NOT NULL,
        [Earned_Date] date NOT NULL,
        [Expires_On] date NOT NULL,
        [Source_Type] nvarchar(64) NOT NULL,
        [Source_Key] nvarchar(256) NOT NULL,
        [Created_At] datetime2(0) NOT NULL CONSTRAINT [DF_point_lots_Created_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_point_lots] PRIMARY KEY CLUSTERED ([LotID]),
        CONSTRAINT [FK_point_lots_Earn_TransactionID] FOREIGN KEY ([Earn_TransactionID]) REFERENCES [rewards].[transactions] ([TransactionID]),
        CONSTRAINT [UQ_point_lots_Earn_TransactionID] UNIQUE ([Earn_TransactionID]),
        CONSTRAINT [CK_point_lots_Original_Points] CHECK ([Original_Points] > 0),
        CONSTRAINT [CK_point_lots_Remaining_Points] CHECK ([Remaining_Points] >= 0 AND [Remaining_Points] <= [Original_Points]),
        CONSTRAINT [CK_point_lots_Expires_On] CHECK ([Expires_On] >= [Earned_Date])
    );
END;

IF OBJECT_ID(N'rewards.lot_allocations', N'U') IS NULL
BEGIN
    CREATE TABLE [rewards].[lot_allocations]
    (
        [AllocationID] bigint IDENTITY(1, 1) NOT NULL,
        [Debit_TransactionID] bigint NOT NULL,
        [LotID] bigint NOT NULL,
        [Points_Allocated] int NOT NULL,
        [Allocated_At] datetime2(0) NOT NULL CONSTRAINT [DF_lot_allocations_Allocated_At] DEFAULT SYSUTCDATETIME(),
        CONSTRAINT [PK_lot_allocations] PRIMARY KEY CLUSTERED ([AllocationID]),
        CONSTRAINT [FK_lot_allocations_Debit_TransactionID] FOREIGN KEY ([Debit_TransactionID]) REFERENCES [rewards].[transactions] ([TransactionID]),
        CONSTRAINT [FK_lot_allocations_LotID] FOREIGN KEY ([LotID]) REFERENCES [rewards].[point_lots] ([LotID]),
        CONSTRAINT [UQ_lot_allocations_Debit_Lot] UNIQUE ([Debit_TransactionID], [LotID]),
        CONSTRAINT [CK_lot_allocations_Points_Allocated] CHECK ([Points_Allocated] > 0)
    );
END;

IF OBJECT_ID(N'rewards.membership_events', N'U') IS NOT NULL
   AND OBJECT_ID(N'rewards.transactions', N'U') IS NOT NULL
   AND NOT EXISTS
   (
       SELECT 1
       FROM sys.foreign_keys
       WHERE [name] = N'FK_membership_events_Credited_TransactionID'
         AND [parent_object_id] = OBJECT_ID(N'rewards.membership_events')
   )
BEGIN
    ALTER TABLE [rewards].[membership_events]
        ADD CONSTRAINT [FK_membership_events_Credited_TransactionID]
        FOREIGN KEY ([Credited_TransactionID]) REFERENCES [rewards].[transactions] ([TransactionID]);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_reward_runs_Snapshot_Date'
      AND [object_id] = OBJECT_ID(N'rewards.reward_runs')
)
BEGIN
    CREATE INDEX [IX_reward_runs_Snapshot_Date]
        ON [rewards].[reward_runs] ([Snapshot_Date], [Run_Type], [Status]);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_chapter_eligibility_periods_Chapter_Date'
      AND [object_id] = OBJECT_ID(N'rewards.chapter_eligibility_periods')
)
BEGIN
    CREATE INDEX [IX_chapter_eligibility_periods_Chapter_Date]
        ON [rewards].[chapter_eligibility_periods] ([Chapter_Code], [Effective_Start_Date], [Effective_End_Date])
        INCLUDE ([ChapterID], [Is_Current]);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_member_daily_snapshot_Chapter'
      AND [object_id] = OBJECT_ID(N'rewards.member_daily_snapshot')
)
BEGIN
    CREATE INDEX [IX_member_daily_snapshot_Chapter]
        ON [rewards].[member_daily_snapshot] ([Snapshot_Date], [ChapterID], [Is_Active], [Is_Tournament_Pass])
        INCLUDE ([Chapter_Code], [Member_Type], [Expiration_Date]);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_member_daily_snapshot_AGAID_Date'
      AND [object_id] = OBJECT_ID(N'rewards.member_daily_snapshot')
)
BEGIN
    CREATE INDEX [IX_member_daily_snapshot_AGAID_Date]
        ON [rewards].[member_daily_snapshot] ([AGAID], [Snapshot_Date])
        INCLUDE ([ChapterID], [Chapter_Code], [Is_Active], [Member_Type]);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_chapter_daily_snapshot_Code'
      AND [object_id] = OBJECT_ID(N'rewards.chapter_daily_snapshot')
)
BEGIN
    CREATE UNIQUE INDEX [IX_chapter_daily_snapshot_Code]
        ON [rewards].[chapter_daily_snapshot] ([Snapshot_Date], [Chapter_Code]);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_membership_events_Status_Deadline'
      AND [object_id] = OBJECT_ID(N'rewards.membership_events')
)
BEGIN
    CREATE INDEX [IX_membership_events_Status_Deadline]
        ON [rewards].[membership_events] ([Status], [Credit_Deadline], [Event_Date])
        INCLUDE ([AGAID], [Event_Type], [Member_Type], [Base_Points]);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_membership_events_AGAID'
      AND [object_id] = OBJECT_ID(N'rewards.membership_events')
)
BEGIN
    CREATE INDEX [IX_membership_events_AGAID]
        ON [rewards].[membership_events] ([AGAID], [Event_Date]);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_transactions_Chapter_Date'
      AND [object_id] = OBJECT_ID(N'rewards.transactions')
)
BEGIN
    CREATE INDEX [IX_transactions_Chapter_Date]
        ON [rewards].[transactions] ([ChapterID], [Effective_Date], [TransactionID])
        INCLUDE ([Chapter_Code], [Transaction_Type], [Points_Delta], [Earned_Date], [Source_Type], [Source_Key]);
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_transactions_RunID'
      AND [object_id] = OBJECT_ID(N'rewards.transactions')
)
BEGIN
    CREATE INDEX [IX_transactions_RunID]
        ON [rewards].[transactions] ([RunID], [Transaction_Type], [TransactionID])
        WHERE [RunID] IS NOT NULL;
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_point_lots_Chapter_Expires'
      AND [object_id] = OBJECT_ID(N'rewards.point_lots')
)
BEGIN
    CREATE INDEX [IX_point_lots_Chapter_Expires]
        ON [rewards].[point_lots] ([ChapterID], [Expires_On], [Earned_Date], [LotID])
        INCLUDE ([Chapter_Code], [Original_Points], [Remaining_Points])
        WHERE [Remaining_Points] > 0;
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE [name] = N'IX_lot_allocations_LotID'
      AND [object_id] = OBJECT_ID(N'rewards.lot_allocations')
)
BEGIN
    CREATE INDEX [IX_lot_allocations_LotID]
        ON [rewards].[lot_allocations] ([LotID], [Debit_TransactionID])
        INCLUDE ([Points_Allocated]);
END;
