-- Copyright 2026, American Go Association, All rights reserved

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'rewards') IS NULL
    EXEC(N'CREATE SCHEMA [rewards]');
GO

CREATE OR ALTER PROCEDURE [rewards].[sp_apply_bayrate_reward_reconciliation]
    @BayrateRunID int,
    @DryRun bit = 1,
    @AppliedByPrincipalName nvarchar(256) = NULL,
    @AppliedByPrincipalId nvarchar(128) = NULL,
    @RuleVersion nvarchar(32) = N'bayrate-rerun-v1'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @BayrateRunID IS NULL OR @BayrateRunID <= 0
        THROW 52900, N'BayrateRunID must be a positive integer.', 1;

    IF OBJECT_ID(N'ratings.bayrate_runs', N'U') IS NULL
       OR OBJECT_ID(N'ratings.bayrate_reward_reconciliations', N'U') IS NULL
       OR OBJECT_ID(N'ratings.tournaments', N'U') IS NULL
       OR OBJECT_ID(N'rewards.reward_runs', N'U') IS NULL
       OR OBJECT_ID(N'rewards.transactions', N'U') IS NULL
       OR OBJECT_ID(N'rewards.point_lots', N'U') IS NULL
       OR OBJECT_ID(N'rewards.lot_allocations', N'U') IS NULL
       OR OBJECT_ID(N'rewards.bayrate_reconciliation_applications', N'U') IS NULL
    BEGIN
        THROW 52901, N'Bayrate reconciliation or Chapter Rewards schema is incomplete.', 1;
    END;

    DECLARE
        @ReconciliationJson nvarchar(max),
        @BayrateCommitStatus nvarchar(32),
        @EffectiveDate date,
        @Today date = CAST(SYSUTCDATETIME() AS date),
        @RewardRunID int,
        @ExistingStatus nvarchar(32),
        @ExistingSummaryJson nvarchar(max),
        @ExistingAppliedAt datetime2(0),
        @ExistingAppliedBy nvarchar(256),
        @ExistingTransactionCount int,
        @ExistingCreditPoints int,
        @ExistingDebitPoints int,
        @ExistingNetPoints int;

    IF @DryRun = 0
        BEGIN TRANSACTION;

    BEGIN TRY
        SELECT
            @ReconciliationJson = reconciliation.[ReconciliationJson],
            @BayrateCommitStatus = JSON_VALUE(
                CASE WHEN ISJSON(run.[SummaryJson]) = 1 THEN run.[SummaryJson] ELSE N'{}' END,
                N'$.commit_status'
            )
        FROM [ratings].[bayrate_reward_reconciliations] AS reconciliation
        INNER JOIN [ratings].[bayrate_runs] AS run
            ON run.[RunID] = reconciliation.[RunID]
        WHERE reconciliation.[RunID] = @BayrateRunID;

        IF @ReconciliationJson IS NULL
            THROW 52902, N'No persisted Chapter Rewards reconciliation exists for this Bayrate run.', 1;

        IF ISJSON(@ReconciliationJson) <> 1
           OR JSON_QUERY(@ReconciliationJson, N'$.chapters') IS NULL
           OR JSON_QUERY(@ReconciliationJson, N'$.scope_tournament_codes') IS NULL
        BEGIN
            THROW 52904, N'The persisted reconciliation is missing its chapter or tournament scope.', 1;
        END;

        SELECT
            @RewardRunID = application.[Reward_RunID],
            @ExistingStatus = application.[Application_Status],
            @EffectiveDate = application.[Effective_Date],
            @ExistingAppliedAt = application.[Applied_At],
            @ExistingAppliedBy = application.[Applied_By],
            @ExistingTransactionCount = application.[Transaction_Count],
            @ExistingCreditPoints = application.[Credit_Points],
            @ExistingDebitPoints = application.[Debit_Points],
            @ExistingNetPoints = application.[Net_Points],
            @ExistingSummaryJson = application.[SummaryJson]
        FROM [rewards].[bayrate_reconciliation_applications] AS application WITH (UPDLOCK, HOLDLOCK)
        WHERE application.[Bayrate_RunID] = @BayrateRunID;

        IF @ExistingStatus IS NOT NULL
        BEGIN
            IF @DryRun = 0 AND XACT_STATE() = 1
                COMMIT TRANSACTION;

            SELECT
                @BayrateRunID AS [Bayrate_RunID],
                @RewardRunID AS [Reward_RunID],
                CAST(@DryRun AS bit) AS [Dry_Run],
                CAST(1 AS bit) AS [Already_Applied],
                CAST(0 AS bit) AS [Can_Apply],
                @ExistingStatus AS [Application_Status],
                @EffectiveDate AS [Effective_Date],
                @ExistingAppliedAt AS [Applied_At],
                @ExistingAppliedBy AS [Applied_By],
                @ExistingTransactionCount AS [Transaction_Count],
                @ExistingCreditPoints AS [Credit_Points],
                @ExistingDebitPoints AS [Debit_Points],
                @ExistingNetPoints AS [Net_Points],
                0 AS [Insufficient_Chapter_Count],
                0 AS [Shortfall_Points],
                JSON_QUERY(@ExistingSummaryJson, N'$.chapters') AS [ChaptersJson];
            RETURN;
        END;

        IF COALESCE(@BayrateCommitStatus, N'') <> N'committed'
            THROW 52903, N'Only a currently committed Bayrate run can be applied to Chapter Rewards.', 1;

        CREATE TABLE #ReconciliationChapters
        (
            [ChapterID] int NOT NULL PRIMARY KEY,
            [Chapter_Code] nvarchar(64) NOT NULL,
            [Chapter_Name] nvarchar(256) NULL,
            [Old_Played_Game_Points] int NOT NULL,
            [New_Played_Game_Points] int NOT NULL,
            [Old_Total_Games_Points] int NOT NULL,
            [New_Total_Games_Points] int NOT NULL,
            [Old_State_Championship_Points] int NOT NULL,
            [New_State_Championship_Points] int NOT NULL,
            [Old_Total_Points] int NOT NULL,
            [New_Total_Points] int NOT NULL,
            [Point_Difference] int NOT NULL,
            [Available_Before_Points] int NOT NULL DEFAULT 0,
            [Shortfall_Points] int NOT NULL DEFAULT 0
        );

        INSERT INTO #ReconciliationChapters
        (
            [ChapterID],
            [Chapter_Code],
            [Chapter_Name],
            [Old_Played_Game_Points],
            [New_Played_Game_Points],
            [Old_Total_Games_Points],
            [New_Total_Games_Points],
            [Old_State_Championship_Points],
            [New_State_Championship_Points],
            [Old_Total_Points],
            [New_Total_Points],
            [Point_Difference]
        )
        SELECT
            chapter.[ChapterID],
            LTRIM(RTRIM(chapter.[Chapter_Code])),
            NULLIF(LTRIM(RTRIM(chapter.[Chapter_Name])), N''),
            chapter.[Old_Played_Game_Points],
            chapter.[New_Played_Game_Points],
            chapter.[Old_Total_Games_Points],
            chapter.[New_Total_Games_Points],
            chapter.[Old_State_Championship_Points],
            chapter.[New_State_Championship_Points],
            chapter.[Old_Total_Points],
            chapter.[New_Total_Points],
            chapter.[Point_Difference]
        FROM OPENJSON(@ReconciliationJson, N'$.chapters')
        WITH
        (
            [ChapterID] int N'$.chapter_id',
            [Chapter_Code] nvarchar(64) N'$.chapter_code',
            [Chapter_Name] nvarchar(256) N'$.chapter_name',
            [Old_Played_Game_Points] int N'$.old_played_game_points',
            [New_Played_Game_Points] int N'$.new_played_game_points',
            [Old_Total_Games_Points] int N'$.old_total_games_points',
            [New_Total_Games_Points] int N'$.new_total_games_points',
            [Old_State_Championship_Points] int N'$.old_state_championship_points',
            [New_State_Championship_Points] int N'$.new_state_championship_points',
            [Old_Total_Points] int N'$.old_total_points',
            [New_Total_Points] int N'$.new_total_points',
            [Point_Difference] int N'$.point_difference'
        ) AS chapter;

        IF EXISTS
        (
            SELECT 1
            FROM #ReconciliationChapters
            WHERE [ChapterID] IS NULL
               OR NULLIF([Chapter_Code], N'') IS NULL
               OR [Point_Difference] <> [New_Total_Points] - [Old_Total_Points]
        )
        BEGIN
            THROW 52904, N'The persisted reconciliation contains invalid chapter totals.', 1;
        END;

        IF TRY_CONVERT(int, JSON_VALUE(@ReconciliationJson, N'$.old_total_points')) IS NULL
           OR TRY_CONVERT(int, JSON_VALUE(@ReconciliationJson, N'$.old_total_points'))
                <> (SELECT COALESCE(SUM([Old_Total_Points]), 0) FROM #ReconciliationChapters)
           OR TRY_CONVERT(int, JSON_VALUE(@ReconciliationJson, N'$.new_total_points')) IS NULL
           OR TRY_CONVERT(int, JSON_VALUE(@ReconciliationJson, N'$.new_total_points'))
                <> (SELECT COALESCE(SUM([New_Total_Points]), 0) FROM #ReconciliationChapters)
           OR TRY_CONVERT(int, JSON_VALUE(@ReconciliationJson, N'$.point_difference')) IS NULL
           OR TRY_CONVERT(int, JSON_VALUE(@ReconciliationJson, N'$.point_difference'))
                <> (SELECT COALESCE(SUM([Point_Difference]), 0) FROM #ReconciliationChapters)
        BEGIN
            THROW 52904, N'The persisted reconciliation summary does not match its chapter totals.', 1;
        END;

        SELECT @EffectiveDate = MAX(tournament.[Tournament_Date])
        FROM OPENJSON(@ReconciliationJson, N'$.scope_tournament_codes')
        WITH ([Tournament_Code] nvarchar(32) N'$') AS scope
        INNER JOIN [ratings].[tournaments] AS tournament
            ON tournament.[Tournament_Code] = scope.[Tournament_Code];

        SET @EffectiveDate = COALESCE(@EffectiveDate, @Today);

        ;WITH [available] AS
        (
            SELECT
                lots.[ChapterID],
                SUM(lots.[Remaining_Points]) AS [Available_Points]
            FROM [rewards].[point_lots] AS lots
            WHERE lots.[Remaining_Points] > 0
              AND lots.[Expires_On] >= @Today
            GROUP BY lots.[ChapterID]
        )
        UPDATE chapters
        SET
            [Available_Before_Points] = COALESCE(available.[Available_Points], 0),
            [Shortfall_Points] = CASE
                WHEN chapters.[Point_Difference] < 0
                 AND COALESCE(available.[Available_Points], 0) < -chapters.[Point_Difference]
                    THEN -chapters.[Point_Difference] - COALESCE(available.[Available_Points], 0)
                ELSE 0
            END
        FROM #ReconciliationChapters AS chapters
        LEFT JOIN [available] AS available
            ON available.[ChapterID] = chapters.[ChapterID];

        DECLARE
            @TransactionCount int = (SELECT COUNT(*) FROM #ReconciliationChapters WHERE [Point_Difference] <> 0),
            @CreditPoints int = (SELECT COALESCE(SUM(CASE WHEN [Point_Difference] > 0 THEN [Point_Difference] ELSE 0 END), 0) FROM #ReconciliationChapters),
            @DebitPoints int = (SELECT COALESCE(SUM(CASE WHEN [Point_Difference] < 0 THEN -[Point_Difference] ELSE 0 END), 0) FROM #ReconciliationChapters),
            @NetPoints int = (SELECT COALESCE(SUM([Point_Difference]), 0) FROM #ReconciliationChapters),
            @InsufficientChapterCount int = (SELECT COUNT(*) FROM #ReconciliationChapters WHERE [Shortfall_Points] > 0),
            @ShortfallPoints int = (SELECT COALESCE(SUM([Shortfall_Points]), 0) FROM #ReconciliationChapters),
            @CanApply bit,
            @ChaptersJson nvarchar(max),
            @ApplicationStatus nvarchar(32),
            @SummaryJson nvarchar(max);

        SET @CanApply = CASE WHEN @InsufficientChapterCount = 0 THEN 1 ELSE 0 END;

        SET @ChaptersJson =
        (
            SELECT
                chapters.[ChapterID] AS [chapter_id],
                chapters.[Chapter_Code] AS [chapter_code],
                chapters.[Chapter_Name] AS [chapter_name],
                chapters.[Old_Played_Game_Points] AS [old_played_game_points],
                chapters.[New_Played_Game_Points] AS [new_played_game_points],
                chapters.[Old_Total_Games_Points] AS [old_total_games_points],
                chapters.[New_Total_Games_Points] AS [new_total_games_points],
                chapters.[Old_State_Championship_Points] AS [old_state_championship_points],
                chapters.[New_State_Championship_Points] AS [new_state_championship_points],
                chapters.[Old_Total_Points] AS [old_total_points],
                chapters.[New_Total_Points] AS [new_total_points],
                chapters.[Point_Difference] AS [point_difference],
                chapters.[Available_Before_Points] AS [available_before_points],
                chapters.[Available_Before_Points] + chapters.[Point_Difference] AS [available_after_points],
                chapters.[Shortfall_Points] AS [shortfall_points]
            FROM #ReconciliationChapters AS chapters
            ORDER BY chapters.[Chapter_Code], chapters.[ChapterID]
            FOR JSON PATH
        );

        IF @DryRun = 1
        BEGIN
            SELECT
                @BayrateRunID AS [Bayrate_RunID],
                CAST(NULL AS int) AS [Reward_RunID],
                CAST(1 AS bit) AS [Dry_Run],
                CAST(0 AS bit) AS [Already_Applied],
                @CanApply AS [Can_Apply],
                CAST(N'pending' AS nvarchar(32)) AS [Application_Status],
                @EffectiveDate AS [Effective_Date],
                CAST(NULL AS datetime2(0)) AS [Applied_At],
                CAST(NULL AS nvarchar(256)) AS [Applied_By],
                @TransactionCount AS [Transaction_Count],
                @CreditPoints AS [Credit_Points],
                @DebitPoints AS [Debit_Points],
                @NetPoints AS [Net_Points],
                @InsufficientChapterCount AS [Insufficient_Chapter_Count],
                @ShortfallPoints AS [Shortfall_Points],
                JSON_QUERY(@ChaptersJson) AS [ChaptersJson];
            RETURN;
        END;

        IF @CanApply = 0
            THROW 52905, N'One or more chapters lack enough unexpired points for the reconciliation debit.', 1;

        INSERT INTO [rewards].[reward_runs]
        (
            [Run_Type],
            [Snapshot_Date],
            [Started_At],
            [Status],
            [SummaryJson]
        )
        VALUES
        (
            N'manual',
            @EffectiveDate,
            SYSUTCDATETIME(),
            N'running',
            (
                SELECT
                    N'bayrate_rerun_reconciliation' AS [processor],
                    @BayrateRunID AS [bayrate_run_id],
                    CAST(N'posting' AS nvarchar(32)) AS [status]
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            )
        );

        SET @RewardRunID = CONVERT(int, SCOPE_IDENTITY());

        DECLARE @InsertedTransactions table
        (
            [TransactionID] bigint NOT NULL PRIMARY KEY,
            [ChapterID] int NOT NULL,
            [Chapter_Code] nvarchar(64) NOT NULL,
            [Points_Delta] int NOT NULL,
            [Source_Key] nvarchar(256) NOT NULL
        );

        INSERT INTO [rewards].[transactions]
        (
            [ChapterID],
            [Chapter_Code],
            [Transaction_Type],
            [Points_Delta],
            [Base_Points],
            [Multiplier],
            [Chapter_Active_Member_Count],
            [Effective_Date],
            [Earned_Date],
            [Valuation_Date],
            [Posted_At],
            [RunID],
            [Source_Type],
            [Source_Key],
            [Rule_Version],
            [MetadataJson],
            [Created_By]
        )
        OUTPUT
            INSERTED.[TransactionID],
            INSERTED.[ChapterID],
            INSERTED.[Chapter_Code],
            INSERTED.[Points_Delta],
            INSERTED.[Source_Key]
        INTO @InsertedTransactions
        SELECT
            chapters.[ChapterID],
            chapters.[Chapter_Code],
            N'adjustment',
            chapters.[Point_Difference],
            NULL,
            NULL,
            NULL,
            @EffectiveDate,
            @EffectiveDate,
            @EffectiveDate,
            SYSUTCDATETIME(),
            @RewardRunID,
            N'bayrate_rerun_reconciliation',
            CONCAT(N'bayrate-run:', @BayrateRunID, N':chapter:', chapters.[ChapterID]),
            @RuleVersion,
            (
                SELECT
                    @BayrateRunID AS [bayrate_run_id],
                    chapters.[ChapterID] AS [chapter_id],
                    chapters.[Chapter_Code] AS [chapter_code],
                    chapters.[Chapter_Name] AS [chapter_name],
                    chapters.[Old_Played_Game_Points] AS [old_played_game_points],
                    chapters.[New_Played_Game_Points] AS [new_played_game_points],
                    chapters.[Old_Total_Games_Points] AS [old_total_games_points],
                    chapters.[New_Total_Games_Points] AS [new_total_games_points],
                    chapters.[Old_State_Championship_Points] AS [old_state_championship_points],
                    chapters.[New_State_Championship_Points] AS [new_state_championship_points],
                    chapters.[Old_Total_Points] AS [old_total_points],
                    chapters.[New_Total_Points] AS [new_total_points],
                    chapters.[Point_Difference] AS [point_difference],
                    @AppliedByPrincipalName AS [applied_by_principal_name],
                    @AppliedByPrincipalId AS [applied_by_principal_id]
                FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
            ),
            LEFT(@AppliedByPrincipalName, 128)
        FROM #ReconciliationChapters AS chapters
        WHERE chapters.[Point_Difference] <> 0;

        INSERT INTO [rewards].[point_lots]
        (
            [Earn_TransactionID],
            [ChapterID],
            [Chapter_Code],
            [Original_Points],
            [Remaining_Points],
            [Earned_Date],
            [Expires_On],
            [Source_Type],
            [Source_Key]
        )
        SELECT
            inserted.[TransactionID],
            inserted.[ChapterID],
            inserted.[Chapter_Code],
            inserted.[Points_Delta],
            inserted.[Points_Delta],
            @EffectiveDate,
            DATEADD(year, 2, @EffectiveDate),
            N'bayrate_rerun_reconciliation',
            inserted.[Source_Key]
        FROM @InsertedTransactions AS inserted
        WHERE inserted.[Points_Delta] > 0;

        DECLARE @AvailableLots table
        (
            [LotID] bigint NOT NULL PRIMARY KEY,
            [ChapterID] int NOT NULL,
            [Earned_Date] date NOT NULL,
            [Expires_On] date NOT NULL,
            [Current_Remaining_Points] int NOT NULL
        );

        INSERT INTO @AvailableLots
        (
            [LotID],
            [ChapterID],
            [Earned_Date],
            [Expires_On],
            [Current_Remaining_Points]
        )
        SELECT
            lots.[LotID],
            lots.[ChapterID],
            lots.[Earned_Date],
            lots.[Expires_On],
            lots.[Remaining_Points]
        FROM [rewards].[point_lots] AS lots WITH (UPDLOCK, HOLDLOCK)
        WHERE lots.[Remaining_Points] > 0
          AND lots.[Expires_On] >= @Today
          AND EXISTS
          (
              SELECT 1
              FROM @InsertedTransactions AS inserted
              WHERE inserted.[ChapterID] = lots.[ChapterID]
                AND inserted.[Points_Delta] < 0
          );

        DECLARE @Allocations table
        (
            [Debit_TransactionID] bigint NOT NULL,
            [LotID] bigint NOT NULL,
            [Points_Allocated] int NOT NULL
        );

        DECLARE
            @DebitTransactionID bigint,
            @DebitChapterID int,
            @DebitRemaining int,
            @LotID bigint,
            @LotRemaining int,
            @Allocated int;

        DECLARE debit_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT
                inserted.[TransactionID],
                inserted.[ChapterID],
                -inserted.[Points_Delta]
            FROM @InsertedTransactions AS inserted
            WHERE inserted.[Points_Delta] < 0
            ORDER BY inserted.[ChapterID], inserted.[TransactionID];

        OPEN debit_cursor;
        FETCH NEXT FROM debit_cursor INTO @DebitTransactionID, @DebitChapterID, @DebitRemaining;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            WHILE @DebitRemaining > 0
            BEGIN
                SET @LotID = NULL;
                SET @LotRemaining = NULL;

                SELECT TOP 1
                    @LotID = available.[LotID],
                    @LotRemaining = available.[Current_Remaining_Points]
                FROM @AvailableLots AS available
                WHERE available.[ChapterID] = @DebitChapterID
                  AND available.[Current_Remaining_Points] > 0
                ORDER BY available.[Expires_On], available.[Earned_Date], available.[LotID];

                IF @LotID IS NULL
                BEGIN
                    CLOSE debit_cursor;
                    DEALLOCATE debit_cursor;
                    THROW 52906, N'Could not allocate a reconciliation debit against available point lots.', 1;
                END;

                SET @Allocated = CASE WHEN @LotRemaining >= @DebitRemaining THEN @DebitRemaining ELSE @LotRemaining END;

                INSERT INTO @Allocations ([Debit_TransactionID], [LotID], [Points_Allocated])
                VALUES (@DebitTransactionID, @LotID, @Allocated);

                UPDATE @AvailableLots
                SET [Current_Remaining_Points] = [Current_Remaining_Points] - @Allocated
                WHERE [LotID] = @LotID;

                SET @DebitRemaining = @DebitRemaining - @Allocated;
            END;

            FETCH NEXT FROM debit_cursor INTO @DebitTransactionID, @DebitChapterID, @DebitRemaining;
        END;

        CLOSE debit_cursor;
        DEALLOCATE debit_cursor;

        INSERT INTO [rewards].[lot_allocations]
        (
            [Debit_TransactionID],
            [LotID],
            [Points_Allocated]
        )
        SELECT
            allocations.[Debit_TransactionID],
            allocations.[LotID],
            SUM(allocations.[Points_Allocated])
        FROM @Allocations AS allocations
        GROUP BY allocations.[Debit_TransactionID], allocations.[LotID];

        ;WITH [lot_usage] AS
        (
            SELECT
                allocations.[LotID],
                SUM(allocations.[Points_Allocated]) AS [Points_Allocated]
            FROM @Allocations AS allocations
            GROUP BY allocations.[LotID]
        )
        UPDATE lots
        SET [Remaining_Points] = lots.[Remaining_Points] - usage.[Points_Allocated]
        FROM [rewards].[point_lots] AS lots
        INNER JOIN [lot_usage] AS usage
            ON usage.[LotID] = lots.[LotID];

        SET @ApplicationStatus = CASE WHEN @TransactionCount = 0 THEN N'no_changes' ELSE N'applied' END;

        SET @SummaryJson =
        (
            SELECT
                N'bayrate_rerun_reconciliation' AS [processor],
                @BayrateRunID AS [bayrate_run_id],
                @RewardRunID AS [reward_run_id],
                @ApplicationStatus AS [application_status],
                @EffectiveDate AS [effective_date],
                @TransactionCount AS [transaction_count],
                @CreditPoints AS [credit_points],
                @DebitPoints AS [debit_points],
                @NetPoints AS [net_points],
                @RuleVersion AS [rule_version],
                JSON_QUERY(@ChaptersJson) AS [chapters]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        );

        INSERT INTO [rewards].[bayrate_reconciliation_applications]
        (
            [Bayrate_RunID],
            [Reward_RunID],
            [Application_Status],
            [Effective_Date],
            [Applied_At],
            [Applied_By],
            [Applied_Principal_Id],
            [Transaction_Count],
            [Credit_Points],
            [Debit_Points],
            [Net_Points],
            [SummaryJson]
        )
        VALUES
        (
            @BayrateRunID,
            @RewardRunID,
            @ApplicationStatus,
            @EffectiveDate,
            SYSUTCDATETIME(),
            @AppliedByPrincipalName,
            @AppliedByPrincipalId,
            @TransactionCount,
            @CreditPoints,
            @DebitPoints,
            @NetPoints,
            @SummaryJson
        );

        UPDATE [rewards].[reward_runs]
        SET
            [Completed_At] = SYSUTCDATETIME(),
            [Status] = N'succeeded',
            [SummaryJson] = @SummaryJson
        WHERE [RunID] = @RewardRunID;

        COMMIT TRANSACTION;

        SELECT
            @BayrateRunID AS [Bayrate_RunID],
            @RewardRunID AS [Reward_RunID],
            CAST(0 AS bit) AS [Dry_Run],
            CAST(0 AS bit) AS [Already_Applied],
            CAST(0 AS bit) AS [Can_Apply],
            @ApplicationStatus AS [Application_Status],
            @EffectiveDate AS [Effective_Date],
            application.[Applied_At],
            application.[Applied_By],
            @TransactionCount AS [Transaction_Count],
            @CreditPoints AS [Credit_Points],
            @DebitPoints AS [Debit_Points],
            @NetPoints AS [Net_Points],
            0 AS [Insufficient_Chapter_Count],
            0 AS [Shortfall_Points],
            JSON_QUERY(@ChaptersJson) AS [ChaptersJson]
        FROM [rewards].[bayrate_reconciliation_applications] AS application
        WHERE application.[Bayrate_RunID] = @BayrateRunID;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO
