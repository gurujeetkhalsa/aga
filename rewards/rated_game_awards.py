import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Protocol, TextIO

from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string
from rewards.snapshot_generator import MAX_MEMBER_AGAID, parse_snapshot_date


SOURCE_TYPE = "rated_game_participation"
RULE_VERSION = "2026-05-02"
BASE_POINTS = 500


class RatedGameAwardSqlAdapter(Protocol):
    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        ...

    def execute_statements(self, statements: Iterable[SqlStatement]) -> None:
        ...


@dataclass(frozen=True)
class RatedGameAwardResult:
    date_from: date
    date_to: date
    dry_run: bool
    run_id: int | None
    participant_count: int
    eligible_award_count: int
    already_awarded_count: int
    new_award_count: int
    point_total: int
    missing_member_snapshot_count: int
    missing_chapter_snapshot_count: int
    inactive_player_count: int
    no_chapter_count: int
    chapter_not_current_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "date_from": self.date_from.isoformat(),
            "date_to": self.date_to.isoformat(),
            "dry_run": self.dry_run,
            "run_id": self.run_id,
            "participant_count": self.participant_count,
            "eligible_award_count": self.eligible_award_count,
            "already_awarded_count": self.already_awarded_count,
            "new_award_count": self.new_award_count,
            "point_total": self.point_total,
            "missing_member_snapshot_count": self.missing_member_snapshot_count,
            "missing_chapter_snapshot_count": self.missing_chapter_snapshot_count,
            "inactive_player_count": self.inactive_player_count,
            "no_chapter_count": self.no_chapter_count,
            "chapter_not_current_count": self.chapter_not_current_count,
        }


RATED_GAME_AWARD_COUNTS_SELECT = """
SELECT
    COUNT(*) AS [ParticipantCount],
    SUM(CASE WHEN [Eligibility_Status] = N'eligible' THEN 1 ELSE 0 END) AS [EligibleAwardCount],
    SUM(CASE WHEN [Eligibility_Status] = N'eligible' AND [Already_TransactionID] IS NOT NULL THEN 1 ELSE 0 END) AS [AlreadyAwardedCount],
    SUM(CASE WHEN [Eligibility_Status] = N'eligible' AND [Already_TransactionID] IS NULL THEN 1 ELSE 0 END) AS [NewAwardCount],
    COALESCE(SUM(CASE WHEN [Eligibility_Status] = N'eligible' AND [Already_TransactionID] IS NULL THEN [Points] ELSE 0 END), 0) AS [PointTotal],
    SUM(CASE WHEN [Eligibility_Status] = N'missing_member_snapshot' THEN 1 ELSE 0 END) AS [MissingMemberSnapshotCount],
    SUM(CASE WHEN [Eligibility_Status] = N'missing_chapter_snapshot' THEN 1 ELSE 0 END) AS [MissingChapterSnapshotCount],
    SUM(CASE WHEN [Eligibility_Status] = N'inactive_player' THEN 1 ELSE 0 END) AS [InactivePlayerCount],
    SUM(CASE WHEN [Eligibility_Status] = N'no_chapter' THEN 1 ELSE 0 END) AS [NoChapterCount],
    SUM(CASE WHEN [Eligibility_Status] = N'chapter_not_current' THEN 1 ELSE 0 END) AS [ChapterNotCurrentCount]
FROM #RatedGameAwards
"""


BUILD_RATED_GAME_AWARDS_TEMP_TABLE_SQL = """
DECLARE @GameDateFrom date = ?;
DECLARE @GameDateTo date = ?;
DECLARE @MaxMemberAGAID int = ?;
DECLARE @BasePoints int = ?;
DECLARE @SourceType nvarchar(64) = ?;

IF OBJECT_ID(N'tempdb..#RatedGameAwards', N'U') IS NOT NULL
BEGIN
    DROP TABLE #RatedGameAwards;
END;

;WITH [game_participants] AS
(
    SELECT
        g.[Game_ID],
        g.[Tournament_Code],
        CAST(g.[Game_Date] AS date) AS [Game_Date],
        g.[Pin_Player_1] AS [AGAID],
        g.[Pin_Player_2] AS [Opponent_AGAID],
        g.[Color_1] AS [Color],
        1 AS [Player_Slot]
    FROM [ratings].[games] AS g
    WHERE g.[Game_ID] IS NOT NULL
      AND g.[Game_Date] IS NOT NULL
      AND CAST(g.[Game_Date] AS date) >= @GameDateFrom
      AND CAST(g.[Game_Date] AS date) <= @GameDateTo
      AND COALESCE(g.[Rated], 1) = 1
      AND COALESCE(g.[Online], 0) = 0
      AND COALESCE(g.[Exclude], 0) = 0
      AND g.[Pin_Player_1] IS NOT NULL
      AND g.[Pin_Player_1] < @MaxMemberAGAID
    UNION ALL
    SELECT
        g.[Game_ID],
        g.[Tournament_Code],
        CAST(g.[Game_Date] AS date) AS [Game_Date],
        g.[Pin_Player_2] AS [AGAID],
        g.[Pin_Player_1] AS [Opponent_AGAID],
        g.[Color_2] AS [Color],
        2 AS [Player_Slot]
    FROM [ratings].[games] AS g
    WHERE g.[Game_ID] IS NOT NULL
      AND g.[Game_Date] IS NOT NULL
      AND CAST(g.[Game_Date] AS date) >= @GameDateFrom
      AND CAST(g.[Game_Date] AS date) <= @GameDateTo
      AND COALESCE(g.[Rated], 1) = 1
      AND COALESCE(g.[Online], 0) = 0
      AND COALESCE(g.[Exclude], 0) = 0
      AND g.[Pin_Player_2] IS NOT NULL
      AND g.[Pin_Player_2] < @MaxMemberAGAID
),
[award_candidates] AS
(
    SELECT
        gp.[Game_ID],
        gp.[Tournament_Code],
        gp.[Game_Date],
        gp.[AGAID],
        gp.[Opponent_AGAID],
        gp.[Color],
        gp.[Player_Slot],
        ms.[Member_Type],
        ms.[ChapterID],
        ms.[Chapter_Code],
        ms.[Is_Active],
        ms.[Is_Tournament_Pass],
        cs.[Is_Current],
        cs.[Active_Member_Count],
        cs.[Multiplier],
        CASE
            WHEN ms.[AGAID] IS NULL THEN N'missing_member_snapshot'
            WHEN ms.[Is_Active] <> 1 THEN N'inactive_player'
            WHEN ms.[ChapterID] IS NULL THEN N'no_chapter'
            WHEN cs.[ChapterID] IS NULL THEN N'missing_chapter_snapshot'
            WHEN cs.[Is_Current] <> 1 THEN N'chapter_not_current'
            ELSE N'eligible'
        END AS [Eligibility_Status],
        CONCAT(@SourceType, N':', CONVERT(nvarchar(32), gp.[Game_ID]), N':', CONVERT(nvarchar(32), gp.[AGAID])) AS [Source_Key]
    FROM [game_participants] AS gp
    LEFT JOIN [rewards].[member_daily_snapshot] AS ms
        ON ms.[Snapshot_Date] = gp.[Game_Date]
       AND ms.[AGAID] = gp.[AGAID]
    LEFT JOIN [rewards].[chapter_daily_snapshot] AS cs
        ON cs.[Snapshot_Date] = gp.[Game_Date]
       AND cs.[ChapterID] = ms.[ChapterID]
)
SELECT
    candidates.[Game_ID],
    candidates.[Tournament_Code],
    candidates.[Game_Date],
    candidates.[AGAID],
    candidates.[Opponent_AGAID],
    candidates.[Color],
    candidates.[Player_Slot],
    candidates.[Member_Type],
    candidates.[ChapterID],
    candidates.[Chapter_Code],
    candidates.[Is_Active],
    candidates.[Is_Tournament_Pass],
    candidates.[Is_Current],
    candidates.[Active_Member_Count],
    candidates.[Multiplier],
    candidates.[Eligibility_Status],
    candidates.[Source_Key],
    existing.[TransactionID] AS [Already_TransactionID],
    CASE
        WHEN candidates.[Eligibility_Status] = N'eligible'
            THEN @BasePoints * candidates.[Multiplier]
        ELSE 0
    END AS [Points]
INTO #RatedGameAwards
FROM [award_candidates] AS candidates
OUTER APPLY
(
    SELECT TOP 1
        t.[TransactionID]
    FROM [rewards].[transactions] AS t
    WHERE t.[Source_Type] = @SourceType
      AND t.[Source_Key] = candidates.[Source_Key]
      AND t.[Transaction_Type] = N'earn'
    ORDER BY t.[TransactionID]
) AS existing;
"""


RATED_GAME_AWARD_PREVIEW_SQL = """
DECLARE @GameDateFrom date = ?;
DECLARE @GameDateTo date = ?;
DECLARE @MaxMemberAGAID int = ?;
DECLARE @BasePoints int = ?;
DECLARE @SourceType nvarchar(64) = ?;

;WITH [game_participants] AS
(
    SELECT
        g.[Game_ID],
        g.[Tournament_Code],
        CAST(g.[Game_Date] AS date) AS [Game_Date],
        g.[Pin_Player_1] AS [AGAID],
        g.[Pin_Player_2] AS [Opponent_AGAID],
        g.[Color_1] AS [Color],
        1 AS [Player_Slot]
    FROM [ratings].[games] AS g
    WHERE g.[Game_ID] IS NOT NULL
      AND g.[Game_Date] IS NOT NULL
      AND CAST(g.[Game_Date] AS date) >= @GameDateFrom
      AND CAST(g.[Game_Date] AS date) <= @GameDateTo
      AND COALESCE(g.[Rated], 1) = 1
      AND COALESCE(g.[Online], 0) = 0
      AND COALESCE(g.[Exclude], 0) = 0
      AND g.[Pin_Player_1] IS NOT NULL
      AND g.[Pin_Player_1] < @MaxMemberAGAID
    UNION ALL
    SELECT
        g.[Game_ID],
        g.[Tournament_Code],
        CAST(g.[Game_Date] AS date) AS [Game_Date],
        g.[Pin_Player_2] AS [AGAID],
        g.[Pin_Player_1] AS [Opponent_AGAID],
        g.[Color_2] AS [Color],
        2 AS [Player_Slot]
    FROM [ratings].[games] AS g
    WHERE g.[Game_ID] IS NOT NULL
      AND g.[Game_Date] IS NOT NULL
      AND CAST(g.[Game_Date] AS date) >= @GameDateFrom
      AND CAST(g.[Game_Date] AS date) <= @GameDateTo
      AND COALESCE(g.[Rated], 1) = 1
      AND COALESCE(g.[Online], 0) = 0
      AND COALESCE(g.[Exclude], 0) = 0
      AND g.[Pin_Player_2] IS NOT NULL
      AND g.[Pin_Player_2] < @MaxMemberAGAID
),
[award_candidates] AS
(
    SELECT
        gp.[Game_ID],
        gp.[Game_Date],
        gp.[AGAID],
        ms.[ChapterID],
        ms.[Is_Active],
        cs.[ChapterID] AS [Snapshot_ChapterID],
        cs.[Is_Current],
        cs.[Multiplier],
        CASE
            WHEN ms.[AGAID] IS NULL THEN N'missing_member_snapshot'
            WHEN ms.[Is_Active] <> 1 THEN N'inactive_player'
            WHEN ms.[ChapterID] IS NULL THEN N'no_chapter'
            WHEN cs.[ChapterID] IS NULL THEN N'missing_chapter_snapshot'
            WHEN cs.[Is_Current] <> 1 THEN N'chapter_not_current'
            ELSE N'eligible'
        END AS [Eligibility_Status],
        CONCAT(@SourceType, N':', CONVERT(nvarchar(32), gp.[Game_ID]), N':', CONVERT(nvarchar(32), gp.[AGAID])) AS [Source_Key]
    FROM [game_participants] AS gp
    LEFT JOIN [rewards].[member_daily_snapshot] AS ms
        ON ms.[Snapshot_Date] = gp.[Game_Date]
       AND ms.[AGAID] = gp.[AGAID]
    LEFT JOIN [rewards].[chapter_daily_snapshot] AS cs
        ON cs.[Snapshot_Date] = gp.[Game_Date]
       AND cs.[ChapterID] = ms.[ChapterID]
),
[awards] AS
(
    SELECT
        candidates.[Eligibility_Status],
        existing.[TransactionID] AS [Already_TransactionID],
        CASE
            WHEN candidates.[Eligibility_Status] = N'eligible'
                THEN @BasePoints * candidates.[Multiplier]
            ELSE 0
        END AS [Points]
    FROM [award_candidates] AS candidates
    OUTER APPLY
    (
        SELECT TOP 1
            t.[TransactionID]
        FROM [rewards].[transactions] AS t
        WHERE t.[Source_Type] = @SourceType
          AND t.[Source_Key] = candidates.[Source_Key]
          AND t.[Transaction_Type] = N'earn'
        ORDER BY t.[TransactionID]
    ) AS existing
)
SELECT
    COUNT(*) AS [ParticipantCount],
    SUM(CASE WHEN [Eligibility_Status] = N'eligible' THEN 1 ELSE 0 END) AS [EligibleAwardCount],
    SUM(CASE WHEN [Eligibility_Status] = N'eligible' AND [Already_TransactionID] IS NOT NULL THEN 1 ELSE 0 END) AS [AlreadyAwardedCount],
    SUM(CASE WHEN [Eligibility_Status] = N'eligible' AND [Already_TransactionID] IS NULL THEN 1 ELSE 0 END) AS [NewAwardCount],
    COALESCE(SUM(CASE WHEN [Eligibility_Status] = N'eligible' AND [Already_TransactionID] IS NULL THEN [Points] ELSE 0 END), 0) AS [PointTotal],
    SUM(CASE WHEN [Eligibility_Status] = N'missing_member_snapshot' THEN 1 ELSE 0 END) AS [MissingMemberSnapshotCount],
    SUM(CASE WHEN [Eligibility_Status] = N'missing_chapter_snapshot' THEN 1 ELSE 0 END) AS [MissingChapterSnapshotCount],
    SUM(CASE WHEN [Eligibility_Status] = N'inactive_player' THEN 1 ELSE 0 END) AS [InactivePlayerCount],
    SUM(CASE WHEN [Eligibility_Status] = N'no_chapter' THEN 1 ELSE 0 END) AS [NoChapterCount],
    SUM(CASE WHEN [Eligibility_Status] = N'chapter_not_current' THEN 1 ELSE 0 END) AS [ChapterNotCurrentCount]
FROM [awards]
"""


CREATE_RATED_GAME_AWARDS_SQL = BUILD_RATED_GAME_AWARDS_TEMP_TABLE_SQL + """
DECLARE @RunType nvarchar(32) = ?;
DECLARE @RuleVersion nvarchar(32) = ?;
DECLARE @RunSnapshotDate date = @GameDateTo;

DECLARE @ParticipantCount int;
DECLARE @EligibleAwardCount int;
DECLARE @AlreadyAwardedCount int;
DECLARE @NewAwardCount int;
DECLARE @PointTotal int;
DECLARE @MissingMemberSnapshotCount int;
DECLARE @MissingChapterSnapshotCount int;
DECLARE @InactivePlayerCount int;
DECLARE @NoChapterCount int;
DECLARE @ChapterNotCurrentCount int;

SELECT
    @ParticipantCount = COUNT(*),
    @EligibleAwardCount = SUM(CASE WHEN [Eligibility_Status] = N'eligible' THEN 1 ELSE 0 END),
    @AlreadyAwardedCount = SUM(CASE WHEN [Eligibility_Status] = N'eligible' AND [Already_TransactionID] IS NOT NULL THEN 1 ELSE 0 END),
    @NewAwardCount = SUM(CASE WHEN [Eligibility_Status] = N'eligible' AND [Already_TransactionID] IS NULL THEN 1 ELSE 0 END),
    @PointTotal = COALESCE(SUM(CASE WHEN [Eligibility_Status] = N'eligible' AND [Already_TransactionID] IS NULL THEN [Points] ELSE 0 END), 0),
    @MissingMemberSnapshotCount = SUM(CASE WHEN [Eligibility_Status] = N'missing_member_snapshot' THEN 1 ELSE 0 END),
    @MissingChapterSnapshotCount = SUM(CASE WHEN [Eligibility_Status] = N'missing_chapter_snapshot' THEN 1 ELSE 0 END),
    @InactivePlayerCount = SUM(CASE WHEN [Eligibility_Status] = N'inactive_player' THEN 1 ELSE 0 END),
    @NoChapterCount = SUM(CASE WHEN [Eligibility_Status] = N'no_chapter' THEN 1 ELSE 0 END),
    @ChapterNotCurrentCount = SUM(CASE WHEN [Eligibility_Status] = N'chapter_not_current' THEN 1 ELSE 0 END)
FROM #RatedGameAwards;

DECLARE @InsertedRun table ([RunID] int NOT NULL);

INSERT INTO [rewards].[reward_runs]
(
    [Run_Type],
    [Snapshot_Date],
    [Started_At],
    [Status],
    [SummaryJson]
)
OUTPUT INSERTED.[RunID] INTO @InsertedRun ([RunID])
VALUES
(
    @RunType,
    @RunSnapshotDate,
    SYSUTCDATETIME(),
    N'running',
    NULL
);

DECLARE @RunID int = (SELECT TOP 1 [RunID] FROM @InsertedRun);

DECLARE @InsertedTransactions table
(
    [TransactionID] bigint NOT NULL,
    [ChapterID] int NOT NULL,
    [Chapter_Code] nvarchar(64) NOT NULL,
    [Points_Delta] int NOT NULL,
    [Earned_Date] date NOT NULL,
    [Source_Type] nvarchar(64) NOT NULL,
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
    [MetadataJson]
)
OUTPUT
    INSERTED.[TransactionID],
    INSERTED.[ChapterID],
    INSERTED.[Chapter_Code],
    INSERTED.[Points_Delta],
    INSERTED.[Earned_Date],
    INSERTED.[Source_Type],
    INSERTED.[Source_Key]
INTO @InsertedTransactions
(
    [TransactionID],
    [ChapterID],
    [Chapter_Code],
    [Points_Delta],
    [Earned_Date],
    [Source_Type],
    [Source_Key]
)
SELECT
    awards.[ChapterID],
    awards.[Chapter_Code],
    N'earn',
    awards.[Points],
    @BasePoints,
    awards.[Multiplier],
    awards.[Active_Member_Count],
    awards.[Game_Date],
    awards.[Game_Date],
    awards.[Game_Date],
    SYSUTCDATETIME(),
    @RunID,
    @SourceType,
    awards.[Source_Key],
    @RuleVersion,
    (
        SELECT
            awards.[Game_ID] AS [game_id],
            awards.[Tournament_Code] AS [tournament_code],
            awards.[Game_Date] AS [game_date],
            awards.[AGAID] AS [agaid],
            awards.[Opponent_AGAID] AS [opponent_agaid],
            awards.[Color] AS [color],
            awards.[Player_Slot] AS [player_slot],
            awards.[Member_Type] AS [member_type],
            awards.[ChapterID] AS [chapter_id],
            awards.[Chapter_Code] AS [chapter_code],
            awards.[Active_Member_Count] AS [chapter_active_member_count],
            awards.[Multiplier] AS [multiplier],
            @BasePoints AS [base_points],
            awards.[Points] AS [points]
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
FROM #RatedGameAwards AS awards
WHERE awards.[Eligibility_Status] = N'eligible'
  AND awards.[Already_TransactionID] IS NULL;

DECLARE @InsertedAwardCount int = @@ROWCOUNT;

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
    tx.[TransactionID],
    tx.[ChapterID],
    tx.[Chapter_Code],
    tx.[Points_Delta],
    tx.[Points_Delta],
    tx.[Earned_Date],
    DATEADD(year, 2, tx.[Earned_Date]),
    tx.[Source_Type],
    tx.[Source_Key]
FROM @InsertedTransactions AS tx;

UPDATE [rewards].[reward_runs]
SET
    [Completed_At] = SYSUTCDATETIME(),
    [Status] = N'succeeded',
    [SummaryJson] =
    (
        SELECT
            @SourceType AS [processor],
            @ParticipantCount AS [participant_count],
            @EligibleAwardCount AS [eligible_award_count],
            @AlreadyAwardedCount AS [already_awarded_count],
            @InsertedAwardCount AS [new_award_count],
            @PointTotal AS [point_total],
            @MissingMemberSnapshotCount AS [missing_member_snapshot_count],
            @MissingChapterSnapshotCount AS [missing_chapter_snapshot_count],
            @InactivePlayerCount AS [inactive_player_count],
            @NoChapterCount AS [no_chapter_count],
            @ChapterNotCurrentCount AS [chapter_not_current_count]
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
WHERE [RunID] = @RunID;
"""


RATED_GAME_AWARD_RESULT_SQL = """
SELECT TOP 1
    [RunID],
    [SummaryJson]
FROM [rewards].[reward_runs]
WHERE [RunID] =
(
    SELECT MAX([RunID])
    FROM [rewards].[reward_runs]
    WHERE [Run_Type] = ?
      AND [Snapshot_Date] = ?
)
"""


def process_rated_game_awards(
    adapter: RatedGameAwardSqlAdapter,
    date_from: date,
    date_to: date,
    *,
    run_type: str = "manual",
    dry_run: bool = False,
    max_member_agaid: int = MAX_MEMBER_AGAID,
    base_points: int = BASE_POINTS,
    source_type: str = SOURCE_TYPE,
    rule_version: str = RULE_VERSION,
) -> RatedGameAwardResult:
    if date_to < date_from:
        raise ValueError("date_to must be on or after date_from.")

    if dry_run:
        rows = adapter.query_rows(
            RATED_GAME_AWARD_PREVIEW_SQL,
            (date_from, date_to, max_member_agaid, base_points, source_type),
        )
        counts = rows[0] if rows else {}
        return _result_from_counts(
            date_from=date_from,
            date_to=date_to,
            dry_run=True,
            run_id=None,
            counts=counts,
        )

    adapter.execute_statements(
        [
            (
                CREATE_RATED_GAME_AWARDS_SQL,
                (
                    date_from,
                    date_to,
                    max_member_agaid,
                    base_points,
                    source_type,
                    run_type,
                    rule_version,
                ),
            )
        ]
    )
    rows = adapter.query_rows(RATED_GAME_AWARD_RESULT_SQL, (run_type, date_to))
    if not rows:
        raise RuntimeError("Rated game award run did not produce a reward_runs row.")
    summary = json.loads(rows[0].get("SummaryJson") or "{}")
    return _result_from_counts(
        date_from=date_from,
        date_to=date_to,
        dry_run=False,
        run_id=_coerce_optional_int(rows[0].get("RunID")),
        counts={
            "ParticipantCount": summary.get("participant_count"),
            "EligibleAwardCount": summary.get("eligible_award_count"),
            "AlreadyAwardedCount": summary.get("already_awarded_count"),
            "NewAwardCount": summary.get("new_award_count"),
            "PointTotal": summary.get("point_total"),
            "MissingMemberSnapshotCount": summary.get("missing_member_snapshot_count"),
            "MissingChapterSnapshotCount": summary.get("missing_chapter_snapshot_count"),
            "InactivePlayerCount": summary.get("inactive_player_count"),
            "NoChapterCount": summary.get("no_chapter_count"),
            "ChapterNotCurrentCount": summary.get("chapter_not_current_count"),
        },
    )


def _result_from_counts(
    *,
    date_from: date,
    date_to: date,
    dry_run: bool,
    run_id: int | None,
    counts: dict[str, Any],
) -> RatedGameAwardResult:
    return RatedGameAwardResult(
        date_from=date_from,
        date_to=date_to,
        dry_run=dry_run,
        run_id=run_id,
        participant_count=_coerce_int(counts.get("ParticipantCount")),
        eligible_award_count=_coerce_int(counts.get("EligibleAwardCount")),
        already_awarded_count=_coerce_int(counts.get("AlreadyAwardedCount")),
        new_award_count=_coerce_int(counts.get("NewAwardCount")),
        point_total=_coerce_int(counts.get("PointTotal")),
        missing_member_snapshot_count=_coerce_int(counts.get("MissingMemberSnapshotCount")),
        missing_chapter_snapshot_count=_coerce_int(counts.get("MissingChapterSnapshotCount")),
        inactive_player_count=_coerce_int(counts.get("InactivePlayerCount")),
        no_chapter_count=_coerce_int(counts.get("NoChapterCount")),
        chapter_not_current_count=_coerce_int(counts.get("ChapterNotCurrentCount")),
    )


def _coerce_int(value: Any) -> int:
    if value is None:
        return 0
    return int(value)


def _coerce_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def print_award_result(result: RatedGameAwardResult, output: TextIO) -> None:
    label = "Rated Game Awards Preview" if result.dry_run else "Rated Game Awards"
    print(label, file=output)
    print(f"  Dates: {result.date_from.isoformat()} to {result.date_to.isoformat()}", file=output)
    if result.run_id is not None:
        print(f"  RunID: {result.run_id}", file=output)
    print(f"  Participant rows: {result.participant_count}", file=output)
    print(f"  Eligible awards: {result.eligible_award_count}", file=output)
    print(f"  Already awarded: {result.already_awarded_count}", file=output)
    print(f"  New awards: {result.new_award_count}", file=output)
    print(f"  New points: {result.point_total}", file=output)
    print("  Ineligible or blocked:", file=output)
    print(f"    missing member snapshot: {result.missing_member_snapshot_count}", file=output)
    print(f"    missing chapter snapshot: {result.missing_chapter_snapshot_count}", file=output)
    print(f"    inactive player: {result.inactive_player_count}", file=output)
    print(f"    no chapter: {result.no_chapter_count}", file=output)
    print(f"    chapter not current: {result.chapter_not_current_count}", file=output)


def main(argv: list[str] | None = None, output: TextIO = sys.stdout) -> int:
    parser = argparse.ArgumentParser(description="Award AGA Chapter Rewards points for eligible rated games.")
    parser.add_argument("--date", type=parse_snapshot_date, help="Single game date in YYYY-MM-DD format.")
    parser.add_argument("--date-from", type=parse_snapshot_date, help="Start game date in YYYY-MM-DD format.")
    parser.add_argument("--date-to", type=parse_snapshot_date, help="End game date in YYYY-MM-DD format.")
    parser.add_argument("--dry-run", action="store_true", help="Preview awards without writing transactions or point lots.")
    parser.add_argument("--run-type", default="manual", choices=["daily", "manual", "import", "backfill"], help="reward_runs.Run_Type value.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    args = parser.parse_args(argv)

    if args.date and (args.date_from or args.date_to):
        parser.error("Use either --date or --date-from/--date-to, not both.")
    if args.date:
        date_from = args.date
        date_to = args.date
    else:
        date_from = args.date_from or date.today()
        date_to = args.date_to or date_from

    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2

    result = process_rated_game_awards(
        SqlAdapter(conn_str),
        date_from,
        date_to,
        run_type=args.run_type,
        dry_run=args.dry_run,
    )
    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True), file=output)
    else:
        print_award_result(result, output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
