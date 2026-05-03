import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Protocol, TextIO

from bayrate.sql_adapter import SqlAdapter, SqlStatement, get_sql_connection_string


MAX_MEMBER_AGAID = 50000


class SnapshotSqlAdapter(Protocol):
    def query_rows(self, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        ...

    def execute_statements(self, statements: Iterable[SqlStatement]) -> None:
        ...


@dataclass(frozen=True)
class SnapshotResult:
    snapshot_date: date
    dry_run: bool
    run_id: int | None
    member_snapshot_count: int
    active_member_count: int
    tournament_pass_count: int
    chapter_snapshot_count: int
    current_chapter_count: int
    multiplier_1_chapter_count: int
    multiplier_2_chapter_count: int
    multiplier_3_chapter_count: int
    existing_member_snapshot_count: int = 0
    existing_chapter_snapshot_count: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "snapshot_date": self.snapshot_date.isoformat(),
            "dry_run": self.dry_run,
            "run_id": self.run_id,
            "member_snapshot_count": self.member_snapshot_count,
            "active_member_count": self.active_member_count,
            "tournament_pass_count": self.tournament_pass_count,
            "chapter_snapshot_count": self.chapter_snapshot_count,
            "current_chapter_count": self.current_chapter_count,
            "multiplier_1_chapter_count": self.multiplier_1_chapter_count,
            "multiplier_2_chapter_count": self.multiplier_2_chapter_count,
            "multiplier_3_chapter_count": self.multiplier_3_chapter_count,
            "existing_member_snapshot_count": self.existing_member_snapshot_count,
            "existing_chapter_snapshot_count": self.existing_chapter_snapshot_count,
        }


SNAPSHOT_EXISTING_COUNTS_SQL = """
SELECT
    (SELECT COUNT(*) FROM [rewards].[member_daily_snapshot] WHERE [Snapshot_Date] = ?) AS [ExistingMemberSnapshotCount],
    (SELECT COUNT(*) FROM [rewards].[chapter_daily_snapshot] WHERE [Snapshot_Date] = ?) AS [ExistingChapterSnapshotCount]
"""


SNAPSHOT_PREVIEW_SQL = """
DECLARE @SnapshotDate date = ?;
DECLARE @MaxMemberAGAID int = ?;

;WITH [member_source] AS
(
    SELECT
        m.[AGAID],
        m.[MemberType],
        m.[ExpirationDate],
        m.[ChapterID]
    FROM [membership].[members] AS m
    WHERE m.[AGAID] < @MaxMemberAGAID
),
[member_facts] AS
(
    SELECT
        [AGAID],
        [MemberType],
        [ChapterID],
        CASE
            WHEN [ExpirationDate] IS NOT NULL
             AND CAST([ExpirationDate] AS date) >= @SnapshotDate
                THEN 1
            ELSE 0
        END AS [Is_Active],
        CASE
            WHEN LTRIM(RTRIM(COALESCE([MemberType], N''))) = N'Tournament Pass'
                THEN 1
            ELSE 0
        END AS [Is_Tournament_Pass]
    FROM [member_source]
),
[chapter_counts] AS
(
    SELECT
        [ChapterID],
        COUNT(*) AS [Active_Member_Count]
    FROM [member_facts]
    WHERE [ChapterID] IS NOT NULL
      AND [Is_Active] = 1
      AND [Is_Tournament_Pass] = 0
    GROUP BY [ChapterID]
),
[chapter_facts] AS
(
    SELECT
        c.[ChapterID],
        COALESCE(counts.[Active_Member_Count], 0) AS [Active_Member_Count],
        CASE
            WHEN COALESCE(counts.[Active_Member_Count], 0) < 5 THEN 3
            WHEN COALESCE(counts.[Active_Member_Count], 0) BETWEEN 5 AND 9 THEN 2
            ELSE 1
        END AS [Multiplier],
        COALESCE(eligibility.[Is_Current], 1) AS [Is_Current]
    FROM [membership].[chapters] AS c
    LEFT JOIN [chapter_counts] AS counts
        ON counts.[ChapterID] = c.[ChapterID]
    OUTER APPLY
    (
        SELECT TOP 1
            periods.[Is_Current]
        FROM [rewards].[chapter_eligibility_periods] AS periods
        WHERE periods.[Effective_Start_Date] <= @SnapshotDate
          AND (periods.[Effective_End_Date] IS NULL OR periods.[Effective_End_Date] >= @SnapshotDate)
          AND
          (
              (periods.[ChapterID] IS NOT NULL AND periods.[ChapterID] = c.[ChapterID])
              OR (periods.[ChapterID] IS NULL AND periods.[Chapter_Code] = c.[ChapterCode])
          )
        ORDER BY
            CASE WHEN periods.[ChapterID] = c.[ChapterID] THEN 0 ELSE 1 END,
            periods.[Effective_Start_Date] DESC,
            periods.[Chapter_Eligibility_Period_ID] DESC
    ) AS eligibility
    WHERE c.[ChapterID] IS NOT NULL
      AND LTRIM(RTRIM(COALESCE(c.[ChapterCode], N''))) <> N''
)
SELECT
    (SELECT COUNT(*) FROM [member_facts]) AS [MemberSnapshotCount],
    (SELECT COUNT(*) FROM [member_facts] WHERE [Is_Active] = 1) AS [ActiveMemberCount],
    (SELECT COUNT(*) FROM [member_facts] WHERE [Is_Tournament_Pass] = 1) AS [TournamentPassCount],
    (SELECT COUNT(*) FROM [chapter_facts]) AS [ChapterSnapshotCount],
    (SELECT COUNT(*) FROM [chapter_facts] WHERE [Is_Current] = 1) AS [CurrentChapterCount],
    (SELECT COUNT(*) FROM [chapter_facts] WHERE [Multiplier] = 1) AS [Multiplier1ChapterCount],
    (SELECT COUNT(*) FROM [chapter_facts] WHERE [Multiplier] = 2) AS [Multiplier2ChapterCount],
    (SELECT COUNT(*) FROM [chapter_facts] WHERE [Multiplier] = 3) AS [Multiplier3ChapterCount];
"""


CREATE_SNAPSHOT_SQL = """
DECLARE @SnapshotDate date = ?;
DECLARE @RunType nvarchar(32) = ?;
DECLARE @MaxMemberAGAID int = ?;
DECLARE @ReplaceExisting bit = ?;

IF @ReplaceExisting = 0
   AND
   (
       EXISTS (SELECT 1 FROM [rewards].[member_daily_snapshot] WHERE [Snapshot_Date] = @SnapshotDate)
       OR EXISTS (SELECT 1 FROM [rewards].[chapter_daily_snapshot] WHERE [Snapshot_Date] = @SnapshotDate)
   )
BEGIN
    THROW 52100, N'Rewards snapshots already exist for this date. Use replace mode to rebuild them.', 1;
END;

IF @ReplaceExisting = 1
BEGIN
    DELETE FROM [rewards].[chapter_daily_snapshot]
    WHERE [Snapshot_Date] = @SnapshotDate;

    DELETE FROM [rewards].[member_daily_snapshot]
    WHERE [Snapshot_Date] = @SnapshotDate;
END;

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
    @SnapshotDate,
    SYSUTCDATETIME(),
    N'running',
    NULL
);

DECLARE @RunID int = (SELECT TOP 1 [RunID] FROM @InsertedRun);

INSERT INTO [rewards].[member_daily_snapshot]
(
    [Snapshot_Date],
    [AGAID],
    [Member_Type],
    [Expiration_Date],
    [ChapterID],
    [Chapter_Code],
    [Is_Active],
    [Is_Tournament_Pass],
    [Created_RunID]
)
SELECT
    @SnapshotDate,
    m.[AGAID],
    m.[MemberType],
    CAST(m.[ExpirationDate] AS date),
    m.[ChapterID],
    c.[ChapterCode],
    CASE
        WHEN m.[ExpirationDate] IS NOT NULL
         AND CAST(m.[ExpirationDate] AS date) >= @SnapshotDate
            THEN 1
        ELSE 0
    END,
    CASE
        WHEN LTRIM(RTRIM(COALESCE(m.[MemberType], N''))) = N'Tournament Pass'
            THEN 1
        ELSE 0
    END,
    @RunID
FROM [membership].[members] AS m
LEFT JOIN [membership].[chapters] AS c
    ON c.[ChapterID] = m.[ChapterID]
WHERE m.[AGAID] < @MaxMemberAGAID;

DECLARE @MemberSnapshotCount int = @@ROWCOUNT;
DECLARE @ActiveMemberCount int =
(
    SELECT COUNT(*)
    FROM [rewards].[member_daily_snapshot]
    WHERE [Snapshot_Date] = @SnapshotDate
      AND [Is_Active] = 1
);
DECLARE @TournamentPassCount int =
(
    SELECT COUNT(*)
    FROM [rewards].[member_daily_snapshot]
    WHERE [Snapshot_Date] = @SnapshotDate
      AND [Is_Tournament_Pass] = 1
);

;WITH [chapter_counts] AS
(
    SELECT
        [ChapterID],
        COUNT(*) AS [Active_Member_Count]
    FROM [rewards].[member_daily_snapshot]
    WHERE [Snapshot_Date] = @SnapshotDate
      AND [ChapterID] IS NOT NULL
      AND [Is_Active] = 1
      AND [Is_Tournament_Pass] = 0
    GROUP BY [ChapterID]
)
INSERT INTO [rewards].[chapter_daily_snapshot]
(
    [Snapshot_Date],
    [ChapterID],
    [Chapter_Code],
    [Chapter_Name],
    [Is_Current],
    [Active_Member_Count],
    [Multiplier],
    [Created_RunID]
)
SELECT
    @SnapshotDate,
    c.[ChapterID],
    c.[ChapterCode],
    c.[ChapterName],
    COALESCE(eligibility.[Is_Current], 1),
    COALESCE(counts.[Active_Member_Count], 0),
    CASE
        WHEN COALESCE(counts.[Active_Member_Count], 0) < 5 THEN 3
        WHEN COALESCE(counts.[Active_Member_Count], 0) BETWEEN 5 AND 9 THEN 2
        ELSE 1
    END,
    @RunID
FROM [membership].[chapters] AS c
LEFT JOIN [chapter_counts] AS counts
    ON counts.[ChapterID] = c.[ChapterID]
OUTER APPLY
(
    SELECT TOP 1
        periods.[Is_Current]
    FROM [rewards].[chapter_eligibility_periods] AS periods
    WHERE periods.[Effective_Start_Date] <= @SnapshotDate
      AND (periods.[Effective_End_Date] IS NULL OR periods.[Effective_End_Date] >= @SnapshotDate)
      AND
      (
          (periods.[ChapterID] IS NOT NULL AND periods.[ChapterID] = c.[ChapterID])
          OR (periods.[ChapterID] IS NULL AND periods.[Chapter_Code] = c.[ChapterCode])
      )
    ORDER BY
        CASE WHEN periods.[ChapterID] = c.[ChapterID] THEN 0 ELSE 1 END,
        periods.[Effective_Start_Date] DESC,
        periods.[Chapter_Eligibility_Period_ID] DESC
) AS eligibility
WHERE c.[ChapterID] IS NOT NULL
  AND LTRIM(RTRIM(COALESCE(c.[ChapterCode], N''))) <> N'';

DECLARE @ChapterSnapshotCount int = @@ROWCOUNT;
DECLARE @CurrentChapterCount int =
(
    SELECT COUNT(*)
    FROM [rewards].[chapter_daily_snapshot]
    WHERE [Snapshot_Date] = @SnapshotDate
      AND [Is_Current] = 1
);
DECLARE @Multiplier1ChapterCount int =
(
    SELECT COUNT(*)
    FROM [rewards].[chapter_daily_snapshot]
    WHERE [Snapshot_Date] = @SnapshotDate
      AND [Multiplier] = 1
);
DECLARE @Multiplier2ChapterCount int =
(
    SELECT COUNT(*)
    FROM [rewards].[chapter_daily_snapshot]
    WHERE [Snapshot_Date] = @SnapshotDate
      AND [Multiplier] = 2
);
DECLARE @Multiplier3ChapterCount int =
(
    SELECT COUNT(*)
    FROM [rewards].[chapter_daily_snapshot]
    WHERE [Snapshot_Date] = @SnapshotDate
      AND [Multiplier] = 3
);

UPDATE [rewards].[reward_runs]
SET
    [Completed_At] = SYSUTCDATETIME(),
    [Status] = N'succeeded',
    [SummaryJson] =
    (
        SELECT
            @MemberSnapshotCount AS [member_snapshot_count],
            @ActiveMemberCount AS [active_member_count],
            @TournamentPassCount AS [tournament_pass_count],
            @ChapterSnapshotCount AS [chapter_snapshot_count],
            @CurrentChapterCount AS [current_chapter_count],
            @Multiplier1ChapterCount AS [multiplier_1_chapter_count],
            @Multiplier2ChapterCount AS [multiplier_2_chapter_count],
            @Multiplier3ChapterCount AS [multiplier_3_chapter_count]
        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    )
WHERE [RunID] = @RunID;
"""


SNAPSHOT_RESULT_SQL = """
SELECT TOP 1
    r.[RunID],
    r.[SummaryJson],
    (SELECT COUNT(*) FROM [rewards].[member_daily_snapshot] WHERE [Snapshot_Date] = ?) AS [MemberSnapshotCount],
    (SELECT COUNT(*) FROM [rewards].[member_daily_snapshot] WHERE [Snapshot_Date] = ? AND [Is_Active] = 1) AS [ActiveMemberCount],
    (SELECT COUNT(*) FROM [rewards].[member_daily_snapshot] WHERE [Snapshot_Date] = ? AND [Is_Tournament_Pass] = 1) AS [TournamentPassCount],
    (SELECT COUNT(*) FROM [rewards].[chapter_daily_snapshot] WHERE [Snapshot_Date] = ?) AS [ChapterSnapshotCount],
    (SELECT COUNT(*) FROM [rewards].[chapter_daily_snapshot] WHERE [Snapshot_Date] = ? AND [Is_Current] = 1) AS [CurrentChapterCount],
    (SELECT COUNT(*) FROM [rewards].[chapter_daily_snapshot] WHERE [Snapshot_Date] = ? AND [Multiplier] = 1) AS [Multiplier1ChapterCount],
    (SELECT COUNT(*) FROM [rewards].[chapter_daily_snapshot] WHERE [Snapshot_Date] = ? AND [Multiplier] = 2) AS [Multiplier2ChapterCount],
    (SELECT COUNT(*) FROM [rewards].[chapter_daily_snapshot] WHERE [Snapshot_Date] = ? AND [Multiplier] = 3) AS [Multiplier3ChapterCount]
FROM [rewards].[reward_runs] AS r
WHERE r.[Snapshot_Date] = ?
  AND r.[Run_Type] = ?
ORDER BY r.[RunID] DESC;
"""


def create_daily_snapshot(
    adapter: SnapshotSqlAdapter,
    snapshot_date: date,
    *,
    run_type: str = "daily",
    dry_run: bool = False,
    replace: bool = False,
    max_member_agaid: int = MAX_MEMBER_AGAID,
) -> SnapshotResult:
    existing = _load_existing_counts(adapter, snapshot_date)
    if dry_run:
        preview = _load_preview_counts(adapter, snapshot_date, max_member_agaid=max_member_agaid)
        return _result_from_counts(
            snapshot_date=snapshot_date,
            dry_run=True,
            run_id=None,
            counts=preview,
            existing=existing,
        )

    if not replace and (
        existing["ExistingMemberSnapshotCount"] > 0 or existing["ExistingChapterSnapshotCount"] > 0
    ):
        raise ValueError(
            f"Rewards snapshots already exist for {snapshot_date.isoformat()}. "
            "Use replace=True to rebuild them."
        )

    adapter.execute_statements(
        [
            (
                CREATE_SNAPSHOT_SQL,
                (snapshot_date, run_type, max_member_agaid, bool(replace)),
            )
        ]
    )
    rows = adapter.query_rows(
        SNAPSHOT_RESULT_SQL,
        (
            snapshot_date,
            snapshot_date,
            snapshot_date,
            snapshot_date,
            snapshot_date,
            snapshot_date,
            snapshot_date,
            snapshot_date,
            snapshot_date,
            run_type,
        ),
    )
    if not rows:
        raise RuntimeError(f"Snapshot run for {snapshot_date.isoformat()} did not produce a reward_runs row.")
    return _result_from_counts(
        snapshot_date=snapshot_date,
        dry_run=False,
        run_id=_coerce_optional_int(rows[0].get("RunID")),
        counts=rows[0],
        existing=existing,
    )


def _load_existing_counts(adapter: SnapshotSqlAdapter, snapshot_date: date) -> dict[str, int]:
    rows = adapter.query_rows(SNAPSHOT_EXISTING_COUNTS_SQL, (snapshot_date, snapshot_date))
    row = rows[0] if rows else {}
    return {
        "ExistingMemberSnapshotCount": _coerce_int(row.get("ExistingMemberSnapshotCount")),
        "ExistingChapterSnapshotCount": _coerce_int(row.get("ExistingChapterSnapshotCount")),
    }


def _load_preview_counts(
    adapter: SnapshotSqlAdapter,
    snapshot_date: date,
    *,
    max_member_agaid: int,
) -> dict[str, int]:
    rows = adapter.query_rows(SNAPSHOT_PREVIEW_SQL, (snapshot_date, max_member_agaid))
    if not rows:
        raise RuntimeError(f"Could not preview rewards snapshot for {snapshot_date.isoformat()}.")
    return rows[0]


def _result_from_counts(
    *,
    snapshot_date: date,
    dry_run: bool,
    run_id: int | None,
    counts: dict[str, Any],
    existing: dict[str, int],
) -> SnapshotResult:
    return SnapshotResult(
        snapshot_date=snapshot_date,
        dry_run=dry_run,
        run_id=run_id,
        member_snapshot_count=_coerce_int(counts.get("MemberSnapshotCount")),
        active_member_count=_coerce_int(counts.get("ActiveMemberCount")),
        tournament_pass_count=_coerce_int(counts.get("TournamentPassCount")),
        chapter_snapshot_count=_coerce_int(counts.get("ChapterSnapshotCount")),
        current_chapter_count=_coerce_int(counts.get("CurrentChapterCount")),
        multiplier_1_chapter_count=_coerce_int(counts.get("Multiplier1ChapterCount")),
        multiplier_2_chapter_count=_coerce_int(counts.get("Multiplier2ChapterCount")),
        multiplier_3_chapter_count=_coerce_int(counts.get("Multiplier3ChapterCount")),
        existing_member_snapshot_count=existing["ExistingMemberSnapshotCount"],
        existing_chapter_snapshot_count=existing["ExistingChapterSnapshotCount"],
    )


def _coerce_int(value: Any) -> int:
    if value is None:
        return 0
    return int(value)


def _coerce_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def parse_snapshot_date(value: str | None) -> date:
    if not value:
        return date.today()
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Snapshot date must use YYYY-MM-DD format.") from exc


def print_snapshot_result(result: SnapshotResult, output: TextIO) -> None:
    label = "Rewards Snapshot Preview" if result.dry_run else "Rewards Snapshot"
    print(label, file=output)
    print(f"  Date: {result.snapshot_date.isoformat()}", file=output)
    if result.run_id is not None:
        print(f"  RunID: {result.run_id}", file=output)
    print(f"  Existing member snapshots: {result.existing_member_snapshot_count}", file=output)
    print(f"  Existing chapter snapshots: {result.existing_chapter_snapshot_count}", file=output)
    print(f"  Member snapshots: {result.member_snapshot_count}", file=output)
    print(f"  Active members: {result.active_member_count}", file=output)
    print(f"  Tournament pass members: {result.tournament_pass_count}", file=output)
    print(f"  Chapter snapshots: {result.chapter_snapshot_count}", file=output)
    print(f"  Current chapters: {result.current_chapter_count}", file=output)
    print(
        "  Chapter multipliers: "
        f"1x={result.multiplier_1_chapter_count}, "
        f"2x={result.multiplier_2_chapter_count}, "
        f"3x={result.multiplier_3_chapter_count}",
        file=output,
    )


def main(argv: list[str] | None = None, output: TextIO = sys.stdout) -> int:
    parser = argparse.ArgumentParser(description="Build AGA Chapter Rewards daily snapshots.")
    parser.add_argument("--date", dest="snapshot_date", type=parse_snapshot_date, help="Snapshot date in YYYY-MM-DD format. Defaults to today.")
    parser.add_argument("--dry-run", action="store_true", help="Preview source counts without writing snapshots.")
    parser.add_argument("--replace", action="store_true", help="Replace existing snapshots for the date.")
    parser.add_argument("--run-type", default="daily", choices=["daily", "manual", "import", "backfill"], help="reward_runs.Run_Type value.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    args = parser.parse_args(argv)

    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        return 2

    result = create_daily_snapshot(
        SqlAdapter(conn_str),
        args.snapshot_date or date.today(),
        run_type=args.run_type,
        dry_run=args.dry_run,
        replace=args.replace,
    )
    if args.json:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True), file=output)
    else:
        print_snapshot_result(result, output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
