"""Export members active on a snapshot date."""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bayrate.sql_adapter import SqlAdapter, get_sql_connection_string


ACTIVE_MEMBER_SQL = """
SELECT
    snapshot.[AGAID],
    CASE
        WHEN NULLIF(LTRIM(RTRIM(member.[FirstName])), N'') IS NOT NULL
         AND NULLIF(LTRIM(RTRIM(member.[LastName])), N'') IS NOT NULL
            THEN LTRIM(RTRIM(member.[FirstName])) + N' ' + LTRIM(RTRIM(member.[LastName]))
        ELSE COALESCE(NULLIF(LTRIM(RTRIM(member.[FirstName])), N''), NULLIF(LTRIM(RTRIM(member.[LastName])), N''))
    END AS [Name],
    NULLIF(LTRIM(RTRIM(member.[EmailAddress])), N'') AS [Email],
    snapshot.[Expiration_Date] AS [ExpirationDate],
    snapshot.[Member_Type] AS [MemberType],
    member.[Status] AS [CurrentStatus]
FROM [rewards].[member_daily_snapshot] AS snapshot
INNER JOIN [membership].[members] AS member
    ON member.[AGAID] = snapshot.[AGAID]
WHERE snapshot.[Snapshot_Date] = ?
  AND snapshot.[Is_Active] = 1
  AND UPPER(LTRIM(RTRIM(COALESCE(member.[Status], N'')))) IN (N'ACTIVE', N'EXPIRED')
  AND LTRIM(RTRIM(COALESCE(snapshot.[Member_Type], N''))) IN
  (
      N'Adult Full',
      N'Full',
      N'Youth',
      N'Adult Full - Lifetime',
      N'Life',
      N'Complimentary'
  )
ORDER BY
    member.[LastName],
    member.[FirstName],
    snapshot.[AGAID]
"""

SNAPSHOT_SUMMARY_SQL = """
SELECT
    snapshot.[Member_Type] AS [MemberType],
    COUNT(*) AS [MemberCount]
FROM [rewards].[member_daily_snapshot] AS snapshot
WHERE snapshot.[Snapshot_Date] = ?
  AND snapshot.[Is_Active] = 1
GROUP BY snapshot.[Member_Type]
ORDER BY snapshot.[Member_Type]
"""


def csv_value(value: object) -> object:
    if isinstance(value, date):
        return value.isoformat()
    return value if value is not None else ""


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fieldnames = ["AGAID", "Name", "Email", "ExpirationDate", "MemberType", "CurrentStatus"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: csv_value(row.get(field)) for field in fieldnames})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    conn_str = get_sql_connection_string()
    if not conn_str:
        raise RuntimeError("Missing SQL connection string.")

    adapter = SqlAdapter(conn_str)
    rows = adapter.query_rows(ACTIVE_MEMBER_SQL, (args.as_of,))
    summary_rows = adapter.query_rows(SNAPSHOT_SUMMARY_SQL, (args.as_of,))
    write_csv(args.output, rows)

    print(f"as_of={args.as_of.isoformat()}")
    print(f"output={args.output}")
    print(f"rows={len(rows)}")
    print("active_snapshot_member_types=")
    for row in summary_rows:
        print(f"  {row.get('MemberType') or '<blank>'}: {row.get('MemberCount')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
