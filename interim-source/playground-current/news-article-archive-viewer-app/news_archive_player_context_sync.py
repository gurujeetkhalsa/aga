# Copyright 2026, American Go Association, All rights reserved

"""Build Ratings Explorer player-context links from archived news articles."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode


DEFAULT_VIEWER_URL = "https://aga-news-archive-viewer.azurewebsites.net/api/news-archive"
DEFAULT_EXCLUDED_SOURCE_MODES = ("external-go-news-digest",)
MAX_MEMBER_AGAID = 50000
JOURNAL_EXCLUDED_MATCH_NAMES = {"chris garlock"}
DEFAULT_JOURNAL_NAME_PREFIXES = (
    "AGA President",
    "AGA Vice President",
    "AGA Board Chair",
    "AGA Board Member",
    "Congress Director",
    "Chapter President",
    "Chapter Vice President",
    "Chapter Secretary",
    "Chapter Treasurer",
    "Executive Director",
    "Membership Director",
    "Tournament Director",
)
JOURNAL_FIRST_NAME_ALIAS_GROUPS = (
    ("abigail", "abby", "abbie", "gail"),
    ("alexander", "alex", "xander"),
    ("alexandra", "alex", "lexi", "sandra"),
    ("andrew", "andy", "drew"),
    ("anthony", "tony"),
    ("barbara", "barb", "barbie"),
    ("benjamin", "ben", "benny"),
    ("catherine", "cathy", "kathy", "kate", "katie"),
    ("charles", "charlie", "chuck"),
    ("christopher", "chris"),
    ("daniel", "dan", "danny"),
    ("david", "dave", "davey"),
    ("deborah", "deb", "debbie"),
    ("donald", "don", "donny"),
    ("dorothy", "dot", "dottie"),
    ("edward", "ed", "eddie", "ted", "teddy", "ned"),
    ("edwin", "ed", "eddie", "ned"),
    ("elizabeth", "beth", "betsy", "betty", "eliza", "liz", "lizzie"),
    ("frances", "fran", "frannie"),
    ("frederick", "fred", "freddie"),
    ("gerald", "gerry", "jerry"),
    ("gregory", "greg"),
    ("james", "jim", "jimmy"),
    ("janet", "jan"),
    ("jennifer", "jen", "jenny"),
    ("john", "jack", "johnny"),
    ("joseph", "joe", "joey"),
    ("katherine", "kathy", "kate", "katie"),
    ("kenneth", "ken", "kenny"),
    ("margaret", "maggie", "meg", "peggy"),
    ("michael", "mike", "mikey"),
    ("nicholas", "nick", "nicky"),
    ("patricia", "pat", "patty", "tricia"),
    ("rebecca", "becky", "becca"),
    ("richard", "rick", "ricky", "dick"),
    ("robert", "rob", "bob", "bobby"),
    ("ronald", "ron", "ronnie"),
    ("samuel", "sam", "sammy"),
    ("stephen", "steve", "stevie"),
    ("steven", "steve", "stevie"),
    ("susan", "sue", "susie"),
    ("theodore", "ted", "teddy", "theo"),
    ("thomas", "tom", "tommy"),
    ("william", "will", "bill", "billy"),
)


@dataclass(frozen=True)
class NewsArchivePlayerContextSyncConfig:
    """Configuration for archive-backed Ratings Explorer article context."""

    viewer_url: str = DEFAULT_VIEWER_URL
    excluded_source_modes: tuple[str, ...] = DEFAULT_EXCLUDED_SOURCE_MODES
    include_inactive: bool = False
    include_not_visible: bool = False
    max_articles: int = 0
    max_member_agaid: int = MAX_MEMBER_AGAID


def sync_news_archive_player_context(adapter: Any, config: NewsArchivePlayerContextSyncConfig) -> dict[str, Any]:
    """Refresh archive-backed player/article matches for Ratings Explorer."""
    ensure_player_context_schema(adapter)
    run_key = f"news-archive-player-context-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    member_lookup = load_member_name_lookup(adapter, config)
    articles = load_archive_articles(adapter, config)
    matches: list[dict[str, Any]] = []
    matched_article_ids: set[int] = set()

    for article in articles:
        text = archive_article_match_text(article)
        article_matches = match_member_rows_in_article(text, member_lookup)
        if article_matches:
            matched_article_ids.add(int(article["News_Article_Snapshot_ID"]))
        for agaid, matched_name in article_matches:
            matches.append(
                {
                    "snapshot_id": int(article["News_Article_Snapshot_ID"]),
                    "article_id": int(article["News_Article_ID"]),
                    "item_id": int(article["Item_ID"]),
                    "agaid": agaid,
                    "matched_name": matched_name,
                    "article_title": text_value(article.get("Headline"))[:500],
                    "article_link": archive_article_link(config, int(article["News_Article_Snapshot_ID"]))[:1000],
                    "article_active_date": article.get("Active_Date"),
                }
            )

    write_player_context_matches(adapter, matches, run_key)
    return {
        "run_key": run_key,
        "source_article_count": len(articles),
        "member_lookup_count": sum(len(rows) for rows in member_lookup.values()),
        "matched_article_count": len(matched_article_ids),
        "match_count": len(matches),
        "excluded_source_modes": list(config.excluded_source_modes),
        "include_inactive": config.include_inactive,
        "include_not_visible": config.include_not_visible,
    }


def ensure_player_context_schema(adapter: Any) -> None:
    """Create the archive-backed player context table when needed."""
    adapter.query_rows(
        """
        IF SCHEMA_ID(N'integration') IS NULL
        BEGIN
            EXEC(N'CREATE SCHEMA [integration]');
        END;

        IF OBJECT_ID(N'integration.news_archive_article_member_match', N'U') IS NULL
        BEGIN
            CREATE TABLE [integration].[news_archive_article_member_match]
            (
                [NewsArchiveArticleMemberMatchID] bigint IDENTITY(1,1) NOT NULL
                    CONSTRAINT [PK_news_archive_article_member_match] PRIMARY KEY,
                [News_Article_Snapshot_ID] bigint NOT NULL,
                [News_Article_ID] bigint NOT NULL,
                [Item_ID] int NOT NULL,
                [AGAID] int NOT NULL,
                [MatchedName] nvarchar(200) NOT NULL,
                [ArticleTitle] nvarchar(500) NOT NULL,
                [ArticleLink] nvarchar(1000) NOT NULL,
                [ArticleActiveDate] date NULL,
                [Refresh_Run_Key] nvarchar(128) NOT NULL,
                [Created_At] datetime2(0) NOT NULL
                    CONSTRAINT [DF_news_archive_article_member_match_created_at] DEFAULT SYSUTCDATETIME(),
                [Updated_At] datetime2(0) NOT NULL
                    CONSTRAINT [DF_news_archive_article_member_match_updated_at] DEFAULT SYSUTCDATETIME()
            );
        END;

        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE [name] = N'UX_news_archive_article_member_match_snapshot_agaid_name'
              AND [object_id] = OBJECT_ID(N'integration.news_archive_article_member_match', N'U')
        )
        BEGIN
            CREATE UNIQUE INDEX [UX_news_archive_article_member_match_snapshot_agaid_name]
                ON [integration].[news_archive_article_member_match]
                (
                    [News_Article_Snapshot_ID],
                    [AGAID],
                    [MatchedName]
                );
        END;

        IF NOT EXISTS
        (
            SELECT 1
            FROM sys.indexes
            WHERE [name] = N'IX_news_archive_article_member_match_agaid_date'
              AND [object_id] = OBJECT_ID(N'integration.news_archive_article_member_match', N'U')
        )
        BEGIN
            CREATE INDEX [IX_news_archive_article_member_match_agaid_date]
                ON [integration].[news_archive_article_member_match]
                (
                    [AGAID],
                    [ArticleActiveDate] DESC,
                    [News_Article_Snapshot_ID] DESC
                )
                INCLUDE ([ArticleTitle], [ArticleLink]);
        END;

        SELECT CAST(1 AS int) AS [Schema_Applied];
        """
    )


def load_archive_articles(adapter: Any, config: NewsArchivePlayerContextSyncConfig) -> list[dict[str, Any]]:
    """Load latest archive snapshots eligible for player-name parsing."""
    excluded_modes = tuple(mode for mode in config.excluded_source_modes if mode)
    mode_filter = ""
    params: list[Any] = []
    if excluded_modes:
        placeholders = ", ".join("?" for _ in excluded_modes)
        mode_filter = f"  AND r.[Source_Mode] NOT IN ({placeholders})\n"
        params.extend(excluded_modes)
    public_filters = ""
    if not config.include_inactive:
        public_filters += "  AND COALESCE(s.[Status], a.[Current_Status]) = N'Active'\n"
    if not config.include_not_visible:
        public_filters += "  AND COALESCE(s.[Visibility], a.[Visibility], N'') IN (N'To Everyone', N'')\n"
    top_clause = f"TOP ({max(1, int(config.max_articles))}) " if int(config.max_articles or 0) > 0 else ""
    return adapter.query_rows(
        f"""
        WITH RankedSnapshots AS
        (
            SELECT
                s.*,
                ROW_NUMBER() OVER
                (
                    PARTITION BY s.[News_Article_ID]
                    ORDER BY s.[Created_At] DESC, s.[News_Article_Snapshot_ID] DESC
                ) AS [Snapshot_Rank]
            FROM [clubexpress].[news_article_snapshots] AS s
        )
        SELECT {top_clause}
            s.[News_Article_Snapshot_ID],
            a.[News_Article_ID],
            a.[Item_ID],
            COALESCE(s.[Headline], a.[Headline]) AS [Headline],
            COALESCE(s.[Author], a.[Author]) AS [Author],
            COALESCE(s.[Active_Date], a.[Active_Date]) AS [Active_Date],
            s.[Summary],
            s.[Search_Text],
            r.[Source_Mode]
        FROM RankedSnapshots AS s
        INNER JOIN [clubexpress].[news_articles] AS a
            ON a.[News_Article_ID] = s.[News_Article_ID]
        INNER JOIN [clubexpress].[news_archive_runs] AS r
            ON r.[News_Archive_Run_ID] = s.[News_Archive_Run_ID]
        WHERE s.[Snapshot_Rank] = 1
          AND a.[Club_ID] <> 0
{mode_filter}{public_filters}
        ORDER BY COALESCE(s.[Active_Date], a.[Active_Date]) DESC, a.[Item_ID] DESC;
        """,
        tuple(params),
    )


def load_member_name_lookup(adapter: Any, config: NewsArchivePlayerContextSyncConfig) -> dict[str, list[tuple[int, str]]]:
    """Load rated AGA members by normalized first/last name."""
    lookup: dict[str, list[tuple[int, str]]] = {}
    rows = adapter.query_rows(
        """
        SELECT DISTINCT
            m.[AGAID],
            m.[FirstName],
            m.[LastName]
        FROM [membership].[members] AS m
        WHERE m.[AGAID] < ?
          AND m.[FirstName] IS NOT NULL
          AND m.[LastName] IS NOT NULL
          AND EXISTS
          (
              SELECT 1
              FROM [ratings].[ratings] AS r
              WHERE r.[Pin_Player] = m.[AGAID]
          );
        """,
        (config.max_member_agaid,),
    )
    for row in rows:
        matched_name = f"{row.get('FirstName') or ''} {row.get('LastName') or ''}".strip()
        key = normalize_person_name(matched_name)
        if not key:
            continue
        entry = (int(row["AGAID"]), matched_name)
        if entry not in lookup.setdefault(key, []):
            lookup[key].append(entry)
    return lookup


def write_player_context_matches(adapter: Any, matches: list[dict[str, Any]], run_key: str) -> None:
    """Upsert current matches and remove rows not seen in this refresh."""
    for chunk in chunked(matches, 150):
        merge_player_context_match_chunk(adapter, chunk, run_key)
    rows = adapter.query_rows(
        """
        SET NOCOUNT ON;
        DELETE FROM [integration].[news_archive_article_member_match]
        WHERE [Refresh_Run_Key] <> ?;
        SELECT @@ROWCOUNT AS [Deleted_Count];
        """,
        (run_key,),
    )
    deleted_count = int(rows[0].get("Deleted_Count") or 0) if rows else 0
    if deleted_count:
        logging.info("Removed %s stale archive player-context matches.", deleted_count)


def merge_player_context_match_chunk(adapter: Any, matches: list[dict[str, Any]], run_key: str) -> None:
    """Merge one chunk of archive player-context matches."""
    if not matches:
        return
    values_sql = ",\n".join("(?, ?, ?, ?, ?, ?, ?, ?, ?)" for _ in matches)
    params: list[Any] = []
    for match in matches:
        params.extend(
            [
                match["snapshot_id"],
                match["article_id"],
                match["item_id"],
                match["agaid"],
                match["matched_name"],
                match["article_title"],
                match["article_link"],
                match["article_active_date"],
                run_key,
            ]
        )
    adapter.query_rows(
        f"""
        SET NOCOUNT ON;
        MERGE [integration].[news_archive_article_member_match] WITH (HOLDLOCK) AS target
        USING
        (
            VALUES
            {values_sql}
        ) AS source
        (
            [News_Article_Snapshot_ID],
            [News_Article_ID],
            [Item_ID],
            [AGAID],
            [MatchedName],
            [ArticleTitle],
            [ArticleLink],
            [ArticleActiveDate],
            [Refresh_Run_Key]
        )
            ON target.[News_Article_Snapshot_ID] = source.[News_Article_Snapshot_ID]
           AND target.[AGAID] = source.[AGAID]
           AND target.[MatchedName] = source.[MatchedName]
        WHEN MATCHED THEN
            UPDATE SET
                [News_Article_ID] = source.[News_Article_ID],
                [Item_ID] = source.[Item_ID],
                [ArticleTitle] = source.[ArticleTitle],
                [ArticleLink] = source.[ArticleLink],
                [ArticleActiveDate] = source.[ArticleActiveDate],
                [Refresh_Run_Key] = source.[Refresh_Run_Key],
                [Updated_At] = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT
            (
                [News_Article_Snapshot_ID],
                [News_Article_ID],
                [Item_ID],
                [AGAID],
                [MatchedName],
                [ArticleTitle],
                [ArticleLink],
                [ArticleActiveDate],
                [Refresh_Run_Key]
            )
            VALUES
            (
                source.[News_Article_Snapshot_ID],
                source.[News_Article_ID],
                source.[Item_ID],
                source.[AGAID],
                source.[MatchedName],
                source.[ArticleTitle],
                source.[ArticleLink],
                source.[ArticleActiveDate],
                source.[Refresh_Run_Key]
            );
        SELECT @@ROWCOUNT AS [Affected_Count];
        """,
        tuple(params),
    )


def archive_article_match_text(article: dict[str, Any]) -> str:
    """Return the article text used for player-name detection."""
    return " ".join(
        text_value(article.get(field))
        for field in ("Headline", "Author", "Summary", "Search_Text")
        if article.get(field)
    )


def archive_article_link(config: NewsArchivePlayerContextSyncConfig, snapshot_id: int) -> str:
    """Return a viewer link for one archived article snapshot."""
    base_url = (config.viewer_url or DEFAULT_VIEWER_URL).strip() or DEFAULT_VIEWER_URL
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{urlencode({'snapshotId': snapshot_id})}"


def match_member_rows_in_article(text: str, member_lookup: dict[str, list[tuple[int, str]]]) -> list[tuple[int, str]]:
    """Match rated AGA members mentioned in article text."""
    matched_rows = set()
    for candidate in extract_candidate_person_names(text):
        for key in journal_person_lookup_keys(candidate):
            if key in JOURNAL_EXCLUDED_MATCH_NAMES:
                continue
            matched_rows.update(member_lookup.get(key, []))
    return sorted(matched_rows, key=lambda row: (row[0], row[1].lower()))


def journal_person_lookup_keys(value: str) -> set[str]:
    """Return normalized lookup keys for a possible person name."""
    key = normalize_person_name(value)
    if not key or " " not in key:
        return set()
    first_name, last_name = key.split(" ", 1)
    return {f"{first_variant} {last_name}" for first_variant in journal_first_name_variants(first_name)}


def journal_first_name_variants(first_name: str) -> set[str]:
    """Return configured nickname variants for a first name."""
    normalized = (first_name or "").strip().lower()
    if not normalized:
        return set()
    variants = {normalized}
    for group in journal_first_name_alias_groups():
        if normalized in group:
            variants.update(group)
    return variants


def journal_first_name_alias_groups() -> list[set[str]]:
    """Return first-name alias groups, including optional app settings."""
    groups = [set(group) for group in JOURNAL_FIRST_NAME_ALIAS_GROUPS]
    extra_groups = os.environ.get("JOURNAL_FIRST_NAME_ALIAS_GROUPS", "")
    for raw_group in extra_groups.split(";"):
        names = {name.lower() for name in re.findall(r"[A-Za-z]+(?:[-'][A-Za-z]+)?", raw_group)}
        if len(names) >= 2:
            groups.append(names)
    return groups


def extract_candidate_person_names(text: str) -> set[str]:
    """Extract possible person names from article text."""
    candidates = set()
    pattern = re.compile(r"\b[A-Z][a-z]+(?:[-'][A-Z][a-z]+)?(?:\s+[A-Z][a-z]+(?:[-'][A-Z][a-z]+)?){1,3}\b")
    for match in pattern.finditer(text or ""):
        candidates.update(expand_candidate_person_names(match.group(0)))
    return candidates


def expand_candidate_person_names(value: str) -> set[str]:
    """Expand a candidate name into likely first/last windows."""
    tokens = re.findall(r"[A-Za-z]+(?:[-'][A-Za-z]+)?", value or "")
    if len(tokens) < 2:
        return set()
    candidates = {" ".join(tokens)}
    max_window = min(len(tokens), 4)
    for window_size in range(2, max_window + 1):
        candidates.add(" ".join(tokens[-window_size:]))
    stripped = strip_journal_name_prefix(tokens)
    if stripped:
        candidates.add(" ".join(stripped))
    return {candidate.strip() for candidate in candidates if candidate.strip()}


def strip_journal_name_prefix(tokens: list[str]) -> list[str]:
    """Strip configured role prefixes from a candidate name."""
    lowered_tokens = [token.lower() for token in tokens]
    for prefix_tokens in journal_name_prefix_token_lists():
        prefix_length = len(prefix_tokens)
        if len(lowered_tokens) <= prefix_length:
            continue
        if lowered_tokens[:prefix_length] == prefix_tokens:
            return tokens[prefix_length:]
    return []


def journal_name_prefix_token_lists() -> list[list[str]]:
    """Return configured role prefixes as token lists."""
    configured = list(DEFAULT_JOURNAL_NAME_PREFIXES)
    extra_prefixes = os.environ.get("JOURNAL_NAME_PREFIXES", "")
    configured.extend(prefix.strip() for prefix in extra_prefixes.split(";") if prefix.strip())
    token_lists = []
    seen = set()
    for prefix in configured:
        tokens = [token.lower() for token in re.findall(r"[A-Za-z]+(?:[-'][A-Za-z]+)?", prefix)]
        if not tokens:
            continue
        key = tuple(tokens)
        if key in seen:
            continue
        seen.add(key)
        token_lists.append(tokens)
    return sorted(token_lists, key=len, reverse=True)


def normalize_person_name(value: str) -> str:
    """Normalize a person name for matching."""
    tokens = re.findall(r"[A-Za-z]+(?:[-'][A-Za-z]+)?", value or "")
    if len(tokens) < 2:
        return ""
    return " ".join(token.lower() for token in tokens[:2])


def text_value(value: Any) -> str:
    """Return compact text for optional SQL values."""
    return re.sub(r"\s+", " ", str(value or "")).strip()


def chunked(values: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    """Yield list chunks."""
    return [values[index:index + size] for index in range(0, len(values), size)]
