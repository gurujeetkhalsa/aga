import csv
import logging
import re
from datetime import date, datetime
from io import StringIO
from typing import Iterable, Optional


class CsvValidationError(ValueError):
    pass


NIGHTLY_MESSAGE_TYPE = "nightly_memchap_csv"
NIGHTLY_CATEGORY_MESSAGE_TYPE = "nightly_member_categories_csv"
CHAPTER_MESSAGE_TYPE = "chapter_csv"
MAX_MEMBER_AGAID = 50000

STAGING_COLUMNS = [
    "AGAID",
    "MemberType",
    "FirstName",
    "MiddleInitial",
    "LastName",
    "Nickname",
    "Pronouns",
    "LoginName",
    "Status",
    "LastLogin",
    "EmailAddress",
    "CellPhone",
    "PhoneNumber",
    "Address1",
    "Address2",
    "City",
    "State",
    "ZipCode",
    "Country",
    "DateOfBirth",
    "WorkTitle",
    "Gender",
    "JoinDate",
    "ExpirationDate",
    "LastRenewalDate",
    "ChapterID",
    "EmergencyContactName",
    "EmergencyContactRelationship",
    "EmergencyContactPhone",
    "EmergencyContactEmail",
]

INT_COLUMNS = {"AGAID", "ChapterID"}
DATE_COLUMNS = {"DateOfBirth", "JoinDate", "ExpirationDate", "LastRenewalDate"}
DATETIME_COLUMNS = {"LastLogin"}
OPTIONAL_SOURCE_COLUMNS = {"LastLogin"}
IGNORED_SOURCE_COLUMNS = {"memberdatecreated"}
MISLABELED_AGAID_SOURCE_COLUMNS = {"active"}
STRING_COLUMNS = set(STAGING_COLUMNS) - INT_COLUMNS - DATE_COLUMNS - DATETIME_COLUMNS
EXPECTED_HEADER_LOOKUP = {re.sub(r"[^a-z0-9]+", "", column.lower()): column for column in STAGING_COLUMNS}

CATEGORY_COLUMNS = ["AGAID", "Category"]
CATEGORY_HEADER_LOOKUP = {"agaid": "AGAID", "category": "Category"}

CHAPTER_COLUMNS = ["ChapterID", "ChapterCode", "ChapterName", "City", "State", "ChapterRepID", "CreatedDate", "Status"]
CHAPTER_INT_COLUMNS = {"ChapterID", "ChapterRepID"}
CHAPTER_DATETIME_COLUMNS = {"CreatedDate"}
CHAPTER_HEADER_ALIASES = {
    "chapterid": "ChapterID",
    "clubexpressid": "ChapterID",
    "id": "ChapterID",
    "chaptercode": "ChapterCode",
    "chaptershortname": "ChapterCode",
    "shortname": "ChapterCode",
    "code": "ChapterCode",
    "chaptername": "ChapterName",
    "name": "ChapterName",
    "city": "City",
    "state": "State",
    "chapterrepid": "ChapterRepID",
    "chapterrepagaid": "ChapterRepID",
    "chapterrepresentativeid": "ChapterRepID",
    "chapterrepresentativeagaid": "ChapterRepID",
    "primarycontactid": "ChapterRepID",
    "primarycontactmemberid": "ChapterRepID",
    "contactid": "ChapterRepID",
    "contactmemberid": "ChapterRepID",
    "presidentid": "ChapterRepID",
    "createddate": "CreatedDate",
    "datecreated": "CreatedDate",
    "status": "Status",
}


def detect_attachment_report_type(name: str, content_bytes: bytes) -> Optional[str]:
    if is_memchap_attachment_name(name):
        return NIGHTLY_MESSAGE_TYPE
    if is_chapter_attachment_name(name):
        return CHAPTER_MESSAGE_TYPE

    canonical_headers = read_csv_header_canonical(content_bytes)
    if not canonical_headers:
        return None
    if is_memchap_header(canonical_headers):
        return NIGHTLY_MESSAGE_TYPE
    if is_member_category_header(canonical_headers):
        return NIGHTLY_CATEGORY_MESSAGE_TYPE
    if is_chapter_header(canonical_headers):
        return CHAPTER_MESSAGE_TYPE
    return None


def is_memchap_attachment_name(name: str) -> bool:
    normalized_name = (name or "").strip().lower()
    return normalized_name.endswith(".csv") and "memchap" in normalized_name


def is_chapter_attachment_name(name: str) -> bool:
    normalized_name = (name or "").strip().lower()
    return normalized_name.endswith(".csv") and "chapterx" in normalized_name


def read_csv_header_canonical(csv_bytes: bytes) -> list[str]:
    rows = read_csv_matrix(csv_bytes, raise_on_error=False)
    if not rows:
        return []

    for row in rows[:2]:
        canonical = [canonicalize_header(value) for value in row if value is not None]
        if is_memchap_header(canonical) or is_member_category_header(canonical) or is_chapter_header(canonical):
            return canonical

    return [canonicalize_header(value) for value in rows[0] if value is not None]


def is_memchap_header(headers: list[str]) -> bool:
    required = {
        canonicalize_header("AGAID"),
        canonicalize_header("MemberType"),
        canonicalize_header("FirstName"),
        canonicalize_header("LastName"),
    }
    return required.issubset(set(headers))


def is_member_category_header(headers: list[str]) -> bool:
    return {canonicalize_header("AGAID"), canonicalize_header("Category")}.issubset(set(headers))


def is_chapter_header(headers: list[str]) -> bool:
    mapped = {CHAPTER_HEADER_ALIASES.get(header, "") for header in headers}
    return {"ChapterID", "ChapterCode", "ChapterName"}.issubset(mapped)


def parse_memchap_rows(csv_bytes: bytes) -> list[tuple]:
    csv_text = decode_csv_text(csv_bytes)

    reader = csv.DictReader(StringIO(csv_text))
    if not reader.fieldnames:
        raise CsvValidationError("CSV is missing a header row.")

    original_header = list(reader.fieldnames)
    incoming_header = normalize_header(original_header)
    canonical_header = [canonicalize_header(name) for name in incoming_header]
    has_explicit_agaid = "agaid" in canonical_header
    source_columns_by_target = {}
    unknown_columns = []
    duplicate_columns = []

    for original_name, normalized_name, canonical_name in zip(original_header, incoming_header, canonical_header):
        mapped = EXPECTED_HEADER_LOOKUP.get(canonical_name)
        if not mapped:
            if canonical_name in MISLABELED_AGAID_SOURCE_COLUMNS:
                if has_explicit_agaid:
                    continue
                mapped = "AGAID"
            else:
                if canonical_name in IGNORED_SOURCE_COLUMNS:
                    continue
                unknown_columns.append(normalized_name)
                continue
        if mapped in source_columns_by_target:
            duplicate_columns.append(mapped)
            continue
        source_columns_by_target[mapped] = original_name

    if unknown_columns:
        raise CsvValidationError(f"CSV contains unsupported columns: {', '.join(unknown_columns)}")
    if duplicate_columns:
        raise CsvValidationError(f"CSV contains duplicate columns: {', '.join(duplicate_columns)}")

    missing_columns = [
        column for column in STAGING_COLUMNS
        if column not in source_columns_by_target and column not in OPTIONAL_SOURCE_COLUMNS
    ]
    if missing_columns:
        raise CsvValidationError(f"CSV is missing required columns: {', '.join(missing_columns)}")

    rows = []
    skipped_non_member_rows = 0
    for row_number, row in enumerate(reader, start=2):
        converted_row = []
        for column in STAGING_COLUMNS:
            source_key = source_columns_by_target.get(column)
            raw_value = row.get(source_key, "") if source_key else ""
            try:
                converted_row.append(convert_value(column, raw_value or ""))
            except CsvValidationError as exc:
                raise CsvValidationError(f"Row {row_number}: {exc}") from exc
        if not is_member_agaid(converted_row[0]):
            skipped_non_member_rows += 1
            continue
        rows.append(tuple(converted_row))

    if skipped_non_member_rows:
        logging.info("Skipped %s MemChap rows with non-member AGAIDs.", skipped_non_member_rows)
    if not rows:
        raise CsvValidationError("CSV did not contain any data rows.")
    return rows


def parse_member_category_rows(csv_bytes: bytes) -> list[tuple[int, str]]:
    csv_rows = read_csv_matrix(csv_bytes)
    if not csv_rows:
        raise CsvValidationError("CSV is missing a header row.")

    header_index = None
    original_header = None
    for idx, row in enumerate(csv_rows[:2]):
        canonical = [canonicalize_header(value) for value in row]
        if is_member_category_header(canonical):
            header_index = idx
            original_header = row
            break

    if header_index is None or original_header is None:
        raise CsvValidationError("CSV is missing required columns: AGAID, Category")

    incoming_header = normalize_header(original_header)
    source_columns_by_target = {}
    unknown_columns = []
    duplicate_columns = []

    for original_name, normalized_name in zip(original_header, incoming_header):
        canonical_name = canonicalize_header(normalized_name)
        mapped = CATEGORY_HEADER_LOOKUP.get(canonical_name)
        if not mapped:
            if canonical_name:
                unknown_columns.append(normalized_name)
            continue
        if mapped in source_columns_by_target:
            duplicate_columns.append(mapped)
            continue
        source_columns_by_target[mapped] = original_name

    if unknown_columns:
        raise CsvValidationError(f"CSV contains unsupported columns: {', '.join(unknown_columns)}")
    if duplicate_columns:
        raise CsvValidationError(f"CSV contains duplicate columns: {', '.join(duplicate_columns)}")

    missing_columns = [column for column in CATEGORY_COLUMNS if column not in source_columns_by_target]
    if missing_columns:
        raise CsvValidationError(f"CSV is missing required columns: {', '.join(missing_columns)}")

    column_indexes = {name: original_header.index(source_columns_by_target[name]) for name in CATEGORY_COLUMNS}

    rows = []
    skipped_non_member_rows = 0
    seen_pairs = set()
    for row_number, row in enumerate(csv_rows[header_index + 1 :], start=header_index + 2):
        padded = list(row) + [""] * (len(original_header) - len(row))
        agaid_raw = padded[column_indexes["AGAID"]] or ""
        category_raw = padded[column_indexes["Category"]] or ""
        try:
            agaid = int(agaid_raw.strip())
        except ValueError as exc:
            raise CsvValidationError(f"Row {row_number}: Column AGAID requires an integer. Received '{agaid_raw}'.") from exc

        category = category_raw.strip()
        if not category:
            raise CsvValidationError(f"Row {row_number}: Column Category is required.")
        if not is_member_agaid(agaid):
            skipped_non_member_rows += 1
            continue

        pair = (agaid, category)
        if pair in seen_pairs:
            raise CsvValidationError(f"Row {row_number}: Duplicate AGAID/category pair {agaid}/{category}.")
        seen_pairs.add(pair)
        rows.append(pair)

    if skipped_non_member_rows:
        logging.info("Skipped %s category rows with non-member AGAIDs.", skipped_non_member_rows)
    return rows


def parse_chapter_rows(csv_bytes: bytes) -> list[tuple]:
    csv_rows = read_csv_matrix(csv_bytes)
    if not csv_rows:
        raise CsvValidationError("CSV is missing a header row.")

    header_index = None
    original_header = None
    for idx, row in enumerate(csv_rows[:2]):
        canonical = [canonicalize_header(value) for value in row]
        if is_chapter_header(canonical):
            header_index = idx
            original_header = row
            break

    if header_index is None or original_header is None:
        raise CsvValidationError("CSV is missing required chapter columns: ChapterID, ChapterCode, ChapterName")

    incoming_header = normalize_header(original_header)
    source_columns_by_target = {}
    duplicate_columns = []

    for column_index, normalized_name in enumerate(incoming_header):
        canonical_name = canonicalize_header(normalized_name)
        mapped = CHAPTER_HEADER_ALIASES.get(canonical_name)
        if not mapped:
            continue
        if mapped in source_columns_by_target:
            duplicate_columns.append(mapped)
            continue
        source_columns_by_target[mapped] = column_index

    if duplicate_columns:
        raise CsvValidationError(f"CSV contains duplicate chapter columns: {', '.join(sorted(set(duplicate_columns)))}")
    if "ChapterID" not in source_columns_by_target:
        raise CsvValidationError("CSV is missing required column: ChapterID")
    if "ChapterCode" not in source_columns_by_target:
        raise CsvValidationError("CSV is missing required column: ChapterCode")
    if "ChapterName" not in source_columns_by_target:
        raise CsvValidationError("CSV is missing required column: ChapterName")

    rows = []
    seen_chapters = set()
    for row_number, row in enumerate(csv_rows[header_index + 1 :], start=header_index + 2):
        if not any((value or "").strip() for value in row):
            continue
        padded = list(row) + [""] * (len(original_header) - len(row))
        converted_row = []
        for column in CHAPTER_COLUMNS:
            column_index = source_columns_by_target.get(column)
            raw_value = padded[column_index] if column_index is not None and column_index < len(padded) else ""
            try:
                converted_row.append(convert_chapter_value(column, raw_value or ""))
            except CsvValidationError as exc:
                raise CsvValidationError(f"Row {row_number}: {exc}") from exc
        chapter_id = converted_row[0]
        if chapter_id in seen_chapters:
            raise CsvValidationError(f"Row {row_number}: Duplicate ChapterID {chapter_id}.")
        seen_chapters.add(chapter_id)
        rows.append(tuple(converted_row))

    if not rows:
        raise CsvValidationError("CSV did not contain any chapter data rows.")
    return rows


def parse_date(value: str) -> date:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise CsvValidationError(f"Invalid date value '{value}'.")


def parse_datetime(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        pass
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M %p",
    ):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    try:
        return datetime.combine(parse_date(normalized), datetime.min.time())
    except CsvValidationError as exc:
        raise CsvValidationError(f"Invalid datetime value '{value}'.") from exc


def convert_value(column: str, raw_value: str):
    value = raw_value.strip()
    if value == "":
        return None
    if column in INT_COLUMNS:
        try:
            return int(value)
        except ValueError as exc:
            raise CsvValidationError(f"Column {column} requires an integer. Received '{raw_value}'.") from exc
    if column in DATE_COLUMNS:
        return parse_date(value)
    if column in DATETIME_COLUMNS:
        return parse_datetime(value)
    if column in STRING_COLUMNS:
        return value
    raise CsvValidationError(f"Unsupported column mapping for {column}.")


def convert_chapter_value(column: str, raw_value: str):
    value = raw_value.strip()
    if value == "":
        if column in {"ChapterID", "ChapterCode", "ChapterName"}:
            raise CsvValidationError(f"Column {column} is required.")
        return None
    if column in CHAPTER_INT_COLUMNS:
        try:
            return int(value)
        except ValueError as exc:
            raise CsvValidationError(f"Column {column} requires an integer. Received '{raw_value}'.") from exc
    if column in CHAPTER_DATETIME_COLUMNS:
        return parse_datetime(value)
    return value


def read_csv_matrix(csv_bytes: bytes, *, raise_on_error: bool = True) -> list[list[str]]:
    try:
        csv_text = csv_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        if raise_on_error:
            raise CsvValidationError("CSV must be UTF-8 encoded.") from exc
        return []
    return list(csv.reader(StringIO(csv_text)))


def decode_csv_text(csv_bytes: bytes) -> str:
    try:
        return csv_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvValidationError("CSV must be UTF-8 encoded.") from exc


def normalize_header(fieldnames: Iterable[Optional[str]]) -> list[str]:
    normalized = []
    for field in fieldnames:
        if field is None:
            normalized.append("")
            continue
        normalized.append(field.strip())
    return normalized


def canonicalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.strip().lower())


def is_member_agaid(agaid: Optional[int]) -> bool:
    return agaid is not None and agaid < MAX_MEMBER_AGAID
