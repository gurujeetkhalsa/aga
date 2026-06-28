import re
from datetime import date, datetime
from html.parser import HTMLParser
from typing import Optional


class EmailProcessingError(ValueError):
    """Represent email processing error failures."""
    pass


class _HtmlTableParser(HTMLParser):
    """Represent html table parser."""
    def __init__(self) -> None:
        """Initialize the html table parser instance."""
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self._table_depth = 0
        self._current_table: list[list[str]] | None = None
        self._current_row: list[str] | None = None
        self._cell_parts: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        """Handle starttag."""
        normalized_tag = tag.lower()
        if normalized_tag == "table":
            self._table_depth += 1
            if self._table_depth == 1:
                self._current_table = []
            return

        if self._table_depth < 1:
            return

        if normalized_tag == "tr":
            self._current_row = []
        elif normalized_tag in {"td", "th"}:
            self._cell_parts = []
        elif normalized_tag == "br" and self._cell_parts is not None:
            self._cell_parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        """Handle endtag."""
        normalized_tag = tag.lower()
        if normalized_tag == "table":
            if self._table_depth == 1 and self._current_table is not None:
                table = [row for row in self._current_table if any(cell.strip() for cell in row)]
                if table:
                    self.tables.append(table)
                self._current_table = None
                self._current_row = None
                self._cell_parts = None
            if self._table_depth:
                self._table_depth -= 1
            return

        if self._table_depth < 1:
            return

        if normalized_tag in {"td", "th"} and self._cell_parts is not None:
            text = re.sub(r"\s+", " ", "".join(self._cell_parts)).strip()
            if self._current_row is not None:
                self._current_row.append(text)
            self._cell_parts = None
        elif normalized_tag == "tr" and self._current_row is not None:
            if self._current_table is not None and any(cell.strip() for cell in self._current_row):
                self._current_table.append(self._current_row)
            self._current_row = None

    def handle_data(self, data: str) -> None:
        """Handle data."""
        if self._cell_parts is not None and data:
            self._cell_parts.append(data)


def parse_new_member_email(text: str) -> dict:
    """Parse new member email."""
    segment = _slice_between_markers(text, "membership in American Go Association.", "Club Url")
    agaid = _extract_required_int(segment, r"Member Number:\s*(\d+)", "AGAID")
    member_type = _extract_member_type(segment)
    if member_type.strip().lower() == "chapter":
        raise EmailProcessingError("Chapter member signup emails should be ignored before parsing.")

    email_address = _extract_optional_text(segment, r"Email:\s*(.*?)\s*Login")
    name_line = _extract_name_line(segment)
    if not name_line:
        raise EmailProcessingError("Could not determine member name from new member email.")

    name_parts = [part for part in name_line.split() if part]
    if len(name_parts) < 2:
        raise EmailProcessingError(f"Full name line is not parseable: {name_line!r}")

    return {
        "AGAID": agaid,
        "MemberType": member_type,
        "FirstName": name_parts[0],
        "LastName": name_parts[-1],
        "EmailAddress": email_address,
        "ExpirationDate": _extract_membership_expiration_date(segment),
    }


def parse_renewal_email(text: str) -> dict:
    """Parse renewal email."""
    segment = _slice_between_markers(text, "A membership renewal has been processed for American Go Association.", "Club Url")
    member_type = _extract_member_type(segment)
    return {
        "AGAID": _extract_required_int(segment, r"Member Number:\s*(\d+)", "AGAID"),
        "PhoneNumber": _extract_optional_text(segment, r"Phone:\s*(.*?)\s*Email"),
        "EmailAddress": _extract_optional_text(segment, r"Email:\s*(.*?)\s*Login Name"),
        "LoginName": _extract_optional_text(segment, r"Login Name:\s*(.*?)\s*(?:Member\s+)?Type"),
        "MemberType": member_type,
        "ExpirationDate": _extract_membership_expiration_date(segment),
        "IsChapterMember": member_type.strip().lower().startswith("chapter"),
    }


def parse_chapter_renewal_notice(html_body: str, text_body: str) -> list[dict]:
    """Parse chapter renewal notice."""
    rows = extract_chapter_renewal_notice_rows_from_html(html_body or "")
    if not rows:
        rows = extract_chapter_renewal_notice_rows_from_text(text_body or "")
    return _dedupe_chapter_renewal_notice_rows(rows)


def extract_chapter_renewal_notice_rows_from_html(html_body: str) -> list[dict]:
    """Extract chapter renewal notice rows from html."""
    if not html_body:
        return []

    parser = _HtmlTableParser()
    parser.feed(html_body)
    parser.close()

    rows: list[dict] = []
    for table in parser.tables:
        rows.extend(_extract_chapter_renewal_notice_rows_from_matrix(table))
    return rows


def extract_chapter_renewal_notice_rows_from_text(text: str) -> list[dict]:
    """Extract chapter renewal notice rows from text."""
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    if not lines:
        return []

    for index, line in enumerate(lines):
        cells = _split_renewal_notice_text_row(line)
        header_lookup = _chapter_renewal_notice_header_lookup(cells)
        if "member" not in header_lookup or "type" not in header_lookup:
            continue

        matrix = [cells]
        for body_line in lines[index + 1:]:
            body_cells = _split_renewal_notice_text_row(body_line)
            if len(body_cells) < 2:
                continue
            matrix.append(body_cells)
        return _extract_chapter_renewal_notice_rows_from_matrix(matrix)

    return []


def _slice_between_markers(text: str, start_marker: str, end_marker: str) -> str:
    """Execute the slice between markers routine."""
    if start_marker in text:
        text = text.split(start_marker, 1)[1]
    if end_marker in text:
        text = text.split(end_marker, 1)[0]
    return text.strip()


def _extract_name_line(text: str) -> Optional[str]:
    """Extract name line."""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for line in lines[:8]:
        if ":" in line:
            continue
        if len(line.split()) >= 2:
            return line
    return None


def _extract_member_type(text: str) -> str:
    """Extract member type."""
    return _extract_required_text(
        text,
        r"(?:Member(?:ship)?\s+)?Type:\s*(.*?)\s*(?=(?:(?:New|Updated|Current)\s+)?(?:Member(?:ship)?\s+)?Expiration(?:\s+Date)?|Expires|Paid\s+Through|Good\s+Through|Total)\s*:?",
        "Member Type",
    )


def _extract_membership_expiration_date(text: str) -> Optional[date]:
    """Extract membership expiration date."""
    date_pattern = r"(\d{4}-\d{1,2}-\d{1,2}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})"
    preferred_patterns = (
        rf"\b(?:New|Updated|Current)\s+(?:Member(?:ship)?\s+)?Expiration(?:\s+Date)?\s*:?\s*{date_pattern}",
        rf"\b(?:Expires|Paid\s+Through|Good\s+Through)\s*:?\s*{date_pattern}",
    )
    for pattern in preferred_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return _parse_email_date(match.group(1), "membership expiration")

    generic_matches = list(
        re.finditer(
            rf"\b(?:Member(?:ship)?\s+)?Expiration(?:\s+Date)?\s*:?\s*{date_pattern}",
            text,
            re.IGNORECASE,
        )
    )
    if generic_matches:
        return _parse_email_date(generic_matches[-1].group(1), "membership expiration")
    return None


def _parse_email_date(value: str, label: str) -> date:
    """Parse email date."""
    normalized = (value or "").strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%m-%d-%y"):
        try:
            return datetime.strptime(normalized, fmt).date()
        except ValueError:
            continue
    raise EmailProcessingError(f"Could not parse {label} date '{value}'.")


def _extract_required_text(text: str, pattern: str, label: str) -> str:
    """Extract required text."""
    value = _extract_optional_text(text, pattern)
    if value is None or value == "":
        raise EmailProcessingError(f"Could not extract required field {label}.")
    return value


def _extract_optional_text(text: str, pattern: str) -> Optional[str]:
    """Extract optional text."""
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return re.sub(r"\s+", " ", match.group(1)).strip()


def _extract_required_int(text: str, pattern: str, label: str) -> int:
    """Extract required int."""
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        raise EmailProcessingError(f"Could not extract required integer field {label}.")
    return int(match.group(1))


def _extract_chapter_renewal_notice_rows_from_matrix(matrix: list[list[str]]) -> list[dict]:
    """Extract chapter renewal notice rows from matrix."""
    rows: list[dict] = []
    for header_index, header_row in enumerate(matrix):
        header_lookup = _chapter_renewal_notice_header_lookup(header_row)
        if "member" not in header_lookup or "type" not in header_lookup:
            continue

        for source_row_number, row in enumerate(matrix[header_index + 1:], start=header_index + 2):
            record = _chapter_renewal_notice_record(header_row, row)
            if not record:
                continue
            member_type = str(record.get("type") or "").strip()
            if member_type.lower() != "chapter":
                continue
            member_raw = str(record.get("member") or "").strip()
            chapter_id = _extract_chapter_id_from_member_cell(member_raw)
            if chapter_id is None:
                raise EmailProcessingError(f"Could not parse ChapterID from renewal notice member cell {member_raw!r}.")
            rows.append(
                {
                    "source_row_number": source_row_number,
                    "chapter_id": chapter_id,
                    "member_raw": member_raw,
                    "member_type": member_type,
                    "row_payload": record,
                }
            )
        if rows:
            return rows
    return rows


def _chapter_renewal_notice_record(header_row: list[str], row: list[str]) -> dict[str, str]:
    """Execute the chapter renewal notice record routine."""
    if not any(str(cell or "").strip() for cell in row):
        return {}
    record: dict[str, str] = {}
    for index, header in enumerate(header_row):
        key = _chapter_renewal_notice_header_key(header)
        if not key:
            continue
        record[key] = row[index].strip() if index < len(row) and row[index] is not None else ""
    return record


def _chapter_renewal_notice_header_lookup(header_row: list[str]) -> dict[str, int]:
    """Execute the chapter renewal notice header lookup routine."""
    lookup: dict[str, int] = {}
    for index, header in enumerate(header_row):
        key = _chapter_renewal_notice_header_key(header)
        if key:
            lookup.setdefault(key, index)
    return lookup


def _chapter_renewal_notice_header_key(header: str) -> str:
    """Execute the chapter renewal notice header key routine."""
    normalized = re.sub(r"[^a-z0-9]+", "", str(header or "").strip().lower())
    aliases = {
        "member": "member",
        "memberid": "member",
        "chapterid": "member",
        "type": "type",
        "membertype": "type",
        "membershiptype": "type",
    }
    return aliases.get(normalized, re.sub(r"[^a-z0-9]+", "_", str(header or "").strip().lower()).strip("_"))


def _split_renewal_notice_text_row(line: str) -> list[str]:
    """Execute the split renewal notice text row routine."""
    if "\t" in line:
        return [cell.strip() for cell in line.split("\t")]
    if "|" in line:
        return [cell.strip() for cell in line.split("|")]
    return [cell.strip() for cell in re.split(r"\s{2,}", line.strip())]


def _extract_chapter_id_from_member_cell(value: str) -> int | None:
    """Extract chapter id from member cell."""
    match = re.search(r"\d+", value or "")
    return int(match.group(0)) if match else None


def _dedupe_chapter_renewal_notice_rows(rows: list[dict]) -> list[dict]:
    """Execute the dedupe chapter renewal notice rows routine."""
    deduped = []
    seen: set[int] = set()
    for row in rows:
        chapter_id = int(row["chapter_id"])
        if chapter_id in seen:
            raise EmailProcessingError(f"Duplicate ChapterID {chapter_id} in chapter renewal notice email.")
        seen.add(chapter_id)
        deduped.append(row)
    return deduped
