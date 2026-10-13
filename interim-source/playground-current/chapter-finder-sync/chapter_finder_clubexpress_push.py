# Copyright 2026, American Go Association, All rights reserved

"""Translate an approved Chapter Finder payload into safe ClubExpress edits."""

from __future__ import annotations

from typing import Any


CLUB_ID = 454497

US_STATE_LABELS = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}

DIRECT_CONTROLS = {
    "chapter_name": ("ctl00_subgroup_name_text", "fill", 50),
    "city": ("ctl00_address_panel_city_text", "fill", 30),
    "postal_code": ("ctl00_address_panel_zip_text", "fill", 10),
    "contact_name": ("ctl00_contact_name_text", "fill", None),
    "contact_email": ("ctl00_email_text", "fill", 100),
    "contact_phone": ("ctl00_phone_text", "fill", 24),
    "website_url": ("ctl00_website_text", "fill", 100),
    "description_text": ("ctl00_directory_text", "fill", 500),
}

ADDRESS_FIELDS = {"address_text", "city", "state_province", "postal_code"}
PRESERVED_CONTROLS = (
    "ctl00_subgroup_number_text",
    "ctl00_status_dropdown",
    "ctl00_committee_dropdown",
    "ctl00_account_dropdown",
    "ctl00_create_channel_checkbox",
    "ctl00_committee_members_from_this_subgroup_only",
    "ctl00_approval_check",
    "ctl00_notices_checkbox",
    "ctl00_address_panel_country_dropdown",
    "ctl00_address_panel_timezone_dropdown",
    "ctl00_contact_title_text",
    "ctl00_fax_text",
    "ctl00_subgroup_check_payable_text",
    "ctl00_subgroup_subgroup_payment_address_text",
    "ctl00_custom_page_category_dropdown",
    "ctl00_custom_page_dropdown",
    "ctl00_notes_text",
)


class PushPlanError(ValueError):
    """Represent an approved payload that cannot be applied without review."""


def build_push_plan(
    *,
    chapter_finder_id: int,
    normalized_payload: dict[str, Any],
    field_diffs: list[dict[str, Any]],
    request_id: int | None = None,
) -> dict[str, Any]:
    """Build exact ClubExpress controls and administrator reconciliation steps."""
    changed = {str(item.get("field_name")) for item in field_diffs}
    writes: list[dict[str, Any]] = []
    derived_fields: list[str] = []
    unsupported_fields: list[str] = []

    for field, (control_id, action, maximum) in DIRECT_CONTROLS.items():
        if field not in changed:
            continue
        value = text_value(normalized_payload.get(field))
        validate_length(field, value, maximum)
        writes.append(control_write(field, control_id, action, value))

    if "address_text" in changed:
        address1, address2 = split_address(normalized_payload.get("address_text"))
        writes.extend(
            [
                control_write("address_text", "ctl00_address_panel_address1_text", "fill", address1),
                control_write("address_text", "ctl00_address_panel_address2_text", "fill", address2),
            ]
        )

    if "state_province" in changed:
        state_label = clubexpress_state_label(normalized_payload.get("state_province"))
        writes.extend(
            [
                control_write("state_province", "ctl00_parent_subgroup_dropdown", "select_label", state_label),
                control_write("state_province", "ctl00_address_panel_state_dropdown", "select_label", state_label),
            ]
        )

    if "postal_text" in changed:
        if changed.intersection({"city", "state_province", "postal_code"}):
            derived_fields.append("postal_text")
        else:
            unsupported_fields.append("postal_text")

    if "website_label" in changed:
        unsupported_fields.append("website_label")

    if changed.intersection(ADDRESS_FIELDS):
        writes.extend(
            [
                control_write("geocoding", "ctl00_latitude", "fill", "", "Regenerate after address change"),
                control_write("geocoding", "ctl00_longitude", "fill", "", "Regenerate after address change"),
            ]
        )

    if unsupported_fields:
        names = ", ".join(sorted(unsupported_fields))
        raise PushPlanError(f"No writable ClubExpress control exists for changed field(s): {names}.")

    administrators = normalized_payload.get("administrators")
    administrator_plan = None
    if "administrators" in changed:
        if not isinstance(administrators, list):
            raise PushPlanError("The approved administrator payload is missing or invalid.")
        administrator_plan = build_administrator_plan(chapter_finder_id, administrators)

    return {
        "request_id": request_id,
        "chapter_finder_id": chapter_finder_id,
        "edit_url": (
            f"https://usgo.org/popup.aspx?page_id=382&club_id={CLUB_ID}"
            f"&item_id={chapter_finder_id}&action=edit"
        ),
        "control_writes": writes,
        "derived_fields": derived_fields,
        "preserve_control_ids": list(PRESERVED_CONTROLS),
        "administrator_plan": administrator_plan,
        "save_control_id": "ctl00_save_button",
        "safety": {
            "require_current_value_check": True,
            "require_confirmation_immediately_before_save": True,
            "never_store_credentials_or_browser_session": True,
            "complete_attempt_only_after_clubexpress_save": True,
            "run_public_sync_after_save": True,
        },
    }


def build_administrator_plan(chapter_finder_id: int, administrators: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the separate ClubExpress administrator reconciliation plan."""
    desired = []
    for item in administrators:
        if not isinstance(item, dict):
            raise PushPlanError("An approved administrator entry is invalid.")
        display_name = text_value(item.get("display_name"))
        if not display_name:
            raise PushPlanError("An approved administrator is missing a display name.")
        agaid = item.get("membership_agaid")
        desired.append({"display_name": display_name, "membership_agaid": int(agaid) if agaid else None})
    return {
        "action": "reconcile",
        "popup_url": (
            f"https://usgo.org/popup.aspx?page_id=383&club_id={CLUB_ID}"
            f"&item_id={chapter_finder_id}"
        ),
        "desired_administrators": desired,
        "match_existing_by": "display_name",
        "add_new_by": "membership_agaid",
        "require_confirmation_before_add_or_remove": True,
    }


def split_address(value: Any) -> tuple[str, str]:
    """Split the public street address into ClubExpress Address 1 and Address 2."""
    text = text_value(value)
    if not text:
        return "", ""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) > 2:
        raise PushPlanError("Street or meeting address must use at most two non-empty lines.")
    while len(lines) < 2:
        lines.append("")
    for line in lines:
        validate_length("address line", line, 50)
    return lines[0], lines[1]


def clubexpress_state_label(value: Any) -> str:
    """Return the ClubExpress state option label for an abbreviation or full name."""
    state = text_value(value).strip()
    if not state:
        raise PushPlanError("State is required when changing the chapter state.")
    abbreviation = state.upper()
    if abbreviation in US_STATE_LABELS:
        return US_STATE_LABELS[abbreviation]
    for label in US_STATE_LABELS.values():
        if state.casefold() == label.casefold():
            return label
    raise PushPlanError(f"ClubExpress state option was not recognized: {state}")


def control_write(
    field: str,
    control_id: str,
    action: str,
    value: str,
    reason: str | None = None,
) -> dict[str, Any]:
    """Build one explicit browser control write."""
    result = {"field": field, "control_id": control_id, "action": action, "value": value}
    if reason:
        result["reason"] = reason
    return result


def text_value(value: Any) -> str:
    """Normalize nullable JSON values for browser form controls."""
    if value is None:
        return ""
    return str(value).replace("\r\n", "\n").replace("\r", "\n").strip()


def validate_length(field: str, value: str, maximum: int | None) -> None:
    """Reject values that ClubExpress would truncate or refuse."""
    if maximum is not None and len(value) > maximum:
        raise PushPlanError(f"{field.replace('_', ' ').title()} exceeds ClubExpress's {maximum}-character limit.")
