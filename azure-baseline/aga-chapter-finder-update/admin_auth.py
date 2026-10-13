# Copyright 2026, American Go Association, All rights reserved

"""Azure Easy Auth plus scoped SQL authorization for portal reviewers."""

from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from typing import Any, Mapping


CHAPTER_FINDER_UPDATES_PERMISSION = "chapter_finder_updates"
ADMIN_ALL_PERMISSION = "admin_all"
EMAIL_CLAIMS = {
    "email",
    "emails",
    "preferred_username",
    "upn",
    "unique_name",
    "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
    "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name",
}
OBJECT_ID_CLAIMS = {
    "oid",
    "objectidentifier",
    "http://schemas.microsoft.com/identity/claims/objectidentifier",
    "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/nameidentifier",
}


@dataclass(frozen=True)
class Principal:
    """Represent an authenticated portal reviewer."""

    principal_name: str
    principal_id: str | None
    display_name: str | None
    identity_provider: str | None
    lookup_names: tuple[str, ...]


@dataclass(frozen=True)
class AuthorizationResult:
    """Represent scoped authorization outcome."""

    ok: bool
    status_code: int
    error: str | None = None
    principal: Principal | None = None
    admin_row: dict[str, Any] | None = None


def authorize_reviewer(headers: Mapping[str, Any], adapter: Any) -> AuthorizationResult:
    """Require Easy Auth and chapter_finder_updates or admin_all permission."""
    if not _runtime_configured():
        return AuthorizationResult(
            ok=False,
            status_code=503,
            error="Reviewer authorization is not configured.",
        )
    principal = extract_principal(headers)
    if principal is None:
        return AuthorizationResult(ok=False, status_code=401, error="Sign in is required.")
    row = find_permission(adapter, principal)
    if row is None:
        return AuthorizationResult(
            ok=False,
            status_code=403,
            error=f"{principal.principal_name} does not have Chapter Finder review permission.",
            principal=principal,
        )
    return AuthorizationResult(ok=True, status_code=200, principal=principal, admin_row=row)


def extract_principal(headers: Mapping[str, Any]) -> Principal | None:
    """Extract the signed-in identity from Azure Easy Auth headers."""
    payload = _decode_principal(_header(headers, "X-MS-CLIENT-PRINCIPAL"))
    claims = _claims_by_type(payload.get("claims") if payload else None)
    header_name = _clean(_header(headers, "X-MS-CLIENT-PRINCIPAL-NAME"))
    principal_name = (
        _first_claim(claims, EMAIL_CLAIMS)
        or _clean(payload.get("userDetails") if payload else None)
        or header_name
        or _dev_email()
    )
    principal_id = (
        _first_claim(claims, OBJECT_ID_CLAIMS)
        or _clean(_header(headers, "X-MS-CLIENT-PRINCIPAL-ID"))
    )
    if not principal_name and not principal_id:
        return None
    if not principal_name:
        principal_name = principal_id or "unknown"
    lookup = tuple(
        dict.fromkeys(
            item
            for item in (
                principal_name,
                header_name,
                _clean(payload.get("userDetails") if payload else None),
            )
            if item
        )
    )
    return Principal(
        principal_name=principal_name,
        principal_id=principal_id,
        display_name=header_name or principal_name,
        identity_provider=(
            _clean(payload.get("identityProvider") if payload else None)
            or _clean(_header(headers, "X-MS-CLIENT-PRINCIPAL-IDP"))
        ),
        lookup_names=lookup,
    )


def find_permission(adapter: Any, principal: Principal) -> dict[str, Any] | None:
    """Find a current scoped permission row."""
    where_parts: list[str] = []
    params: list[Any] = []
    if principal.lookup_names:
        placeholders = ", ".join("?" for _ in principal.lookup_names)
        where_parts.append(f"LOWER([Principal_Name]) IN ({placeholders})")
        params.extend(value.lower() for value in principal.lookup_names)
    if principal.principal_id:
        where_parts.append("[Principal_Id] = ?")
        params.append(principal.principal_id)
    if not where_parts:
        return None
    rows = adapter.query_rows(
        f"""
SELECT TOP (1)
    [AdminPermissionID], [Principal_Name], [Principal_Id], [Display_Name], [Permission_Code]
FROM [ratings].[admin_permissions]
WHERE [Is_Active] = 1
  AND [Permission_Code] IN (?, ?)
  AND ({' OR '.join(where_parts)})
ORDER BY CASE WHEN [Permission_Code] = ? THEN 0 ELSE 1 END, [AdminPermissionID];
""",
        (CHAPTER_FINDER_UPDATES_PERMISSION, ADMIN_ALL_PERMISSION, *params, CHAPTER_FINDER_UPDATES_PERMISSION),
    )
    return rows[0] if rows else None


def _runtime_configured() -> bool:
    return _truthy(os.environ.get("BAYRATE_TRUST_EASY_AUTH")) or (
        not os.environ.get("WEBSITE_SITE_NAME") and bool(_dev_email())
    )


def _dev_email() -> str | None:
    value = os.environ.get("BAYRATE_DEV_AUTH_EMAIL")
    return value.strip() if value and value.strip() else None


def _decode_principal(value: str | None) -> dict[str, Any] | None:
    if not value:
        return None
    try:
        padded = value + ("=" * (-len(value) % 4))
        decoded = base64.b64decode(padded).decode("utf-8")
        payload = json.loads(decoded)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _claims_by_type(values: Any) -> dict[str, list[str]]:
    claims: dict[str, list[str]] = {}
    if not isinstance(values, list):
        return claims
    for item in values:
        if not isinstance(item, dict):
            continue
        claim_type = _clean(item.get("typ"))
        claim_value = _clean(item.get("val"))
        if claim_type and claim_value:
            claims.setdefault(claim_type.lower(), []).append(claim_value)
    return claims


def _first_claim(claims: dict[str, list[str]], claim_types: set[str]) -> str | None:
    for claim_type in claim_types:
        values = claims.get(claim_type.lower())
        if values:
            return values[0]
    return None


def _header(headers: Mapping[str, Any], name: str) -> str | None:
    try:
        value = headers.get(name)
    except Exception:
        value = None
    if value is not None:
        return _clean(value)
    lowered = name.lower()
    try:
        for key, item_value in headers.items():
            if str(key).lower() == lowered:
                return _clean(item_value)
    except Exception:
        pass
    return None


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
