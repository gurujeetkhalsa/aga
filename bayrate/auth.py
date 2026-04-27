from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from typing import Any, Mapping


EMAIL_CLAIM_TYPES = {
    "email",
    "emails",
    "preferred_username",
    "upn",
    "unique_name",
    "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
    "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name",
}
NAME_CLAIM_TYPES = {
    "name",
    "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname",
}
OBJECT_ID_CLAIM_TYPES = {
    "oid",
    "objectidentifier",
    "http://schemas.microsoft.com/identity/claims/objectidentifier",
    "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/nameidentifier",
}
ROLE_CLAIM_TYPES = {
    "roles",
    "role",
    "http://schemas.microsoft.com/ws/2008/06/identity/claims/role",
}


@dataclass(frozen=True)
class BayRatePrincipal:
    principal_name: str
    principal_id: str | None
    identity_provider: str | None
    display_name: str | None
    lookup_names: tuple[str, ...]
    roles: tuple[str, ...]
    claims: dict[str, tuple[str, ...]]


@dataclass(frozen=True)
class BayRateAuthorizationResult:
    ok: bool
    status_code: int
    error: str | None = None
    principal: BayRatePrincipal | None = None
    admin_row: dict[str, Any] | None = None


def authorize_bayrate_admin(
    headers: Mapping[str, Any],
    adapter: Any,
    *,
    environ: Mapping[str, str] | None = None,
) -> BayRateAuthorizationResult:
    env = os.environ if environ is None else environ
    if not bayrate_auth_runtime_configured(env):
        return BayRateAuthorizationResult(
            ok=False,
            status_code=503,
            error=(
                "BayRate authorization is not configured. Enable Azure App Service Authentication "
                "and set BAYRATE_TRUST_EASY_AUTH=true, or set BAYRATE_DEV_AUTH_EMAIL for local development."
            ),
        )

    principal = extract_bayrate_principal(headers, environ=env)
    if principal is None:
        return BayRateAuthorizationResult(
            ok=False,
            status_code=401,
            error="Sign in is required before running BayRate.",
        )

    try:
        admin_row = find_bayrate_admin(adapter, principal)
    except Exception as exc:
        message = str(exc)
        if "bayrate_admins" in message.lower() or "invalid object name" in message.lower():
            return BayRateAuthorizationResult(
                ok=False,
                status_code=503,
                error="BayRate admin authorization table is missing. Apply bayrate/sql/bayrate_authorization_schema.sql.",
                principal=principal,
            )
        return BayRateAuthorizationResult(
            ok=False,
            status_code=503,
            error="BayRate admin authorization could not be verified.",
            principal=principal,
        )

    if admin_row is None:
        return BayRateAuthorizationResult(
            ok=False,
            status_code=403,
            error=f"{principal.principal_name} is not an active BayRate admin.",
            principal=principal,
        )

    return BayRateAuthorizationResult(ok=True, status_code=200, principal=principal, admin_row=admin_row)


def bayrate_auth_runtime_configured(environ: Mapping[str, str] | None = None) -> bool:
    env = os.environ if environ is None else environ
    if _truthy(env.get("BAYRATE_TRUST_EASY_AUTH")):
        return True
    if not env.get("WEBSITE_SITE_NAME") and _dev_auth_email(env):
        return True
    return False


def extract_bayrate_principal(
    headers: Mapping[str, Any] | None,
    *,
    environ: Mapping[str, str] | None = None,
) -> BayRatePrincipal | None:
    env = os.environ if environ is None else environ
    if headers is None:
        headers = {}

    client_principal = _decode_client_principal(_header(headers, "X-MS-CLIENT-PRINCIPAL"))
    claims = _claims_by_type(client_principal.get("claims") if client_principal else None)

    header_name = _clean_text(_header(headers, "X-MS-CLIENT-PRINCIPAL-NAME"))
    principal_name = (
        _first_claim(claims, EMAIL_CLAIM_TYPES)
        or _clean_text(client_principal.get("userDetails") if client_principal else None)
        or header_name
        or _dev_auth_email(env)
    )
    principal_id = (
        _first_claim(claims, OBJECT_ID_CLAIM_TYPES)
        or _clean_text(_header(headers, "X-MS-CLIENT-PRINCIPAL-ID"))
    )
    display_name = _first_claim(claims, NAME_CLAIM_TYPES) or header_name or principal_name
    identity_provider = (
        _clean_text(client_principal.get("identityProvider") if client_principal else None)
        or _clean_text(client_principal.get("auth_typ") if client_principal else None)
        or _clean_text(_header(headers, "X-MS-CLIENT-PRINCIPAL-IDP"))
    )
    roles = _claim_values(claims, ROLE_CLAIM_TYPES)
    lookup_names = _unique_clean_values(
        (
            principal_name,
            header_name,
            _first_claim(claims, EMAIL_CLAIM_TYPES),
            _clean_text(client_principal.get("userDetails") if client_principal else None),
        )
    )

    if not principal_name and not principal_id:
        return None
    if not principal_name:
        principal_name = principal_id or "unknown"

    return BayRatePrincipal(
        principal_name=principal_name,
        principal_id=principal_id,
        identity_provider=identity_provider,
        display_name=display_name,
        lookup_names=lookup_names,
        roles=roles,
        claims=claims,
    )


def find_bayrate_admin(adapter: Any, principal: BayRatePrincipal) -> dict[str, Any] | None:
    where_parts: list[str] = []
    params: list[Any] = []

    lookup_names = principal.lookup_names or ((principal.principal_name,) if principal.principal_name else ())
    if lookup_names:
        placeholders = ", ".join("?" for _ in lookup_names)
        where_parts.append(f"LOWER([Principal_Name]) IN ({placeholders})")
        params.extend(value.lower() for value in lookup_names)

    if principal.principal_id:
        where_parts.append("[Principal_Id] = ?")
        params.append(principal.principal_id)

    if not where_parts:
        return None

    rows = adapter.query_rows(
        f"""
SELECT TOP (1)
    [AdminID],
    [Principal_Name],
    [Principal_Id],
    [Display_Name],
    [Created_At]
FROM [ratings].[bayrate_admins]
WHERE [Is_Active] = 1
  AND ({' OR '.join(where_parts)})
ORDER BY [AdminID]
""",
        tuple(params),
    )
    return rows[0] if rows else None


def _header(headers: Mapping[str, Any], name: str) -> str | None:
    try:
        value = headers.get(name)  # type: ignore[attr-defined]
    except Exception:
        value = None
    if value is not None:
        return _clean_text(value)

    lowered = name.lower()
    try:
        items = headers.items()
    except Exception:
        return None
    for key, item_value in items:
        if str(key).lower() == lowered:
            return _clean_text(item_value)
    return None


def _decode_client_principal(value: str | None) -> dict[str, Any] | None:
    if not value:
        return None
    try:
        padded = value + ("=" * (-len(value) % 4))
        decoded = base64.b64decode(padded).decode("utf-8")
        payload = json.loads(decoded)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _claims_by_type(raw_claims: Any) -> dict[str, tuple[str, ...]]:
    claims: dict[str, list[str]] = {}
    if not isinstance(raw_claims, list):
        return {}
    for item in raw_claims:
        if not isinstance(item, dict):
            continue
        claim_type = _clean_text(item.get("typ") or item.get("type"))
        value = _clean_text(item.get("val") or item.get("value"))
        if not claim_type or not value:
            continue
        claims.setdefault(claim_type.lower(), []).append(value)
    return {key: tuple(values) for key, values in claims.items()}


def _first_claim(claims: dict[str, tuple[str, ...]], claim_types: set[str]) -> str | None:
    values = _claim_values(claims, claim_types)
    return values[0] if values else None


def _claim_values(claims: dict[str, tuple[str, ...]], claim_types: set[str]) -> tuple[str, ...]:
    values: list[str] = []
    lowered_types = {claim_type.lower() for claim_type in claim_types}
    for claim_type, claim_values in claims.items():
        if claim_type not in lowered_types:
            continue
        values.extend(claim_values)
    return _unique_clean_values(values)


def _unique_clean_values(values: Any) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _clean_text(value)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return tuple(result)


def _dev_auth_email(environ: Mapping[str, str]) -> str | None:
    return _clean_text(environ.get("BAYRATE_DEV_AUTH_EMAIL") or environ.get("BAYRATE_DEV_PRINCIPAL_NAME"))


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
