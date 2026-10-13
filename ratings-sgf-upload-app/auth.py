from __future__ import annotations

import base64
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Mapping

import storage


LOGGER = logging.getLogger("ratings_sgf_upload.auth")


EMAIL_CLAIMS = {
    "email", "emails", "preferred_username", "upn", "unique_name",
    "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
    "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name",
}
OBJECT_ID_CLAIMS = {
    "oid", "objectidentifier",
    "http://schemas.microsoft.com/identity/claims/objectidentifier",
    "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/nameidentifier",
}


@dataclass(frozen=True)
class Authorization:
    ok: bool
    status_code: int
    error: str | None = None
    principal_name: str | None = None
    principal_id: str | None = None
    identity_provider: str | None = None
    permission_code: str | None = None


def _header(headers: Mapping[str, Any], name: str) -> str | None:
    lowered = name.lower()
    for key, value in headers.items():
        if str(key).lower() == lowered and str(value).strip():
            return str(value).strip()
    return None


def _principal(headers: Mapping[str, Any]) -> tuple[str | None, str | None, str | None, tuple[str, ...]]:
    raw = _header(headers, "X-MS-CLIENT-PRINCIPAL")
    payload: dict[str, Any] = {}
    if raw:
        try:
            payload = json.loads(base64.b64decode(raw + "=" * (-len(raw) % 4)).decode("utf-8"))
        except Exception:
            payload = {}
    claims: dict[str, list[str]] = {}
    for item in payload.get("claims", []) if isinstance(payload, dict) else []:
        if not isinstance(item, dict):
            continue
        claim_type = str(item.get("typ") or item.get("type") or "").strip().lower()
        value = str(item.get("val") or item.get("value") or "").strip()
        if claim_type and value:
            claims.setdefault(claim_type, []).append(value)
    names: list[str] = []
    for claim_type in EMAIL_CLAIMS:
        names.extend(claims.get(claim_type, []))
    for value in (
        payload.get("userDetails") if isinstance(payload, dict) else None,
        _header(headers, "X-MS-CLIENT-PRINCIPAL-NAME"),
        os.environ.get("SGF_UPLOAD_DEV_AUTH_EMAIL") if not os.environ.get("WEBSITE_SITE_NAME") else None,
    ):
        if value and str(value).strip():
            names.append(str(value).strip())
    unique_names = tuple(dict.fromkeys(value.lower() for value in names))
    principal_name = names[0] if names else None
    principal_id = _header(headers, "X-MS-CLIENT-PRINCIPAL-ID")
    if not principal_id:
        for claim_type in OBJECT_ID_CLAIMS:
            values = claims.get(claim_type, [])
            if values:
                principal_id = values[0]
                break
    provider = str(payload.get("identityProvider") or payload.get("auth_typ") or "").strip() or None
    return principal_name, principal_id, provider, unique_names


def authorize(headers: Mapping[str, Any]) -> Authorization:
    if os.environ.get("WEBSITE_SITE_NAME") and os.environ.get("BAYRATE_TRUST_EASY_AUTH", "").lower() not in {"1", "true", "yes", "on"}:
        return Authorization(False, 503, "Easy Auth trust is not configured.")
    principal_name, principal_id, provider, names = _principal(headers)
    if not principal_name and not principal_id:
        return Authorization(False, 401, "Sign in is required before using SGF Upload.")
    where: list[str] = []
    params: list[Any] = ["sgf_import", "admin_all"]
    if names:
        where.append("LOWER([Principal_Name]) IN (" + ",".join("?" for _ in names) + ")")
        params.extend(names)
    if principal_id:
        where.append("[Principal_Id] = ?")
        params.append(principal_id)
    if not where:
        return Authorization(False, 403, "The signed-in account has no SGF import permission.", principal_name, principal_id, provider)
    try:
        rows = storage.query_rows(
            f"""
SELECT TOP (1) [Permission_Code]
FROM [ratings].[admin_permissions]
WHERE [Is_Active] = 1
  AND [Permission_Code] IN (?, ?)
  AND ({' OR '.join(where)})
ORDER BY CASE WHEN [Permission_Code] = 'sgf_import' THEN 0 ELSE 1 END
""",
            params,
        )
    except Exception:
        LOGGER.exception("SGF import authorization SQL lookup failed")
        return Authorization(False, 503, "SGF import authorization could not be verified.", principal_name, principal_id, provider)
    if not rows:
        return Authorization(False, 403, f"{principal_name or principal_id} does not have sgf_import permission.", principal_name, principal_id, provider)
    return Authorization(True, 200, principal_name=principal_name, principal_id=principal_id, identity_provider=provider, permission_code=str(rows[0]["Permission_Code"]))


def public_payload(result: Authorization) -> dict[str, Any]:
    return {
        "principal_name": result.principal_name,
        "principal_id": result.principal_id,
        "identity_provider": result.identity_provider,
        "permission_code": result.permission_code,
    }
