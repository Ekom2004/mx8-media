from __future__ import annotations

import os

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .models import AccountRole, AuthPrincipal

bearer_scheme = HTTPBearer(auto_error=False)


def require_api_key(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> None:
    resolve_principal(request, credentials)


def resolve_principal(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> AuthPrincipal:
    del request
    configured = configured_api_keys()
    if not configured:
        return AuthPrincipal(account_id="local-dev", role=AccountRole.OPERATOR)
    token = None if credentials is None else credentials.credentials.strip()
    principal = None if token is None else configured.get(token)
    if principal is not None:
        return principal
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="unauthorized",
        headers={"WWW-Authenticate": "Bearer"},
    )


def configured_api_keys() -> dict[str, AuthPrincipal]:
    raw = os.getenv("MX8_API_KEYS", "").strip()
    if raw:
        parsed: dict[str, AuthPrincipal] = {}
        for part in raw.split(","):
            normalized = part.strip()
            if not normalized:
                continue
            if "=" in normalized:
                account_id, token = normalized.split("=", 1)
                account = account_id.strip()
                secret = token.strip()
                if not account or not secret:
                    continue
                parsed[secret] = AuthPrincipal(account_id=account, role=_role_for_account(account))
            else:
                parsed[normalized] = AuthPrincipal(
                    account_id="default",
                    role=AccountRole.CUSTOMER,
                )
        return parsed
    single = os.getenv("MX8_API_KEY", "").strip()
    if single:
        return {single: AuthPrincipal(account_id="default", role=AccountRole.CUSTOMER)}
    return {}


def _role_for_account(account_id: str) -> AccountRole:
    normalized = account_id.strip().lower()
    if normalized.startswith("op:") or normalized == "operator":
        return AccountRole.OPERATOR
    return AccountRole.CUSTOMER
