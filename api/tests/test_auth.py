from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from api.auth import configured_api_keys, require_api_key, resolve_principal
from api.models import AccountRole


class ApiAuthTests(unittest.TestCase):
    def setUp(self) -> None:
        self.prev_api_key = os.environ.get("MX8_API_KEY")
        self.prev_api_keys = os.environ.get("MX8_API_KEYS")

    def tearDown(self) -> None:
        if self.prev_api_key is None:
            os.environ.pop("MX8_API_KEY", None)
        else:
            os.environ["MX8_API_KEY"] = self.prev_api_key
        if self.prev_api_keys is None:
            os.environ.pop("MX8_API_KEYS", None)
        else:
            os.environ["MX8_API_KEYS"] = self.prev_api_keys

    def test_configured_api_keys_supports_single_key(self) -> None:
        os.environ["MX8_API_KEY"] = "secret-token"
        os.environ.pop("MX8_API_KEYS", None)

        configured = configured_api_keys()
        self.assertEqual(set(configured), {"secret-token"})
        self.assertEqual(configured["secret-token"].account_id, "default")
        self.assertEqual(configured["secret-token"].role, AccountRole.CUSTOMER)

    def test_configured_api_keys_supports_multiple_keys(self) -> None:
        os.environ.pop("MX8_API_KEY", None)
        os.environ["MX8_API_KEYS"] = "cust-acme=customer-key,op:internal=operator-key"

        configured = configured_api_keys()
        self.assertEqual(set(configured), {"customer-key", "operator-key"})
        self.assertEqual(configured["customer-key"].account_id, "cust-acme")
        self.assertEqual(configured["customer-key"].role, AccountRole.CUSTOMER)
        self.assertEqual(configured["operator-key"].account_id, "op:internal")
        self.assertEqual(configured["operator-key"].role, AccountRole.OPERATOR)

    def test_require_api_key_allows_requests_when_auth_is_disabled(self) -> None:
        os.environ.pop("MX8_API_KEY", None)
        os.environ.pop("MX8_API_KEYS", None)

        require_api_key(object(), None)
        principal = resolve_principal(object(), None)
        self.assertEqual(principal.account_id, "local-dev")
        self.assertEqual(principal.role, AccountRole.OPERATOR)

    def test_require_api_key_rejects_missing_or_incorrect_token(self) -> None:
        os.environ["MX8_API_KEY"] = "secret-token"
        os.environ.pop("MX8_API_KEYS", None)

        with self.assertRaises(HTTPException) as missing:
            require_api_key(object(), None)
        with self.assertRaises(HTTPException) as wrong:
            require_api_key(
                object(),
                HTTPAuthorizationCredentials(scheme="Bearer", credentials="wrong"),
            )

        self.assertEqual(missing.exception.status_code, 401)
        self.assertEqual(wrong.exception.status_code, 401)

    def test_require_api_key_accepts_valid_token(self) -> None:
        os.environ["MX8_API_KEY"] = "secret-token"
        os.environ.pop("MX8_API_KEYS", None)

        require_api_key(
            object(),
            HTTPAuthorizationCredentials(scheme="Bearer", credentials="secret-token"),
        )
        principal = resolve_principal(
            object(),
            HTTPAuthorizationCredentials(scheme="Bearer", credentials="secret-token"),
        )
        self.assertEqual(principal.account_id, "default")
        self.assertEqual(principal.role, AccountRole.CUSTOMER)


if __name__ == "__main__":
    unittest.main()
