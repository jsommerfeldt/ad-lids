# auth.py
from __future__ import annotations

import base64
import json
import logging

import msal

from modules.config import Config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class AuthProvider:
    """
    Auth provider for app-only (client credentials) tokens using MSAL.
    """

    def __init__(self, cfg: Config):
        self.cfg = cfg

    def acquire_token(self) -> str:
        app = msal.ConfidentialClientApplication(
            self.cfg.CLIENT_ID,
            authority=f"https://login.microsoftonline.com/{self.cfg.TENANT_ID}",
            client_credential=self.cfg.CLIENT_SECRET,
        )
        result = app.acquire_token_for_client(scopes=self.cfg.SCOPE)
        if "access_token" not in result:
            raise RuntimeError(f"Auth failed: {result}")
        token = result["access_token"]

        # Optional: log roles for debugging
        try:
            _, payload_b64, _ = token.split(".")
            payload = json.loads(
                base64.urlsafe_b64decode(payload_b64 + "=" * (-len(payload_b64) % 4))
            )
            logger.info("Token roles: %s", payload.get("roles"))
        except Exception:
            pass

        return token