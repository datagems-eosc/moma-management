import logging
import threading
import time

import requests
from jose import jwt

logger = logging.getLogger(__name__)


class Authentication:
    """
    """

    def __init__(self, issuer: str, audience: str | None = None, ttl: int = 300) -> None:
        self._issuer = issuer
        self._audience = audience
        self._ttl = ttl
        self._lock = threading.Lock()
        self._jwks: dict | None = None
        self._fetched_at: float = 0.0

    def _get_jwks(self) -> dict:
        """Return the JWKS, refreshing the cache when stale."""
        now = time.monotonic()
        with self._lock:
            if self._jwks is None or (now - self._fetched_at) > self._ttl:
                url = f"{self._issuer}/protocol/openid-connect/certs"
                logger.info("Refreshing JWKS from %s", url)
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()
                self._jwks = resp.json()
                self._fetched_at = now
        return self._jwks

    def validate(self, token: str) -> dict:
        """Decode and validate *token*.

        Verifies signature (RS256), issuer, and audience (when configured).
        Raises ``jose.JWTError`` on any validation failure.
        """
        return jwt.decode(
            token,
            self._get_jwks(),
            algorithms=["RS256"],
            issuer=self._issuer,
            audience=self._audience,
            options={"verify_aud": self._audience is not None},
        )
