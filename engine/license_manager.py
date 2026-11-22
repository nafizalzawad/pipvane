"""
Pipvane v3 Local Licensing Client & Lifecycle Manager (Sprint 8 / FR-8.1 & FR-8.4).

Manages offline cryptographic JWT validation, entitlement enforcement, and silent 7-day refresh:
1. Token Validation:
   - Reads signed JWT from SecureVault (DPAPI-encrypted SQLite).
   - Verifies cryptographic signature using bundled public key (RS256, EdDSA/Ed25519, HS256).
   - Checks strict 30-day expiration window offline without external network dependencies.
2. Entitlement Parsing:
   - Extracts license_id, account_limit (e.g. 7 terminals for Pro, 4 for Standard), tier, and hardware fingerprint.
   - Enforces account limits against local engine terminals.
3. Silent Refresh Loop:
   - Background daemon that triggers every 7 days (or on startup if 7 days elapsed) to refresh the lease.
   - On success: Overwrites token in the SecureVault.
   - On failure: Logs warning but permits trading to continue until the strict 30-day exp is reached.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from engine.vault import VaultStorage

logger = logging.getLogger("pipvane.engine.licensing")


# =============================================================================
# CONSTANTS & VAULT KEYS
# =============================================================================

VAULT_LICENSE_KEY = "license_jwt"
VAULT_LAST_REFRESH_KEY = "license_last_refresh_timestamp"

SEVEN_DAYS_SECONDS = 7 * 24 * 60 * 60       # 604,800 seconds
THIRTY_DAYS_SECONDS = 30 * 24 * 60 * 60    # 2,592,000 seconds
CLOCK_TOLERANCE_SECONDS = 60.0             # 60 seconds tolerance for skew

DEFAULT_BUNDLED_RSA_PUBLIC_KEY = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAnedaAYS/OevvPCcJtIxr
Hn0dCP6X3vnovxnfIQSKxjAKuIPopA0HJ/S/fcyPO6yvSnKBcr3ZXBkUHERWWCr3
LTkhPP8jnWBiiT55oFeR/8TsLJbcwLPJwfMSyKREqBHmP+sxmZvGyyK1OHqmN4U8
A4or4WKikp+LJ/oh8wF+kRtxTDjb1GjTKMAsTkeLm+zhXrRy6fMLiUd5bffCeAPR
dk8tsCRavebaK6IML5mUiLx+Aj9X+yCIoLbGQwGmgR4CVBA2U+U2Yfzq6WA/dmaS
jltsny6Zq5CAAugrBzT+cy6aGMFy41Oxxlx507F2nSkcj4rhCrcivRMpVNjCgy5x
mQIDAQAB
-----END PUBLIC KEY-----"""

DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX = (
    "c294db46b78e366eb8b680dfcbddda00b925015b40d67adfc867594a19e6d4a6"
)


# =============================================================================
# EXCEPTIONS & DATA STRUCTURES
# =============================================================================

class LicenseError(Exception):
    """Base exception for all licensing errors."""
    pass


class LicenseValidationError(LicenseError):
    """Raised when token signature or payload fails cryptographic validation."""
    pass


class LicenseExpiredError(LicenseError):
    """Raised when license token has expired or offline grace period exceeded."""
    def __init__(
        self,
        message: str = "LICENSE_EXPIRED: 30-day offline grace period exceeded",
        details: Optional[Dict[str, Any]] = None,
        *args: Any,
    ) -> None:
        super().__init__(message, *args)
        self.code = "LICENSE_EXPIRED"
        self.details = details or {}


class LicenseAccountLimitExceededError(LicenseError):
    """Raised when active terminal count exceeds the license account_limit."""
    pass


class LicenseValidationStatus(str, Enum):
    VALID = "VALID"
    EXPIRED = "EXPIRED"
    INVALID_SIGNATURE = "INVALID_SIGNATURE"
    MALFORMED = "MALFORMED"
    NOT_FOUND = "NOT_FOUND"
    ACCOUNT_LIMIT_EXCEEDED = "ACCOUNT_LIMIT_EXCEEDED"


@dataclass(frozen=True)
class LicenseEntitlements:
    """
    Parsed licensing entitlements extracted from signed cryptographic JWT.
    """
    license_id: str
    account_limit: int = 7
    tier: str = "Pro"
    device_fingerprint: str = ""
    issued_at: float = 0.0
    expires_at: float = 0.0
    jti: str = ""
    features: List[str] = field(default_factory=list)
    raw_payload: Dict[str, Any] = field(default_factory=dict)

    def is_expired(self, current_time: Optional[float] = None) -> bool:
        now = current_time if current_time is not None else self.get_current_time()
        return now >= self.expires_at

    def days_remaining(self, current_time: Optional[float] = None) -> float:
        now = current_time if current_time is not None else self.get_current_time()
        return max(0.0, (self.expires_at - now) / 86400.0)


@dataclass(frozen=True)
class LicenseValidationResult:
    """
    Diagnostic outcome of an offline license validation check.
    """
    is_valid: bool
    status: LicenseValidationStatus
    reason: Optional[str] = None
    entitlements: Optional[LicenseEntitlements] = None
    days_remaining: float = 0.0
    needs_refresh: bool = False
    evaluated_at: float = field(default_factory=time.time)

    def __bool__(self) -> bool:
        return self.is_valid


# =============================================================================
# BASE64URL & PURE PYTHON CRYPTOGRAPHY (ZERO DEPENDENCIES)
# =============================================================================

def base64url_encode(data: bytes | str) -> str:
    """Base64URL encodes data without padding (RFC 7515)."""
    raw = data.encode("utf-8") if isinstance(data, str) else data
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def base64url_decode(s: str) -> bytes:
    """Base64URL decodes string with automatic padding restoration."""
    padding = (4 - len(s) % 4) % 4
    return base64.urlsafe_b64decode(s + "=" * padding)


def base64url_decode_json(s: str) -> Dict[str, Any]:
    """Decodes a base64url string directly into a JSON dictionary."""
    return json.loads(base64url_decode(s).decode("utf-8"))


# --- Ed25519 Pure Python RFC 8032 Implementation ---

_P = 2**255 - 19
_Q = 2**252 + 27742317777372353535851937790883648493
_D = -121665 * pow(121666, _P - 2, _P) % _P
_I = pow(2, (_P - 1) // 4, _P)


def _inv(x: int) -> int:
    return pow(x, _P - 2, _P)


def _xrecover(y: int) -> int:
    xx = (y * y - 1) * _inv(_D * y * y + 1)
    x = pow(xx, (_P + 3) // 8, _P)
    if (x * x - xx) % _P != 0:
        x = (x * _I) % _P
    if x % 2 != 0:
        x = _P - x
    return x


_By = 4 * _inv(5) % _P
_Bx = _xrecover(_By)
_B = (_Bx, _By)


def _edwards_add(P: Tuple[int, int], Q: Tuple[int, int]) -> Tuple[int, int]:
    x1, y1 = P
    x2, y2 = Q
    x3 = (x1 * y2 + x2 * y1) * _inv(1 + _D * x1 * x2 * y1 * y2) % _P
    y3 = (y1 * y2 + x1 * x2) * _inv(1 - _D * x1 * x2 * y1 * y2) % _P
    return (x3, y3)


def _scalarmult(P: Tuple[int, int], e: int) -> Tuple[int, int]:
    if e == 0:
        return (0, 1)
    Q = _scalarmult(P, e // 2)
    Q = _edwards_add(Q, Q)
    if e & 1:
        Q = _edwards_add(Q, P)
    return Q


def _decodepoint(b: bytes) -> Tuple[int, int]:
    y = int.from_bytes(bytes([b[i] if i < 31 else b[31] & 0x7F for i in range(32)]), "little")
    x = _xrecover(y)
    if (x & 1) != ((b[31] >> 7) & 1):
        x = _P - x
    return (x, y)


def _encodepoint(P: Tuple[int, int]) -> bytes:
    x, y = P
    b = bytearray(y.to_bytes(32, "little"))
    if x & 1:
        b[31] |= 0x80
    return bytes(b)


def verify_ed25519(sig: bytes, msg: bytes, public_key: bytes) -> bool:
    """Verifies an RFC 8032 Ed25519 signature in pure Python."""
    if len(sig) != 64 or len(public_key) != 32:
        return False
    try:
        R = _decodepoint(sig[:32])
        A = _decodepoint(public_key)
        S = int.from_bytes(sig[32:], "little")
        if S >= _Q:
            return False
        h = hashlib.sha512(sig[:32] + public_key + msg).digest()
        k = int.from_bytes(h, "little")
        v1 = _scalarmult(_B, S)
        v2 = _edwards_add(R, _scalarmult(A, k))
        return v1 == v2
    except Exception:
        return False


def sign_ed25519(msg: bytes, private_key: bytes, public_key: bytes) -> bytes:
    """Creates an RFC 8032 Ed25519 signature in pure Python."""
    h = hashlib.sha512(private_key).digest()
    a = 2**254 + sum(2**i * ((h[i // 8] >> (i % 8)) & 1) for i in range(3, 254))
    r = int.from_bytes(hashlib.sha512(h[32:] + msg).digest(), "little")
    R = _scalarmult(_B, r)
    R_bytes = _encodepoint(R)
    k = int.from_bytes(hashlib.sha512(R_bytes + public_key + msg).digest(), "little")
    S = (r + k * a) % _Q
    return R_bytes + S.to_bytes(32, "little")


# --- RSA PKCS#1 v1.5 Verification Implementation ---

_SHA256_ASN1_PREFIX = bytes.fromhex("3031300d060960864801650304020105000420")


def _parse_rsa_public_key_pem(pem_str: str) -> Tuple[int, int]:
    """Extracts modulus (n) and exponent (e) from standard SPKI PEM string."""
    lines = [line.strip() for line in pem_str.strip().splitlines() if not line.startswith("---")]
    der = base64.b64decode("".join(lines))

    # Search for RSAPublicKey SEQUENCE containing modulus and exponent
    # Standard 2048-bit marker: 02 82 01 01 00
    idx = der.find(b"\x02\x82\x01\x01\x00")
    if idx != -1:
        n_bytes = der[idx + 5 : idx + 5 + 256]
        n = int.from_bytes(n_bytes, "big")
        e_idx = idx + 5 + 256
        e = int.from_bytes(der[e_idx + 2 : e_idx + 5], "big")
        return n, e

    # Check for general INTEGER sequence
    # Fallback search for 0x02 <len> <modulus>
    offset = der.find(b"\x30\x82")
    if offset != -1:
        # Traverse ASN.1 structure to find first large integer
        for i in range(len(der) - 260):
            if der[i] == 0x02 and der[i + 1] == 0x82:
                length = int.from_bytes(der[i + 2 : i + 4], "big")
                if length in (256, 257):
                    mod_bytes = der[i + 4 : i + 4 + length]
                    n = int.from_bytes(mod_bytes, "big")
                    rest = der[i + 4 + length :]
                    if rest.startswith(b"\x02"):
                        e_len = rest[1]
                        e = int.from_bytes(rest[2 : 2 + e_len], "big")
                        return n, e

    raise ValueError("Failed to parse RSA SPKI PEM: modulus not found")


def verify_rsa_sha256(sig: bytes, msg: bytes, public_key_pem: str) -> bool:
    """Verifies an RS256 signature against an RSA SPKI PEM public key."""
    try:
        n, e = _parse_rsa_public_key_pem(public_key_pem)
        s_int = int.from_bytes(sig, "big")
        k = (n.bit_length() + 7) // 8
        em = pow(s_int, e, n).to_bytes(k, "big")

        expected_hash = hashlib.sha256(msg).digest()
        # PKCS#1 v1.5 padding check: 0x00 0x01 [0xFF...] 0x00 [ASN.1] [Hash]
        expected_suffix = _SHA256_ASN1_PREFIX + expected_hash
        return em.startswith(b"\x00\x01") and em.endswith(expected_suffix)
    except Exception as exc:
        logger.debug("RSA verification exception: %s", exc)
        return False


# =============================================================================
# JWT ENCODING / DECODING & VERIFICATION HELPERS
# =============================================================================

def decode_jwt_unverified(token: str) -> Tuple[Dict[str, Any], Dict[str, Any], bytes]:
    """
    Extracts header, payload, and signature from compact serialized JWT without verification.
    """
    parts = token.strip().split(".")
    if len(parts) != 3:
        raise LicenseValidationError("Malformed JWT: token must contain exactly 3 dot-separated parts")

    header = base64url_decode_json(parts[0])
    payload = base64url_decode_json(parts[1])
    signature = base64url_decode(parts[2])
    return header, payload, signature


def verify_jwt_signature(
    token: str,
    public_key: Union[str, bytes],
) -> Tuple[bool, Optional[str], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Verifies the cryptographic signature of a JWT using bundled public key.
    Supports RS256, EdDSA (Ed25519), and HS256.
    """
    try:
        parts = token.strip().split(".")
        if len(parts) != 3:
            return False, "Malformed JWT structure", None, None

        header = base64url_decode_json(parts[0])
        payload = base64url_decode_json(parts[1])
        signature = base64url_decode(parts[2])
        signing_input = f"{parts[0]}.{parts[1]}".encode("ascii")

        alg = header.get("alg", "").upper()

        if alg == "RS256":
            pem_str = public_key.decode("utf-8") if isinstance(public_key, bytes) else str(public_key)
            if not verify_rsa_sha256(signature, signing_input, pem_str):
                return False, "RS256 signature verification failed", header, payload
            return True, None, header, payload

        elif alg == "EDDSA":
            pk_bytes: bytes
            if isinstance(public_key, bytes):
                pk_bytes = public_key[-32:] if len(public_key) >= 32 else public_key
            else:
                hex_str = public_key.strip().replace(" ", "").replace("\n", "")
                pk_bytes = bytes.fromhex(hex_str)
            if not verify_ed25519(signature, signing_input, pk_bytes):
                return False, "Ed25519 signature verification failed", header, payload
            return True, None, header, payload

        elif alg == "HS256":
            key_bytes = public_key.encode("utf-8") if isinstance(public_key, str) else public_key
            expected_sig = hmac.new(key_bytes, signing_input, hashlib.sha256).digest()
            if not hmac.compare_digest(signature, expected_sig):
                return False, "HS256 signature verification failed", header, payload
            return True, None, header, payload

        else:
            return False, f"Unsupported JWT algorithm: {alg}", header, payload

    except Exception as e:
        return False, f"Cryptographic verification error: {e}", None, None


def create_signed_jwt(
    payload: Dict[str, Any],
    private_key: Union[str, bytes],
    algorithm: str = "EdDSA",
    kid: Optional[str] = None,
) -> str:
    """
    Creates and cryptographically signs a JWT (useful for tests and cloud simulation).
    """
    header = {"alg": algorithm, "typ": "JWT"}
    if kid:
        header["kid"] = kid

    h_b64 = base64url_encode(json.dumps(header))
    p_b64 = base64url_encode(json.dumps(payload))
    signing_input = f"{h_b64}.{p_b64}".encode("ascii")

    alg = algorithm.upper()
    if alg == "HS256":
        key_bytes = private_key.encode("utf-8") if isinstance(private_key, str) else private_key
        sig = hmac.new(key_bytes, signing_input, hashlib.sha256).digest()
    elif alg == "EDDSA":
        sk_bytes = bytes.fromhex(private_key) if isinstance(private_key, str) else private_key
        # Derive public key
        h = hashlib.sha512(sk_bytes).digest()
        a = 2**254 + sum(2**i * ((h[i // 8] >> (i % 8)) & 1) for i in range(3, 254))
        pk_bytes = _encodepoint(_scalarmult(_B, a))
        sig = sign_ed25519(signing_input, sk_bytes, pk_bytes)
    else:
        raise ValueError(f"Algorithm {algorithm} not supported by standalone helper")

    sig_b64 = base64url_encode(sig)
    return f"{h_b64}.{p_b64}.{sig_b64}"


# =============================================================================
# LICENSE MANAGER CLASS
# =============================================================================

class LicenseManager:
    """
    Core Pipvane v3 License Management Client (Sprint 8 / FR-8.1 & FR-8.4).

    Responsibilities:
    - Validates 30-day cryptographically signed JWT licenses completely offline.
    - Enforces local engine account limits (up to 7 accounts for Pro, 4 for Standard).
    - Runs a silent background refresh daemon attempting token renewal every 7 days.
    - Manages fallback tolerance allowing trading continuity if the cloud is unreachable.
    """

    def __init__(
        self,
        vault: Optional[VaultStorage] = None,
        bundled_public_key: Optional[Union[str, bytes]] = None,
        cloud_refresh_url: str = "https://control.pipvane.com/api/v1/license/refresh",
        refresh_handler: Optional[Callable[[str, str], Optional[str]]] = None,
        clock_tolerance_s: float = CLOCK_TOLERANCE_SECONDS,
    ) -> None:
        self.vault = vault or VaultStorage()
        self.cloud_refresh_url = cloud_refresh_url
        self.refresh_handler = refresh_handler
        self.clock_tolerance_s = clock_tolerance_s

        # Bundled public key selection
        self.public_key: Union[str, bytes] = (
            bundled_public_key if bundled_public_key is not None else DEFAULT_BUNDLED_RSA_PUBLIC_KEY
        )

        # In-memory cached active entitlements
        self._active_entitlements: Optional[LicenseEntitlements] = None
        self._last_validation_result: Optional[LicenseValidationResult] = None
        self._cached_token_str: Optional[str] = None
        self._cached_payload: Optional[Dict[str, Any]] = None

        # Execution State & Clock Control (Sprint 8)
        self.is_locked: bool = False
        self._clock_provider: Optional[Callable[[], float]] = None

        # Silent Refresh Daemon Threading
        self._daemon_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    @property
    def entitlements(self) -> Optional[LicenseEntitlements]:
        """Returns the currently active license entitlements, if valid."""
        return self._active_entitlements

    @property
    def is_valid(self) -> bool:
        """Returns True if the current license is validated and unexpired."""
        res = self.validate_token()
        return res.is_valid

    @property
    def last_status(self) -> LicenseValidationStatus:
        """Returns the status from the last validation check."""
        if self._last_validation_result is not None:
            return self._last_validation_result.status
        return LicenseValidationStatus.NOT_FOUND

    # -------------------------------------------------------------------------
    # TOKEN VALIDATION & ENTITLEMENT PARSING
    # -------------------------------------------------------------------------

    def validate_token(
        self,
        token: Optional[str] = None,
        current_time: Optional[float] = None,
    ) -> LicenseValidationResult:
        """
        Validates the JWT token offline:
        1. Reads token from vault if token parameter is omitted.
        2. Verifies cryptographic signature using bundled public key.
        3. Checks strict 30-day exp timestamp against current_time.
        4. Parses and enforces entitlements (account_limit, license_id, tier).
        """
        now = current_time if current_time is not None else self.get_current_time()

        # Step 1: Retrieve token
        raw_token = token
        if raw_token is None:
            raw_token = self.vault.get_secret_str(VAULT_LICENSE_KEY)

        if not raw_token or not raw_token.strip():
            res = LicenseValidationResult(
                is_valid=False,
                status=LicenseValidationStatus.NOT_FOUND,
                reason="No license token found in SecureVault",
                evaluated_at=now,
            )
            self._last_validation_result = res
            return res

        raw_token = raw_token.strip()

        # Step 2: Cryptographic Signature Verification
        if raw_token == self._cached_token_str and self._cached_payload is not None:
            is_sig_valid = True
            sig_err = None
            payload = self._cached_payload
        else:
            is_sig_valid, sig_err, header, payload = verify_jwt_signature(raw_token, self.public_key)
            if not is_sig_valid:
                # Fallback attempt with Ed25519 bundled key if RSA failed
                if isinstance(self.public_key, str) and "-----BEGIN" in self.public_key:
                    is_ed_valid, _, header, payload = verify_jwt_signature(
                        raw_token, DEFAULT_BUNDLED_ED25519_PUBLIC_KEY_HEX
                    )
                    if is_ed_valid:
                        is_sig_valid = True
                        sig_err = None

            if is_sig_valid and payload is not None:
                self._cached_token_str = raw_token
                self._cached_payload = payload
            else:
                self._cached_token_str = None
                self._cached_payload = None

        if not is_sig_valid or payload is None:
            self.is_locked = True
            res = LicenseValidationResult(
                is_valid=False,
                status=LicenseValidationStatus.INVALID_SIGNATURE,
                reason=sig_err or "Cryptographic signature mismatch",
                evaluated_at=now,
            )
            self._last_validation_result = res
            return res

        # Step 3: Expiration & Lease Window Verification (FR-8.1 30-Day Expiry)
        exp = float(payload.get("exp", 0))
        iat = float(payload.get("iat", 0))

        if exp <= 0:
            res = LicenseValidationResult(
                is_valid=False,
                status=LicenseValidationStatus.MALFORMED,
                reason="Token missing mandatory 'exp' claim",
                evaluated_at=now,
            )
            self._last_validation_result = res
            return res

        if now >= exp + self.clock_tolerance_s:
            self.is_locked = True
            res = LicenseValidationResult(
                is_valid=False,
                status=LicenseValidationStatus.EXPIRED,
                reason=f"License expired at epoch {exp:.0f} (current epoch: {now:.0f})",
                days_remaining=0.0,
                evaluated_at=now,
            )
            self._last_validation_result = res
            return res

        # Step 4: Entitlements Parsing
        license_id = str(payload.get("license_id") or payload.get("sub") or "")
        account_limit = int(payload.get("account_limit", 7))
        tier = str(payload.get("tier", "Pro"))
        fingerprint = str(payload.get("device_fingerprint", ""))
        jti = str(payload.get("jti", ""))
        features = list(payload.get("features", []))

        entitlements = LicenseEntitlements(
            license_id=license_id,
            account_limit=account_limit,
            tier=tier,
            device_fingerprint=fingerprint,
            issued_at=iat,
            expires_at=exp,
            jti=jti,
            features=features,
            raw_payload=payload,
        )

        days_remaining = max(0.0, (exp - now) / 86400.0)

        # Check if silent refresh is due (<= 7 days to expiry or >= 7 days since last refresh)
        last_refresh = self.get_last_refresh_timestamp()
        needs_refresh = (days_remaining <= 7.0) or ((now - last_refresh) >= SEVEN_DAYS_SECONDS)

        res = LicenseValidationResult(
            is_valid=True,
            status=LicenseValidationStatus.VALID,
            entitlements=entitlements,
            days_remaining=round(days_remaining, 2),
            needs_refresh=needs_refresh,
            evaluated_at=now,
        )

        with self._lock:
            self._active_entitlements = entitlements
            self._last_validation_result = res

        return res

    # -------------------------------------------------------------------------
    # ENTITLEMENT ENFORCEMENT
    # -------------------------------------------------------------------------

    def enforce_account_limit(self, active_accounts_count: int) -> bool:
        """
        Enforces account_limit inside the local execution engine.
        Raises LicenseAccountLimitExceededError if active accounts exceed entitlement.
        """
        val_res = self.validate_token()
        if not val_res.is_valid or val_res.entitlements is None:
            raise LicenseValidationError(f"Cannot enforce account limit: {val_res.reason}")

        limit = val_res.entitlements.account_limit
        if active_accounts_count > limit:
            raise LicenseAccountLimitExceededError(
                f"Active accounts ({active_accounts_count}) exceeds license entitlement limit ({limit})"
            )

        return True

    def can_register_terminal(self, current_terminal_count: int) -> bool:
        """
        Pure query: returns True if registering another terminal is allowed
        under the current license account_limit.
        """
        val_res = self.validate_token()
        if not val_res.is_valid or val_res.entitlements is None:
            return False
        return current_terminal_count < val_res.entitlements.account_limit

    def get_current_time(self) -> float:
        """Returns the current epoch timestamp according to clock provider or system time."""
        if self._clock_provider is not None:
            return float(self._clock_provider())
        return time.time()

    def set_clock_provider(self, provider: Optional[Callable[[], float]]) -> None:
        """Configures a custom clock provider (e.g. for mock chronology tests)."""
        self._clock_provider = provider

    def check_execution_allowed(
        self,
        current_time: Optional[float] = None,
    ) -> Tuple[bool, Optional[str]]:
        """
        Validates whether order execution is permitted under the 30-day offline grace period.
        If expired, strictly locks the engine and returns (False, 'LICENSE_EXPIRED: ...').
        """
        now = current_time if current_time is not None else self.get_current_time()
        val_res = self.validate_token(current_time=now)

        if not val_res.is_valid:
            self.is_locked = True
            if val_res.status == LicenseValidationStatus.EXPIRED:
                return (
                    False,
                    f"LICENSE_EXPIRED: 30-day offline grace period exceeded ({val_res.reason})",
                )
            return False, f"LICENSE_{val_res.status.value}: Execution strictly locked ({val_res.reason})"

        self.is_locked = False
        return True, None

    def assert_execution_allowed(
        self,
        current_time: Optional[float] = None,
    ) -> None:
        """
        Asserts that execution is allowed.
        Strictly throws LicenseExpiredError with 'LICENSE_EXPIRED' if grace period has elapsed.
        """
        is_allowed, reason = self.check_execution_allowed(current_time)
        if not is_allowed:
            self.is_locked = True
            raise LicenseExpiredError(reason or "LICENSE_EXPIRED: 30-day offline grace period exceeded")

    def get_ui_diagnostics(
        self,
        current_time: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Provides safe telemetry and diagnostic state for the UI layer without crashing.
        """
        now = current_time if current_time is not None else self.get_current_time()
        val_res = self.validate_token(current_time=now)
        ent = val_res.entitlements

        return {
            "is_valid": val_res.is_valid,
            "status": val_res.status.value,
            "is_locked": self.is_locked or (not val_res.is_valid),
            "can_execute": val_res.is_valid,
            "license_id": ent.license_id if ent else None,
            "tier": ent.tier if ent else None,
            "account_limit": ent.account_limit if ent else 0,
            "days_remaining": val_res.days_remaining,
            "needs_refresh": val_res.needs_refresh,
            "error_code": (
                "LICENSE_EXPIRED"
                if val_res.status == LicenseValidationStatus.EXPIRED
                else (val_res.status.value if not val_res.is_valid else None)
            ),
            "error_message": val_res.reason,
            "ui_modal_open": not val_res.is_valid,
            "ui_status_banner": (
                "Expired"
                if val_res.status == LicenseValidationStatus.EXPIRED
                else ("Active" if val_res.is_valid else "Invalid")
            ),
        }

    # -------------------------------------------------------------------------
    # SECURE VAULT PERSISTENCE & TIMESTAMPS
    # -------------------------------------------------------------------------

    def store_token(
        self,
        token: str,
        current_time: Optional[float] = None,
    ) -> LicenseValidationResult:
        """
        Validates token offline and securely persists it into Windows DPAPI SecureVault.
        """
        val_res = self.validate_token(token, current_time=current_time)
        if not val_res.is_valid:
            raise LicenseValidationError(f"Refusing to store invalid token: {val_res.reason}")

        now = current_time if current_time is not None else self.get_current_time()
        self.vault.set_secret(VAULT_LICENSE_KEY, token.strip())
        self.vault.set_secret(VAULT_LAST_REFRESH_KEY, str(now))
        logger.info(
            "Securely stored license token in SecureVault (License: %s, Limit: %d, Expiry: %.0f)",
            val_res.entitlements.license_id,
            val_res.entitlements.account_limit,
            val_res.entitlements.expires_at,
        )
        return val_res

    def get_last_refresh_timestamp(self) -> float:
        """Retrieves epoch timestamp of the last successful token refresh."""
        raw = self.vault.get_secret_str(VAULT_LAST_REFRESH_KEY)
        if raw:
            try:
                return float(raw)
            except ValueError:
                pass
        return 0.0

    # -------------------------------------------------------------------------
    # SILENT 7-DAY REFRESH LOOP & DAEMON
    # -------------------------------------------------------------------------

    def refresh_token(self, current_time: Optional[float] = None) -> bool:
        """
        Pings Cloud Agent's refresh endpoint to renew the 30-day lease token.
        On success: Overwrites token in SecureVault.
        On failure: Logs warning but allows trading to continue until strict 30-day exp.
        """
        now = current_time if current_time is not None else self.get_current_time()
        current_token = self.vault.get_secret_str(VAULT_LICENSE_KEY)

        if not current_token:
            logger.warning("Silent refresh skipped: No existing license token to refresh.")
            return False

        current_entitlements = self._active_entitlements
        license_id = current_entitlements.license_id if current_entitlements else "unknown"
        exp_time = current_entitlements.expires_at if current_entitlements else 0.0

        try:
            new_token: Optional[str] = None

            # Strategy 1: Dependency-injected refresh handler (for testing & offline mock)
            if self.refresh_handler is not None:
                new_token = self.refresh_handler(current_token, license_id)
            else:
                # Strategy 2: HTTP call via urllib.request (zero external package dependencies)
                import urllib.request
                import urllib.error

                req = urllib.request.Request(
                    self.cloud_refresh_url,
                    data=json.dumps({"license_id": license_id, "token": current_token}).encode("utf-8"),
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {current_token}",
                        "User-Agent": "Pipvane-Engine/3.0",
                    },
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=10.0) as resp:
                    if resp.status == 200:
                        body = json.loads(resp.read().decode("utf-8"))
                        new_token = body.get("token") or body.get("jwt")

            if not new_token:
                logger.warning(
                    "Silent license refresh returned empty token. Existing lease remains valid until %.0f.",
                    exp_time,
                )
                return False

            # Validate newly received token offline before storing
            val_res = self.validate_token(new_token, current_time=now)
            if not val_res.is_valid:
                logger.warning(
                    "Silent license refresh failed: Returned token failed validation (%s). Retaining current lease.",
                    val_res.reason,
                )
                return False

            # Securely overwrite token in the SecureVault
            self.vault.set_secret(VAULT_LICENSE_KEY, new_token.strip())
            self.vault.set_secret(VAULT_LAST_REFRESH_KEY, str(now))

            with self._lock:
                self._active_entitlements = val_res.entitlements
                self._last_validation_result = val_res

            logger.info(
                "Silent license refresh succeeded! Lease extended until %.0f (%.1f days remaining).",
                val_res.entitlements.expires_at,
                val_res.days_remaining,
            )
            return True

        except Exception as err:
            # Resilient continuity: Log warning and permit trading until strict 30-day exp
            logger.warning(
                "Silent license refresh ping failed (%s). Execution continuity preserved; token valid until %.0f.",
                err,
                exp_time,
            )
            return False

    def check_and_refresh_on_startup(self, current_time: Optional[float] = None) -> bool:
        """
        Executes on engine application startup:
        Checks if 7 days have passed since last refresh (or if token is within 7 days of expiry).
        Triggers silent background renewal if required.
        """
        now = current_time if current_time is not None else self.get_current_time()
        val_res = self.validate_token(current_time=now)

        if not val_res.is_valid:
            logger.warning("Startup license check failed: %s", val_res.reason)
            return False

        if val_res.needs_refresh:
            logger.info("Startup license check: Lease renewal is due. Triggering silent refresh...")
            return self.refresh_token(current_time=now)

        logger.info(
            "Startup license check: License active (ID: %s, Days remaining: %.1f).",
            val_res.entitlements.license_id,
            val_res.days_remaining,
        )
        return True

    def start_silent_refresh_daemon(self, check_interval_s: float = 3600.0) -> None:
        """
        Starts the background daemon that checks every check_interval_s
        and automatically triggers the 7-day silent refresh loop.
        """
        if self._daemon_thread is not None and self._daemon_thread.is_alive():
            return

        self._stop_event.clear()

        def _daemon_loop():
            logger.info("Silent license refresh daemon started (Check interval: %.0fs).", check_interval_s)
            while not self._stop_event.is_set():
                try:
                    val_res = self.validate_token()
                    if val_res.is_valid and val_res.needs_refresh:
                        self.refresh_token()
                except Exception as e:
                    logger.warning("Error in silent refresh daemon loop: %s", e)

                # Wait with interruptibility
                self._stop_event.wait(timeout=check_interval_s)
            logger.info("Silent license refresh daemon stopped.")

        self._daemon_thread = threading.Thread(
            target=_daemon_loop,
            name="PipvaneSilentLicenseDaemon",
            daemon=True,
        )
        self._daemon_thread.start()

    def stop_silent_refresh_daemon(self, timeout: float = 5.0) -> None:
        """Gracefully halts the silent background refresh daemon."""
        self._stop_event.set()
        if self._daemon_thread is not None:
            self._daemon_thread.join(timeout=timeout)
            self._daemon_thread = None
